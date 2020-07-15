(ns hitchhiker.tree.bootstrap.redis
  "Description of refcounting system in redis

  The refcounting system allows any key in redis to be managed
  by refcounting. This refcounter doesn't do cycle protection, but
  weakrefs would be very simple to add.

  To have a key point to another, we call add-refs with the pointer key
  and list of pointee keys. Usually, the pointer key would be a struct
  with children.

  Each key which is pointed to has an auxiliary key, which has the same name
  but ends in :rc. This is an int of the refcount of the key; the system
  deletes the key when its refcount reaches 0.

  Each key which is a pointer has an auxiliary key, which has the same name
  but ends in :rl. This is the list of keys that we have a reference, or pointer, to.
  rl=reflist. This list is used when the pointer is finally deleted--every
  key which the pointer points to must have its refcount decremented, and if
  any refcount reaches 0, that key must too be deleted.

  To reduce the frequency that keys are orphaned, we allow for new roots to
  be marked by the new-root function. This function stores the given key as a
  newly created root pointer, which is put onto a list with its creation time.
  Somehow, we track when root pointers are older than a certain time, so that
  we can delete them automatically."
  (:require
   [clojure.core.cache :as cache]
   [clojure.string :as str]
   [hitchhiker.tree.utils.async :as ha]
   [hitchhiker.tree :as tree]
   [hitchhiker.tree.node :as n]
   [hitchhiker.tree.op :as op]
   [hitchhiker.tree.backend :as b]
   [hitchhiker.tree.codec.nippy :as hn]
   [clojure.core.async :refer [promise-chan] :as async]
   [hitchhiker.tree.messaging :as msg]
   [taoensso.carmine :as car :refer [wcar]]
   [taoensso.nippy :as nippy]))

(defn add-refs
  [node-key children-keys]
  (apply car/eval*
         (str/join
          \newline
          ["local parent = ARGV[1]"
           "for i=2,#ARGV do"
           "  local child = ARGV[i]"
           "  redis.call('incr', child .. ':rc')"
           "  redis.call('rpush', parent .. ':rl', child)"
           "end"])
         0
         node-key
         children-keys))

(def drop-ref-lua
  "The string of the drop-ref function in lua. Returns the code in a local
   function with the named drop_ref"
  (str/join \newline
            ["local drop_ref = function (ref)"
             "  local to_delete = { ref }"
             "  while next(to_delete) ~= nil do"
             "    local cur = table.remove(to_delete)"
             "    if redis.call('decr', cur .. ':rc') <= 0 then"
             "      local to_follow = redis.call('lrange', cur .. ':rl', 0, -1)"
             "      for i = 1,#to_follow do"
             "        table.insert(to_delete, to_follow[i])"
             "      end"
             "      redis.call('del', cur .. ':rl')"
             "      redis.call('del', cur .. ':rc')"
             "      redis.call('del', cur)"
             "    end"
             "  end"
             "end"]))

(defn drop-ref
  [key]
  (car/lua (str drop-ref-lua "\ndrop_ref(_:my-key)")
           {} {:my-key key}))

(defn get-next-expiry
  "Given the current time, returns the next expiry time"
  [now]
  (car/lua (str/join \newline
                     [drop-ref-lua
                      "local to_drop = redis.call('zrangebyscore', 'refcount:expiry', 0, _:now)"
                      "for i=1,#to_drop do"
                      "  redis.call('zrem', 'refcount:expiry', to_drop[i])"
                      "  drop_ref(to_drop[i])"
                      "end"
                      "if redis.call('zcard', 'refcount:expiry') == 0 then"
                      "  return 1"
                      "else"
                      "  local head = redis.call('zrange', 'refcount:expiry', 0, 0, 'WITHSCORES')"
                      "  local head_time = head[2]"
                      "  local time_to_sleep = head_time - _:now"
                      "  if time_to_sleep < 0 then"
                      "    time_to_sleep = 1"
                      "  end"
                      "  return time_to_sleep"
                      "end"])
           {} {:now now}))

(defn start-expiry-thread!
  []
  (doto (Thread. (fn []
                   (while true
                     (->> (System/currentTimeMillis)
                          (get-next-expiry)
                          (wcar {})
                          (Thread/sleep)))))
    (.setName "redis rc refcounting expirer")
    (.setDaemon true)
    (.start)))

(defn add-to-expiry
  "Takes a refcounting key and a time for that key to expire"
  [key when-to-expire]
  ;; Redis sorted sets us 64 bit floats, and the time only needs 41 bits
  ;; 64 bit floats have 52 bit mantissas, so all is fine for the next century
  (car/lua (str/join \newline
                     ["redis.call('incr', _:my-key .. ':rc')"
                      "redis.call('zadd', 'refcount:expiry', _:when-to-expire, _:my-key)"])
           {} {:my-key key :when-to-expire when-to-expire}))

;; How we'll represent the timer-pointers
;; We'll make a zset to store keys to expire by their time
;; One function takes "now" as the arg & expires all the keys that should be expired, returning the time of the next key
;; The other function takes now & a key, and adds the rc

(let [cache (-> {}
                (cache/lru-cache-factory :threshold 10000)
                atom)]
  (defn totally-fetch
    [redis-key]
    (let [run (delay
               (loop [i 0]
                 (if (= i 1000)
                   (do (println "total fail") (throw (ex-info "total fail" {:key redis-key})))
                   (let [x (wcar {} (car/get redis-key))]
                     (if x
                       x
                       (do (Thread/sleep 25) (recur (inc i))))))))
          cs (swap! cache (fn [c]
                            (if (cache/has? c redis-key)
                              (cache/hit c redis-key)
                              (cache/miss c redis-key run))))
          val (cache/lookup cs redis-key)]
      (if val (ha/<?? val) @run)))

  (defn seed-cache!
    [redis-key val]
    (swap! cache cache/miss redis-key val)))

#_(def totally-fetch
    (memo/lru (fn [redis-key]
                (loop [i 0]
                  (if (= i 1000)
                    (do (println "total fail") (System/exit 1))
                    (let [x (wcar {} (car/get redis-key))]
                      (if x
                        x
                        (do (Thread/sleep 25) (recur (inc i))))))))
              :lru/threshold 400))

(defn synthesize-storage-addr
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (doto (promise-chan)
    (async/offer! key)))

(defrecord RedisAddr [last-key
                      redis-key
                      storage-addr]
  n/IAddress
  (-dirty? [_] false)
  (-resolve-chan [_]
    (ha/go-try (-> (totally-fetch redis-key)
                   (assoc :storage-addr (synthesize-storage-addr redis-key)))))

  n/INode
  (-last-key [_] last-key))

(comment
  (:cfg (wcar {} (car/get "b89bb965-e584-45a2-9232-5b76bf47a21c")))
  (update-in {:op-buf [1 2 3]} [:op-buf] into [4 5 6]))

(defn redis-addr
  [last-key redis-key]
  (->RedisAddr last-key redis-key (synthesize-storage-addr redis-key)))

(hn/ensure-installed!)

(nippy/extend-freeze RedisAddr :b-tree/redis-addr
                     [{:keys [last-key redis-key]} data-output]
                     (nippy/freeze-to-out! data-output last-key)
                     (nippy/freeze-to-out! data-output redis-key))

(nippy/extend-thaw :b-tree/redis-addr
                   [data-input]
                   (let [last-key (nippy/thaw-from-in! data-input)
                         redis-key (nippy/thaw-from-in! data-input)]
                     (redis-addr last-key redis-key)))

(defrecord RedisBackend [#_service]
  b/IBackend
  (-new-session [_] (atom {:writes 0
                           :deletes 0}))
  (-anchor-root [_ {:keys [redis-key] :as node}]
    (wcar {} (add-to-expiry redis-key (+ 5000 (System/currentTimeMillis))))
    node)
  (-write-node [_ node session]
    (ha/go-try
     (swap! session update-in [:writes] inc)
     (let [key (str (java.util.UUID/randomUUID))
           addr (redis-addr (n/-last-key node) key)]
                                        ;(.submit service #(wcar {} (car/set key node)))
       (when (some #(not (satisfies? op/IOperation %)) (:op-buf node))
         (println (str "Found a broken node, has " (count (:op-buf node)) " ops"))
         (println (str "The node data is " node))
         (println (str "and " (:op-buf node))))
       (wcar {}
             (car/set key node)
             (when (tree/index-node? node)
               (add-refs key
                         (for [child (:children node)
                               :let [child-key (ha/<?? (:storage-addr child))]]
                           child-key))))
       (seed-cache! key (ha/go-try node))
       addr)))
  (-delete-addr [_ addr session]
    (wcar {} (car/del addr))
    (swap! session update-in :deletes inc)))

(defn get-root-key
  [tree]
  (-> tree :storage-addr (async/poll!)))

(defn create-tree-from-root-key
  [root-key]
  (let [last-key (n/-last-key (wcar {} (car/get root-key)))] ; need last key to bootstrap
    (-> (->RedisAddr last-key root-key (synthesize-storage-addr root-key))
        n/-resolve-chan
        ha/<??)))

(comment
  (wcar {} (car/ping) (car/set "foo" "bar") (car/get "foo"))

  (println "cleared"
           (wcar {} (apply car/del
                           (count (wcar {} (car/keys "*")))))))

;; Benchmarks:
;; We'll have 2 workloads: in-order (the natural numbers) and random (doubles in 0-1)
;; We'll record 2 things:
;; - Series of timings per 0.1% of inserts
;; - Series of flush cost per X keys
;; The flush batch size should be a factor of b or of n--the benchmarks should
;; see results for both.
;; We'll do this for msg and core versions
;; We'll also benchmark a sortedset
;;
;; We'll look at the plots with log & linear R^2 values
;;
;; There should also be a burn-in test to confirm
;; Will be easier for testing after we add KV. Then build a dataset to store there.
;;
(comment

  (do
    (wcar {}
          (doseq [k ["foo" "bar" "baz" "quux"]
                  e ["" ":rc" ":rs" ":rl"]]
            (car/del (str k e))))

    (do (wcar {} (car/set "foo" 22))
                                        ;(wcar {} (car/set "foo:rc" 1))
        (wcar {} (car/set "bar" 33))
        (wcar {} (car/set "baz" "onehundred"))
        (wcar {} (car/set "quux" "teply"))
        (wcar {} (add-refs "baz" ["quux"]))
        (wcar {} (add-refs "foo" ["bar" "baz"])))
    (wcar {} (drop-ref "foo")))
  (doseq [k ["foo" "bar" "baz" "quux"]
          e ["" ":rc" ":rs" ":rl"]]
    (println (str k e) "=" (wcar {} ((if (= e ":rl")
                                       #(car/lrange % 0 -1)
                                       car/get) (str k e)))))
  (wcar {} (drop-ref "foo"))

  (wcar {} (create-refcounted "foo" 22))

  (wcar {} (car/flushall))
  (count (wcar {} (car/keys "*")))
  (count (tree/lookup-fwd-iter (create-tree-from-root-key (<? (:storage-addr (:tree my-tree)))) -1))
  (count (tree/lookup-fwd-iter (create-tree-from-root-key (<? (:storage-addr (:tree my-tree-updated)))) -1))
  (def my-tree (tree/flush-tree
                (time (reduce msg/insert
                              (tree/b-tree (tree/->Config 17 300 (- 300 17)))
                              (range 50000)))
                (->RedisBackend)))
  (def my-tree-updated (core/flush-tree
                        (msg/delete (:tree my-tree) 10)
                        (->RedisBackend)))
  (wcar {} (car/get (str @(:storage-addr (:tree my-tree)))))
  (wcar {} (car/get (str @(:storage-addr (:tree my-tree-updated)))))
  (wcar {} (car/set "foo" 10))
  (wcar {} (car/get "foo"))
  (wcar {} (drop-ref "foo"))
  (wcar {} (drop-ref @(:storage-addr (:tree my-tree))))
  (wcar {} (drop-ref @(:storage-addr (:tree my-tree-updated)))))
