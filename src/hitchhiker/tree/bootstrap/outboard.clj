(ns hitchhiker.tree.boostrap.outboard
  "API
  * transact! conn txns -- Applies the given transactions to the structure
  * snapshot conn -- returns an immutable snapshot of the structure
  * extend-lifetime snapshot until-when -- ensures the snapshot will
    not be GCed until the specified time (default to 5s)
  * lookup/lookup-fwd-iter snapshot -- operates on the snapshot
  * speculate snapshot txns -- returns a new snapshot with the txns applied to it
  * save-as snapshot new-name -- saves the given snapshot to the given name
  * create new-name -- creates a new empty structure at the given name
  * open name -- returns a connection to the named structure
  * close conn

  This API makes it easy to optimize the IO due to flushing, since we
  have complete control over when it happens.  It is similar to the
  Datomic API, but it remains to decide what transactions look like

  The update-queue is a blocking queue of functions which will be
  applied to the outboard

  The tree-atom is an atom containing the latest view of the
  outboard. This is for snapshotting

  The close signal is an atom which, when it's set to :shutdown,
  causes the outboard to shut its in-memory portions down"
  (:require
   [hitchhiker.tree.bootstrap.redis :as redis]
   [hitchhiker.tree :as tree]
   [hitchhiker.tree.utils.async :as ha]
   [hitchhiker.tree.codec.nippy :as nippy]
   [hitchhiker.tree.messaging :as msg]
   [taoensso.carmine :as car :refer [wcar]])
  (:import [java.util.concurrent
            LinkedBlockingQueue
            TimeUnit]))

(defrecord OutboardConnection [update-queue tree-atom close-signal thread
                               tree-name])

(defonce ^:private refcount-expiry-thread (redis/start-expiry-thread!))

(defonce ^:private connection-registry (atom {}))

(defn- launch-outboard-processer!
  "This starts a thread which will manage the given connection.
   Terrible things will happen if you call this on the same connection twice!"
  [conn save-name]
  (nippy/ensure-installed!)
  (let [^LinkedBlockingQueue q (:update-queue conn)
        {:keys [close-signal tree-atom]} conn
        flush-tree #(when-not (redis/get-root-key @tree-atom)
                      (swap! tree-atom
                             (fn [tree]
                               (:tree (ha/<?? (tree/flush-tree
                                               tree
                                               (redis/->RedisBackend))))))
                      (let [new-root (redis/get-root-key @tree-atom)]
                        (wcar
                         {}
                         (car/incr (str new-root ":rc"))
                         (redis/drop-ref (wcar {} (car/hget "named-hhs" save-name)))
                         (car/hset "named-hhs" save-name new-root))))]
    (reset! (:thread conn)
            (doto (Thread.
                   (fn* []
                        (loop [pending-writes 0
                               timed-out false]
                          (if (not= :shutdown @close-signal)
                            (if (or timed-out (> pending-writes 1000))
                              (do (flush-tree)
                                  (recur 0 false))
                              (if-let [update (try
                                                (.poll q 5 TimeUnit/SECONDS)
                                                (catch InterruptedException e
                                                  nil))]
                                (do (swap! tree-atom update)
                                    (recur (inc pending-writes) false))
                                (do (recur pending-writes true))))
                            (do (flush-tree)
                                (println "Shutting down" save-name))))))
              (.setName (str "Outboard processor for " save-name))
              (.start)))))

(defn create
  "Creates a new, empty outboard with the given name. Returns a connection to it."
  [new-name]
  (when (or (contains? @connection-registry new-name)
            (wcar {}
                  (car/hget "named-hhs" new-name)))
    (throw (ex-info (str "Cannot create outboard with name " new-name
                         ", its already in use") {:used-name new-name})))
  ;; TODO race condition where additional calls to create could all
  ;; succeed we should guard against this
  (let [conn (->OutboardConnection (LinkedBlockingQueue.)
                                   (-> (tree/->Config 30 600 870)
                                       tree/b-tree
                                       ha/<??
                                       atom)
                                   (atom :running)
                                   (atom nil)
                                   new-name)]
    (launch-outboard-processer! conn new-name)
    (swap! connection-registry assoc new-name conn)
    conn))

(defn destroy
  "Destroys the named outboard."
  [name]
  (when-not (string? name)
    (throw (ex-info "destroy takes the name of an outboard" {:name name})))
  (when (contains? @connection-registry name)
    (throw (ex-info "Cannot destroy outboard which is currently in use" {:name name})))
  (wcar {}
        (redis/drop-ref (wcar {} (car/hget "named-hhs" name)))
        (car/hdel "named-hhs" name)))

(defn open
  "Returns a connection to the named structure"
  [name]
  (or (get @connection-registry name)
      (if-let [root-key (wcar {} (car/hget "named-hhs" name))]
        (let [conn (->OutboardConnection
                    (LinkedBlockingQueue.)
                    (atom (redis/create-tree-from-root-key root-key))
                    (atom :running)
                    (atom nil)
                    name)]
          (launch-outboard-processer! conn name)
          (swap! connection-registry assoc name conn)
          conn)
        (throw (ex-info (str "Didn't find root-addr at " name) {})))))

(defn close
  "Frees the in-VM resources associated with the connection. The connection
   will no longer work."
  [conn]
  (when-not (instance? OutboardConnection conn)
    (throw (ex-info "close takes an outboard connection as an argument" {:conn conn})))
  (reset! (:close-signal conn) :shutdown)
  (.interrupt ^Thread @(:thread conn))
  (swap! connection-registry dissoc (:tree-name conn)))

(defn update!
  [conn update-fn]
  (.put ^LinkedBlockingQueue (:update-queue conn) update-fn))

(defn snapshot
  [conn]
  @(:tree-atom conn))

(defn insert
  "Inserts key/value pairs into the outboard data snapshot"
  [snapshot k v & kvs]
  (let [tree snapshot]
    (if (and (seq kvs) (even? (count kvs)))
      (loop [tree (ha/<?? (msg/insert tree k v))
             [k v & kvs] kvs]
        (if (and k v)
          (recur (ha/<?? (msg/insert tree k v)) kvs)
          tree))
      (ha/<?? (msg/insert tree k v)))))

(defn delete
  "Deletes keys from the outboard data snapshot"
  [snapshot k & ks]
  (let [tree snapshot]
    (if (seq ks)
      (reduce #(ha/<?? (msg/delete %1 %2)) tree (cons k ks))
      (ha/<?? (msg/delete tree k)))))

(defn lookup
  "Returns the value for the given key, or not-found which defaults to nil"
  ([snapshot k]
   (ha/<?? (msg/lookup snapshot k)))
  ([snapshot k not-found]
   (or (ha/<?? (msg/lookup snapshot k)) not-found)))

(defn lookup-fwd-iter
  "Returns a lazy iterator of KV pairs starting from the key.

   Be careful, this will continue for the entire tree. Make sure to stop."
  ([snapshot k]
   (msg/lookup-fwd-iter snapshot k)))

(defn save-as
  "Saves the snapshot to the given new name. This lets you
   incrementally clone data."
  [snapshot new-name]
  (let [new-conn (create new-name)
        flushed-snapshot (if (redis/get-root-key snapshot)
                           snapshot ; already flushed
                           (:tree (tree/flush-tree snapshot
                                                   (redis/->RedisBackend))))]
    (wcar {}
          (car/hset "named-hhs" new-name (redis/get-root-key flushed-snapshot))
          (car/incr (str (redis/get-root-key flushed-snapshot) ":rc")))
    (reset! (:tree-atom new-conn) flushed-snapshot)
    new-conn))

#_(defn extend-lifetime
    "Ensures the given snapshot will be readable for at least additional-ms longer."
    ;;TODO this is complex b/c we need to find all the reachable non-dirty nodes, and either add or extend their lifetimes...
    [snapshot additional-ms]
                                        ;((wcar {} (car/zincrby (re))))
    )

(comment
                                        ;First we'll create a new tree
  (def my-tree (create "my-tree"))
                                        ;(def my-tree (open "my-tree"))
  (println (count @connection-registry))
                                        ;This is how we'd close the tree
  (close my-tree)
                                        ;Once the tree is closed, you can destroy it to free its resources
  (destroy "my-tree")
                                        ;Here, we can iterate through the elements of the tree
  (lookup-fwd-iter (snapshot my-tree) "")

                                        ;save-as lets us take a snapshot and save it under another name
  (def other-tree (save-as (snapshot my-tree) "other-tree"))
                                        ;it returns a managed connection that can be interacted with like anything usual
  (lookup-fwd-iter (snapshot other-tree) "")
  (close other-tree)
  (destroy "other-tree")

                                        ; To write to a tree, send it an update function with update!
                                        ; Your function should take a snapshot as the argument, and return the modified snapshot to replace the data structure
  (update! my-tree (fn [snapshot] (insert snapshot "first key" "has a value of 22")))
  (update! my-tree (fn [snapshot] (insert snapshot "second key" {:lol 33})))
  (update! my-tree (fn [snapshot] (insert snapshot "3" 4)))

  (wcar {} (car/keys "*"))
  (wcar {} (car/flushall))
  (wcar {} (car/zrange "refcount:expiry" 0 -1))
  (wcar {} (car/hget "named-hhs" "my-tree"))
  (wcar {} (car/hget "named-hhs" "other-tree"))
  (wcar {} (car/get (str (wcar {} (car/hget "named-hhs" "my-tree")) ":rc")))

  )
