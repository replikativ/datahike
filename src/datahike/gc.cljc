(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.index.interface :refer [-mark -seed-root! with-storage]]
            [datahike.index.secondary :as sec]
            [konserve.core :as k]
            [konserve.gc :refer [sweep!]]
            [replikativ.logging :as log]
            [superv.async :refer [<? S go-try <<?]]
            ;; go-loop drives start-background-gc!'s scheduler; it's a MACRO, so
            ;; cljs needs it via :require-macros (mirrors datahike.versioning).
            #?(:clj  [clojure.core.async :as async :refer [go-loop]]
               :cljs [clojure.core.async :as async])
            [datahike.schema-cache :as sc])
  #?(:clj  (:import [java.util Date])
     :cljs (:require-macros [clojure.core.async :refer [go-loop]])))

;; meta-data does not get passed in macros
(defn get-time [d]
  (.getTime ^Date d))

(defn- reachable-in-branch [store branch after-date config]
  (go-try S
          (let [head-cid (<? S (k/get-in store [branch :meta :datahike/commit-id]))]
            (loop [[to-check & r] [branch]
                   visited        #{}
                   reachable      #{branch head-cid}]
              (if to-check
                (if (visited to-check) ;; skip
                  (recur r visited reachable)
                  (if-let [record (<? S (k/get store to-check))]
                    (let [{:keys                         [eavt-key avet-key aevt-key
                                                          temporal-eavt-key temporal-avet-key temporal-aevt-key
                                                          eavt-root aevt-root avet-root
                                                          temporal-eavt-root temporal-aevt-root temporal-avet-root
                                                          schema-meta-key secondary-index-keys]
                           {:keys [datahike/parents
                                   datahike/created-at
                                   datahike/updated-at]} :meta}
                          record
                          in-range? (> (get-time (or updated-at created-at))
                                       (get-time after-date))]
                      (let [sec-reachable (when (seq secondary-index-keys)
                                            (reduce-kv
                                             (fn [acc _idx-ident key-map]
                                               (set/union acc (sec/mark-from-key-map key-map store)))
                                             #{} secondary-index-keys))
                          ;; Stored roots are storage-detached; bind them to
                          ;; this store's storage so -mark can walk the tree.
                          ;; Root fusion: inlined roots aren't separate konserve
                          ;; objects, so -mark on the lazy index would try to
                          ;; restore the root by address and fail. Seed the
                          ;; inlined root into the with-storage COPY (owned,
                          ;; unpublished) — never into the stored record's
                          ;; index, which may be shared through the store's
                          ;; cache (mirrors stored->db) — so walk-addresses
                          ;; uses it and only its children are fetched.
                            mark (fn [idx root]
                                   (-mark (cond-> (with-storage (:index config) idx (:storage store))
                                            root (-seed-root! root))))
                            new-reachable (cond-> (set/union reachable #{to-check}
                                                             (when schema-meta-key #{schema-meta-key})
                                                             (mark eavt-key eavt-root)
                                                             (mark aevt-key aevt-root)
                                                             (mark avet-key avet-root))
                                            (:keep-history? config)
                                            (set/union (mark temporal-eavt-key temporal-eavt-root)
                                                       (mark temporal-aevt-key temporal-aevt-root)
                                                       (mark temporal-avet-key temporal-avet-root))
                                            sec-reachable
                                            (set/union sec-reachable))]
                        (recur (concat r (when in-range? parents))
                               (conj visited to-check)
                               new-reachable)))
                    ;; Record absent: already swept by an earlier pass with a
                    ;; narrower window, or the store runs :commit-graph? false
                    ;; and never persisted it. Lineage ends here — nothing to
                    ;; mark. (Without this guard the nil destructure NPEs at
                    ;; get-time.)
                    (recur r (conj visited to-check) reachable)))
                reachable)))))

(defn gc-storage!
  "Invokes garbage collection on the database by whitelisting currently known branches.
  All db snapshots on these branches before remove-before date will also be
  erased (defaults to beginning of time [no erasure]). The branch heads will
  always be retained.

  Safe to run CONCURRENTLY with an active writer: `now` is captured BEFORE the
  reachability walk, and the sweep only deletes unreachable objects written
  strictly before `now`. Any object a concurrent commit writes therefore
  post-dates the cutoff and is spared, and everything the walk's branch heads
  reach is whitelisted — in an immutable copy-on-write tree a new head can only
  reference nodes it shares with the old head (whitelisted) or nodes it just
  wrote (post-cutoff), so there is no gap. Which historical snapshots survive is
  governed entirely by `remove-before`."
  ([db] (gc-storage! db (#?(:clj Date. :cljs js/Date.) 0)))
  ([db remove-before]
   (go-try S
           (let [now #?(:clj (Date.) :cljs (js/Date.))
                 _ (log/debug :datahike/gc-start {:time now})
                 {:keys [config store]} db
                 _ (sc/clear-write-cache (:store config)) ; Clear the schema write cache for this store
                 branches (<? S (k/get store :branches))
                 _ (log/trace :datahike/gc-retain-branches {:branches branches})
                 reachable (->> branches
                                (map #(reachable-in-branch store % remove-before config))
                                async/merge
                                (<<? S)
                                (apply set/union))
                 reachable (conj reachable :branches)]
             (log/trace :datahike/gc-reachable {:reachable-count (count reachable)})
             (<? S (sweep! store reachable now))))))

(defn start-background-gc!
  "Runs `gc-storage!` on `conn`'s database periodically in the background and
   returns a zero-arg stop function. EXPERIMENTAL.

   Unlike the freed-address online GC (`datahike.online-gc`), this is a full
   reachability collection: it works with MULTIPLE BRANCHES, is insensitive to
   how garbage was produced (it needs no mutation-time tracking), and also
   prunes commit-graph records when a history window is set. It runs
   CONCURRENTLY with the writer — marking is read-only and the sweep spares
   everything written after the cycle started (see `gc-storage!`). Its cost is a
   walk of all reachable nodes per cycle, so prefer the online GC's
   freed-tracking for single-branch bulk imports where write amplification
   dominates; the two compose (this collector reclaims what freed-tracking
   cannot see).

   Options:
     :interval-ms       — cycle period (default 300000 = 5 min).
     :history-window-ms — when set, commits older than (now - window) are
                          pruned from the commit graph each cycle (ranged GC);
                          default nil keeps all history.

   Errors in a cycle are logged and the loop continues; stop with the
   returned function."
  ([conn] (start-background-gc! conn {}))
  ([conn {:keys [interval-ms history-window-ms]
          :or {interval-ms 300000}}]
   (let [stop-ch (async/chan)]
     (go-loop []
       (let [[_ ch] (async/alts! [stop-ch (async/timeout interval-ms)])]
         (when-not (= ch stop-ch)
           (let [remove-before (if history-window-ms
                                 (#?(:clj Date. :cljs js/Date.)
                                  (- #?(:clj (System/currentTimeMillis)
                                        :cljs (.getTime (js/Date.)))
                                     (long history-window-ms)))
                                 (#?(:clj Date. :cljs js/Date.) 0))
                 res (async/<! (gc-storage! @conn remove-before))]
             (if (instance? #?(:clj Throwable :cljs js/Error) res)
               (log/warn :datahike/background-gc-error {:error res})
               (log/debug :datahike/background-gc-cycle {:swept (count res)})))
           (recur))))
     (fn stop-background-gc! [] (async/close! stop-ch) :stopped))))
