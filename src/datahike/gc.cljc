(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.gc-guard :as guard]
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

  The sweep deletes an unreachable object only if it was written before this
  store's SAFE POINT (`datahike.gc-guard/safe-point`) — NOT before `now`.

  WHY: a commit writes every value the new head references and only THEN flips the
  branch head (`datahike.writing/commit!`; the barrier invariant). For the duration
  of that sequence the commit's objects are on disk and reachable from NOTHING —
  the head still names the previous snapshot — while ALREADY being older than
  `now`. A mark that runs inside the window calls them garbage and a `now` cutoff
  deletes them; the head then lands pointing at deleted objects. The safe point is
  the instant before which that verdict is final, and it is exact: it is `now` when
  nothing is in flight, and the START of the oldest in-flight sequence otherwise.
  `branch!`, `force-branch!` and `create-database` take the same guard, so this
  holds for them too, though they never touch the writer.

  Note the safe point is NOT `remove-before`. `remove-before` is a MARK-side policy
  (how far back the commit graph stays reachable — how much history you keep) and
  makes GC collect MORE. The safe point is a SWEEP-side bound (how recently written
  an object may be and still be judged) and makes it collect LESS.

  RUN IT WHERE THE WRITERS ARE. This follows from datahike's writer model, not from
  anything specific to GC: ALL WRITERS FOR A DATABASE RUN IN ONE JVM — they coordinate
  in memory, not through the store — and writer-side maintenance runs with them.
  `d/gc-storage` is a writer op, so it is already in the right place; the note is here
  because \"collect from a cron sidecar\" is a tempting shape and it is outside the
  model. Such a collector cannot observe the writer's in-flight sequences, and datahike
  cannot warn you: a second process gets a `:self` writer by default and looks like a
  writer too. (Cross-process writers are outside the model for a more basic reason as
  well — there is no head fencing yet, so they can lose each other's commits regardless
  of GC. See issue #878.) Readers are unconstrained."
  ([db] (gc-storage! db (#?(:clj Date. :cljs js/Date.) 0)))
  ([db remove-before]
   (go-try S
           (let [{:keys [config store]} db
                 store-id (:id (:store config))
                 now #?(:clj (Date.) :cljs (js/Date.))
                 ;; Capture `now` BEFORE reading the guard: a sequence that opened
                 ;; and closed between the two reads has landed its pointer, and the
                 ;; mark (which runs after) sees it. Reading the guard first would
                 ;; miss a sequence that opens in between.
                 cutoff (let [sp (guard/safe-point store-id)]
                          (if (< (get-time sp) (get-time now)) sp now))
                 _ (log/debug :datahike/gc-start {:time now :cutoff cutoff})
                 _ (when-not (= :self (:backend (:writer config)))
                     (log/warn :datahike/gc-without-local-writer
                               {:writer (:backend (:writer config))
                                :note "collecting a store this process does not write: in-flight commits elsewhere are invisible and may be swept"}))
                 _ (sc/clear-write-cache (:store config)) ; Clear the schema write cache for this store
                 branches (<? S (k/get store :branches))
                 _ (log/trace :datahike/gc-retain-branches {:branches branches})
                 reachable (->> branches
                                (map #(reachable-in-branch store % remove-before config))
                                async/merge
                                (<<? S)
                                (apply set/union))
                 reachable (conj reachable :branches)]
             (log/trace :datahike/gc-reachable {:reachable-count (count reachable)
                                                :cutoff cutoff})
             (<? S (sweep! store reachable cutoff))))))

(defn start-background-gc!
  "Runs `gc-storage!` on `conn`'s database periodically in the background and
   returns a zero-arg stop function. EXPERIMENTAL.

   Unlike the freed-address online GC (`datahike.online-gc`), this is a full
   reachability collection: it works with MULTIPLE BRANCHES, is insensitive to
   how garbage was produced (it needs no mutation-time tracking), and also
   prunes commit-graph records when a history window is set. Its cost is a walk
   of all reachable nodes per cycle, so prefer the online GC's freed-tracking for
   single-branch bulk imports where write amplification dominates; the two
   compose (this collector reclaims what freed-tracking cannot see).

   It runs CONCURRENTLY with the writer, and is safe to do so because the sweep
   stops at the store's SAFE POINT rather than at `now`: an in-flight commit's
   objects are written after that point and are spared (see `gc-storage!`). Like all
   writer-side maintenance, it belongs in the process the writers run in — which is
   datahike's writer model, not a GC-specific rule (`gc-storage!`).

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
