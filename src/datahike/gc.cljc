(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.index.interface :refer [-mark -seed-root! with-storage]]
            [datahike.index.secondary :as sec]
            [konserve.core :as k]
            [konserve.gc :refer [sweep!]]
            [replikativ.logging :as log]
            [superv.async :refer [<? S go-try <<?]]
            [clojure.core.async  :as async]
            [datahike.schema-cache :as sc])
  #?(:clj (:import [java.util Date])))

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
                  (let [{:keys                         [eavt-key avet-key aevt-key
                                                        temporal-eavt-key temporal-avet-key temporal-aevt-key
                                                        eavt-root aevt-root avet-root
                                                        temporal-eavt-root temporal-aevt-root temporal-avet-root
                                                        schema-meta-key secondary-index-keys]
                         {:keys [datahike/parents
                                 datahike/created-at
                                 datahike/updated-at]} :meta}
                        (<? S (k/get store to-check))
                        ;; Root fusion: inlined roots aren't separate konserve
                        ;; objects, so -mark on the lazy index would try to
                        ;; restore the root by address and fail. Seed each
                        ;; inlined root into its index (mirrors stored->db;
                        ;; with-storage's shallow copy in `mark` below carries
                        ;; the seeded root forward) so walk-addresses uses it
                        ;; and only its children are fetched.
                        _ (do (when eavt-root (-seed-root! eavt-key eavt-root))
                              (when aevt-root (-seed-root! aevt-key aevt-root))
                              (when avet-root (-seed-root! avet-key avet-root))
                              (when temporal-eavt-root (-seed-root! temporal-eavt-key temporal-eavt-root))
                              (when temporal-aevt-root (-seed-root! temporal-aevt-key temporal-aevt-root))
                              (when temporal-avet-root (-seed-root! temporal-avet-key temporal-avet-root)))
                        in-range? (> (get-time (or updated-at created-at))
                                     (get-time after-date))]
                    (let [sec-reachable (when (seq secondary-index-keys)
                                          (reduce-kv
                                           (fn [acc _idx-ident key-map]
                                             (set/union acc (sec/mark-from-key-map key-map store)))
                                           #{} secondary-index-keys))
                          ;; Stored roots are storage-detached; bind them to
                          ;; this store's storage so -mark can walk the tree.
                          mark (fn [idx] (-mark (with-storage (:index config) idx (:storage store))))
                          new-reachable (cond-> (set/union reachable #{to-check}
                                                           (when schema-meta-key #{schema-meta-key})
                                                           (mark eavt-key)
                                                           (mark aevt-key)
                                                           (mark avet-key))
                                          (:keep-history? config)
                                          (set/union (mark temporal-eavt-key)
                                                     (mark temporal-aevt-key)
                                                     (mark temporal-avet-key))
                                          sec-reachable
                                          (set/union sec-reachable))]
                      (recur (concat r (when in-range? parents))
                             (conj visited to-check)
                             new-reachable))))
                reachable)))))

(defn gc-storage!
  "Invokes garbage collection on the database by whitelisting currently known branches.
  All db snapshots on these branches before remove-before date will also be
  erased (defaults to beginning of time [no erasure]). The branch heads will
  always be retained."
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
