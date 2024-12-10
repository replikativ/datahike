(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.index.interface :refer [-mark]]
            [konserve.core :as k]
            [konserve.gc :refer [sweep!]]
            [taoensso.timbre :refer [debug trace]]
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
                                                        schema-meta-key]
                         {:keys [datahike/parents
                                 datahike/created-at
                                 datahike/updated-at]} :meta}
                        (<? S (k/get store to-check))
                        in-range? (> (get-time (or updated-at created-at))
                                     (get-time after-date))]
                    (recur (concat r (when in-range? parents))
                           (conj visited to-check)
                           (set/union reachable #{to-check}
                                      (when schema-meta-key #{schema-meta-key})
                                      (-mark eavt-key)
                                      (-mark aevt-key)
                                      (-mark avet-key)
                                      (when (:keep-history? config)
                                        (set/union
                                         (-mark temporal-eavt-key)
                                         (-mark temporal-aevt-key)
                                         (-mark temporal-avet-key)))))))
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
                 _ (debug "starting gc" now)
                 {:keys [config store]} db
                 _ (sc/clear-write-cache (:store config)) ; Clear the schema write cache for this store
                 branches (<? S (k/get store :branches))
                 _ (trace  "retaining branches" branches)
                 reachable (->> branches
                                (map #(reachable-in-branch store % remove-before config))
                                async/merge
                                (<<? S)
                                (apply set/union))
                 reachable (conj reachable :branches)]
             (trace  "gc reached: " reachable)
             (<? S (sweep! store reachable now))))))
