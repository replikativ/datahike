(ns datahike.experimental.gc
  (:require [clojure.set :as set]
            [datahike.index.interface :refer [-mark]]
            [konserve.core :as k]
            [konserve.gc :refer [sweep!]]
            [taoensso.timbre :refer [debug trace]]
            [superv.async :refer [<? S go-try go-loop-try]]
            [clojure.core.async  :as async])
  (:import [java.util Date]))

(defn- reachable-in-branch [store branch date config]
  (go-loop-try S [[to-check & r] [branch]
                  reachable #{branch}]
    (if to-check
      (let [{:keys [eavt-key avet-key aevt-key temporal-eavt-key temporal-avet-key temporal-aevt-key]
             {:keys [datahike/parents
                     datahike/created-at
                     datahike/updated-at]} :meta} (<? S (k/get store to-check))
            in-range?                             (> (.getTime ^Date (or updated-at
                                                                                   created-at))
                                                     (.getTime ^Date date))]
        (recur (concat r (when in-range? parents))
               (set/union reachable #{to-check}
                          (-mark eavt-key)
                          (-mark aevt-key)
                          (-mark avet-key)
                          (when (:keep-history? config)
                            (set/union
                             (-mark temporal-eavt-key)
                             (-mark temporal-aevt-key)
                             (-mark temporal-avet-key))))))
      reachable)))

(defn gc!
  "Invokes garbage collection on the database by whitelisting currently known branches.
  All db snapshots on these branches before-date will also be erased (defaults
  to beginning of time [no erasure])."
  ([db] (gc! db (Date.)))
  ([db before-date]
   (go-try S
     (let [now (Date.)
           _ (debug "starting gc" now)
           {:keys [config store]} db
           branches (<? S (k/get store :branches))
           _ (trace "retaining branches" branches)
           reachable (<? S (async/merge (map #(reachable-in-branch store % before-date config)
                                             branches)))
           reachable (conj reachable :branches)]
       (trace "gc reached: " reachable)
       (<? S (sweep! store reachable now))))))
