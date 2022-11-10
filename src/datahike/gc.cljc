(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.index.interface :refer [-mark]]
            [konserve.core :as k]
            [konserve.gc :refer [sweep!]]
            [taoensso.timbre :refer [debug]]
            [clojure.core.async :refer [go go-loop <!]]))

(defn- reachable-in-branch [store branch ^java.util.Date date config]
  (go-loop [[to-check & r] [branch]
            reachable #{branch}]
    (let [{:keys [^java.util.Date tx-instant
                  eavt-key avet-key aevt-key temporal-eavt-key temporal-avet-key temporal-aevt-key]
           {parents branch} :branches} (<! (k/get store to-check))
          is-head (= to-check branch)]
      (if (and to-check
               (or is-head (> (.getTime tx-instant) (.getTime date))))
        (recur (concat r parents)
               (set/union reachable #{to-check}
                          (-mark eavt-key)
                          (-mark aevt-key)
                          (-mark avet-key)
                          (when (:keep-history? config)
                            (set/union
                             (-mark temporal-eavt-key)
                             (-mark temporal-aevt-key)
                             (-mark temporal-avet-key)))))
        (if (seq r)
          (recur r reachable)
          reachable)))))

(defn gc!
  "Invokes garbage collection on the database erasing all fragments that are not
  reachable after date (defaults to one day)."
  ([db] (gc! db (java.util.Date. (- (.getTime (java.util.Date.))
                                    (* 24 60 60 1000)))))
  ([db after-date]
   (go
     (let [now (java.util.Date.)
           {:keys [config store branches]} db
           reachable (loop [[branch & r] (keys branches)
                            reachable #{}]
                       (if branch
                         (recur r
                                (set/union reachable
                                           (<! (reachable-in-branch store branch after-date config))))
                         reachable))]
       (debug "gc reached: " reachable)
       (<! (sweep! store reachable now))))))
