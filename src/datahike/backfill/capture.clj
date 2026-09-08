(ns ^:no-doc datahike.backfill.capture
  (:require [datahike.backfill.journal :as journal]
            [replikativ.logging :as log]))

(defn- dispose-safely! [descriptor]
  (try
    (journal/dispose! descriptor)
    (catch Exception e
      (log/warn :datahike/backfill-scratch-cleanup-failed
                {:id (journal/descriptor-id descriptor) :message (ex-message e)}))))

(defn dispose-owned!
  "Release scratch owned by a stopped writer. Never called on a live
   writer: earlier immutable cursors may still be needed by queued reports."
  [owned]
  (locking owned
    (doseq [[_ descriptor] @owned]
      (dispose-safely! descriptor))
    (reset! owned (with-meta {} {::closed? true}))))

(defn finish!
  "Release journals detached by a durably published report."
  [owned report]
  (doseq [descriptor (::retired report)]
    (dispose-safely! descriptor)
    (swap! owned dissoc (journal/descriptor-id descriptor))))

(defn prepare-report!
  "Capture notifications after transaction predicates accept a report, before
   chaining or enqueuing it. Each database retains its exact immutable prefix.
   Scratch is not a durability source: reconnect rebuilds from primary data."
  [owned report]
  (if (or (not (:db-after report))
          (and (empty? (get-in report [:db-after :secondary-index-build-deltas]))
               (empty? (get-in report [:db-before :secondary-index-build-journals]))))
    report
    (locking owned
      (when (::closed? (meta @owned))
        (throw (ex-info "Backfill journal owner is closed." {:type :writer-shut-down})))
      (let [before (:db-before report)
            after (:db-after report)
            previous (:secondary-index-build-journals before)
            current? (fn [ident]
                       (and (= :building (get-in after [:schema ident :db.secondary/status]))
                            (= (get-in before [:schema ident :db.secondary/building-since-tx])
                               (get-in after [:schema ident :db.secondary/building-since-tx]))))
            retained (into {} (filter (comp current? key)) previous)
            retired (mapv val (remove (comp current? key) previous))
            created (atom [])]
        (try
          (let [journals
                (reduce-kv
                 (fn [journals ident notifications]
                   (if (and (seq notifications)
                            (= :building (get-in after [:schema ident :db.secondary/status])))
                     (let [descriptor (or (get journals ident)
                                          (let [d (journal/create!
                                                   (get-in after [:config :writer :backfill-journal] {}))]
                                            (swap! created conj d)
                                            (swap! owned assoc (journal/descriptor-id d) d)
                                            d))]
                       (assoc journals ident (journal/append! descriptor notifications)))
                     journals))
                 retained (:secondary-index-build-deltas after {}))]
            (cond-> (-> report
                        (update :db-after dissoc :secondary-index-build-deltas
                                :secondary-index-build-journals))
              (seq journals) (assoc-in [:db-after :secondary-index-build-journals] journals)
              (seq retired) (assoc ::retired retired)))
          (catch Throwable e
          ;; Previously accepted prefixes remain authoritative if a later
          ;; journal fails. Unaccepted tails must not become visible on retry.
            (doseq [descriptor @created]
              (dispose-safely! descriptor)
              (swap! owned dissoc (journal/descriptor-id descriptor)))
            (throw e)))))))

(defn reduce-deltas
  "Replay exactly the database's captured prefix, then any pure pending data."
  [db ident f init]
  (let [stopped? (volatile! false)
        step (fn [acc notification]
               (let [result (f acc notification)]
                 (when (reduced? result) (vreset! stopped? true))
                 result))
        captured (if-let [descriptor (get-in db [:secondary-index-build-journals ident])]
                   (journal/reduce-journal descriptor step init)
                   init)]
    (if @stopped?
      captured
      (reduce f captured (get-in db [:secondary-index-build-deltas ident] [])))))
