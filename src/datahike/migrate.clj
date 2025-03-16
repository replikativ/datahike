(ns ^:no-doc datahike.migrate
  (:require [datahike.api :as api]
            [datahike.constants :as c]
            [datahike.datom :as d]
            [datahike.db :as db]
            [clj-cbor.core :as cbor]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [taoensso.timbre :as timbre]
            [clj-cbor.tags.time :as tags.time])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [java.sql Timestamp]))

(defn export-db
  "Export the database in a flat-file of datoms at path.
  Intended as a temporary solution, pending developments in Wanderung."
  [conn path]
  (let [db @conn
        cfg (:config db)]
    (cbor/spit-all path (cond->> (sort-by
                                  (juxt d/datom-tx :e)
                                  (api/datoms (if (:keep-history? cfg) (api/history db) db) :eavt))
                          (:attribute-refs? cfg) (remove #(= (d/datom-tx %) c/tx0))
                          true (map seq)))))

(defn- instance-to-date [v]
  (if (instance? java.time.Instant v) (java.util.Date/from v) v))

(defn- instance-to-double [v]
  (if (float? v)
    (double v)
    v))

(defn process-cbor-file
  "Reads a CBOR file from `filename` and calls `process-fn` this allows for
  ingesting a large number of datoms without running out of memory."
  [filename process-fn stop-fn]
  (with-open [in (io/input-stream filename)]
    (loop []
      (when-let [data (cbor/decode cbor/default-codec in)]
        (process-fn data)
        (recur))))
  (stop-fn))

(defn import-db [conn path & opts]
  (let [filter-schema? (get opts :filter-schema? false)
        sync? (get opts :sync? true)
        load-entities? (get opts :load-entities? false)

        star-time (System/currentTimeMillis)
        tx-max (atom 0)
        datom-count (atom 0)
        txn-count (atom 0)
        stop (atom false)
        processed (atom false)
        update-tx-max (fn [tx] (reset! tx-max (max @tx-max tx)))
        q (LinkedBlockingQueue.) ;; thread safe queue for datoms
        prepare-datom (fn [item]
                        ;; update as we go so we don't run out of memory
                        (update-tx-max (nth item 3))
                        (swap! datom-count inc)
                        (-> (apply d/datom item)
                            ;; convert Instant to Date (previously experienced bug)
                            (update :v instance-to-date)
                            ;; convert Float to Double (previously reported bug)
                            (update :v instance-to-double)))
        add-datom (fn [item]
                     ;; skip schema datoms
                    (if filter-schema?
                      (when (-> item (nth 1) (str "0000") (subs 0 4) (not= ":db/")) (.put q (prepare-datom item)))
                      (.put q (prepare-datom item))))
        drain-queue (fn []
                      (let [acc (java.util.ArrayList.)]
                          ;; max required otherwise if previous write is slow too many in transaction
                        (.drainTo q acc 200000)
                        (vec acc)))]
      ;; prevent all the datoms that failed from being logged
    (timbre/merge-config! {:min-level [[#{"datahike.writing"} :fatal] [#{"datahike.writer"} :fatal]]})

    (async/thread
      (process-cbor-file
       path
       add-datom
       (fn []
         (reset! processed true))))

    (async/thread
      (timbre/info "Starting import")
      (loop []
        (Thread/sleep 100) ; batch writes for improved performance
        (when (or @processed (not sync?))
          (let [datoms (drain-queue)]
            (try
              (timbre/debug "loading" (count datoms) "datoms")
              (swap! txn-count + (count datoms))

              ;; in sync mode the max-tx will be as the test expect
              ;; in async mode the max-tx will be tx-max + 1
              (when sync?
                (swap! conn assoc :max-tx @tx-max))

              (if load-entities?
                (api/load-entities conn datoms) ;; load entities is faster for large datasets
                (api/transact conn datoms)) ;; transact is slow but preserves max-tx id
              (catch Exception e
                  ;; we can't print the message as it contains all the datoms
                (timbre/error "Error loading" (count datoms))))))
        (when (and (>= @txn-count @datom-count) @processed)
            ;; stop when we've transacted all datoms
          (when-not sync?
            (swap! conn assoc :max-tx @tx-max))
          (timbre/info "\nImported" @datom-count "datoms in total. \nTime elapsed" (- (System/currentTimeMillis) star-time) "ms")
          (reset! stop true))
        (when (not @stop) (recur))))

    (when sync?
      (loop []
        (Thread/sleep 100)
        (timbre/info "remaining" (- @datom-count @txn-count))
        (when (not @stop) (recur))))

      ;; allow the user to stop or monitor the ingestion thread with this atom
    (fn []
      {:complete? @stop
       :preprocessed? @processed
       :total-datoms @datom-count
       :remaining (- @datom-count @txn-count)})))

(comment
  ;; include
  ;; [io.replikativ/datahike-jdbc "0.3.49"]
  ;; [org.xerial/sqlite-jdbc "3.41.2.2"]
  (require '[stub-server.migrate :as m] :reload)
  (require '[datahike.api :as d])
  (require '[clojure.java.io :as io])
  (require '[datahike-jdbc.core])
  (def dev-db (str "./temp/sqlite/ingest"))
  (io/make-parents dev-db)
  ;; for testing large db's it's best to use sqlite. File store may run out of resources threads or pointers 
  ;; my test 21 seconds with SQLite with 16M datoms
  (def cfg      {:store {:backend :jdbc :dbtype "sqlite" :dbname dev-db}
                 :keep-history? true
                 :allow-unsafe-config true
                 :store-cache-size 20000
                 :search-cache-size 20000})
  (d/create-database cfg)
  (def conn (d/connect cfg))
  (def status (m/import-db conn "prod.backup.cbor" {:sync? true :filter-schema? false})))