(ns datahike.pod
  (:refer-clojure :exclude [read read-string])
  (:require [bencode.core :as bencode]
            [clojure.java.io :as io]
            [cognitect.transit :as transit]
            [datahike.api :as d])
  (:import [java.io PushbackInputStream]
           [java.util Date])
  (:gen-class))

(set! *warn-on-reflection* true)

(def stdin (PushbackInputStream. System/in))
(def stdout System/out)
(def stderr System/err)

(def debug? true)

(defn debug [& strs]
  (when debug?
    (binding [*out* (io/writer System/err)]
      (apply prn strs))))

(defn write
  ([v] (write stdout v))
  ([stream v]
   (debug :writing v)
   (bencode/write-bencode stream v)
   (flush)))

(defn write-err
  ([v] (write stderr v))
  ([stream v]
   (debug :writing v)
   (bencode/write-bencode stream v)
   (flush)))

(defn read-string [^"[B" v]
  (String. v))

(defn read [stream]
  (bencode/read-bencode stream))

(defn read-transit [^String v]
  (transit/read
   (transit/reader
    (java.io.ByteArrayInputStream. (.getBytes v "utf-8"))
    :json)))

(defn write-transit [v]
  (write-err (str "VVVVVVVVVV: " v))
  (let [baos (java.io.ByteArrayOutputStream.)]
    (transit/write (transit/writer baos :json) v)
    (.toString baos "utf-8")))

(defonce conns (atom {}))

(defonce dbs (atom {}))

(defn create-database
  ([] (d/create-database))
  ([config] (d/create-database config)))

(defn delete-database
  ([] (d/delete-database))
  ([config] (d/delete-database config)))

(defn database-exists?
  ([] (d/database-exists?))
  ([config] (d/database-exists? config)))

(defn pull
  ([db arg-map]
   (let [db (get @dbs db)]
     (d/pull db arg-map)))
  ([db selector eid]
   (let [db (get @dbs db)]
     (d/pull db selector eid))))

(defn pull-many [db selector eids]
  (let [db (get @dbs db)]
    (d/pull-many db selector eids)))

(defn resolve-arg [arg]
  (if (and (string? arg)
           (->> (re-find #"^(\S+?):" arg)
                second
                keyword
                (#{:db :asof :historical :since})))
    (get @dbs arg)
    arg))

(defn q
  ([arg-map]
   (let [args (apply resolve-arg
                     (:args arg-map))]
     (-> (assoc arg-map :args [args])
         (d/q))))
  ([query & args]
   (let [args (apply resolve-arg
                     args)]
     (d/q query args))))

(defn db [conn-id]
  (let [conn (get @conns conn-id)
        {:keys [max-eid max-tx] :as db} (d/db conn)
        db-id (str "db:" max-tx max-eid)]
    (swap! dbs assoc db-id db)
    db-id))

(defn connect
  ([] (connect {}))
  ([config]
   (let [conn-hash (hash config)
         conn-id (str "conn:" conn-hash)]
     (swap! conns assoc conn-id (d/connect config))
     conn-id)))

(defn transact [conn arg-map]
  (let [c (get @conns conn)
        {:keys [db-before db-after tx-meta tempids]} (d/transact c arg-map)]
    (-> {:tempids tempids}
        (assoc :db-before (select-keys db-before [:max-tx :max-eid]))
        (assoc :db-after (select-keys db-after [:max-tx :max-eid]))
        (assoc :tx-meta (select-keys tx-meta [:db/txInstant :db/commitId]))
        (update-in [:tx-meta :db/txInstant] str)
        (update-in [:tx-meta :db/commitId] str))))

(defn metrics [db]
  (let [db (get @dbs db)]
    (d/metrics db)))

(defn as-of [db {:keys [millis tx-id] :as time-point}]
  (let [ddb (get @dbs db)
        db-id (str "asof:" (hash [db time-point]))]
    (if tx-id
      (swap! dbs assoc db-id (d/as-of ddb tx-id))
      (->> (Date. millis)
           (d/as-of ddb)
           (swap! dbs assoc db-id)))
    db-id))

(defn since [db {:keys [millis tx-id] :as time-point}]
  (let [ddb (get @dbs db)
        db-id (str "since:" (hash [db time-point]))]
    (if tx-id
      (swap! dbs assoc db-id (d/since ddb tx-id))
      (->> (Date. millis)
           (d/since ddb)
           (swap! dbs assoc db-id)))
    db-id))

(defn history [db]
  (let [db (get @dbs db)
        db-id (str "historical:" (hash db))]
    (swap! dbs assoc db-id (d/history db))
    db-id))

(comment
  (def myconf {:keep-history? true,
               :search-cache-size 10000,
               :index :datahike.index/persistent-set,
               :store {:id "inexpensive-red-fox", :backend :mem},
               :store-cache-size 1000,
               :attribute-refs? false,
               :writer {:backend :self},
               :crypto-hash? false,
               :schema-flexibility :read,
               :branch :db})
  (delete-database myconf)
  (create-database myconf)
  (database-exists? myconf)
  (def myconn (connect myconf))
  (transact myconn [{:name  "Alice", :age   20}
                    {:name  "Bob", :age   30}
                    {:name  "Charlie", :age   40}
                    {:age 15}])
  (def mytimestamp (System/currentTimeMillis))
  (def myteimestamp (Date.))
  (transact myconn {:tx-data [{:db/id 3 :age 25}]})
  (transact myconn [{:name "FOO"  :age "BAR"}])
  (q {:query '{:find [?e ?n ?a]
               :where
               [[?e :name ?n]
                [?e :age ?a]]}
      :args [(db myconn)]})
  (q '[:find ?e ?n ?a
       :where
       [?e :name ?n]
       [?e :age ?a]]
     (db myconn))
  (pull (db myconn) '[*] 1)
  (pull-many (db myconn) '[*] [1 2 3])
  (metrics (db myconn))
  (q '[:find ?e ?n ?a
       :where
       [?e :name ?n]
       [?e :age ?a]]
     (as-of (db myconn) {:tx-id 536870916})
     #_(as-of (db myconn) {:millis mytimestamp})
     #_(since (db myconn) {:tx-id 536870914})
     #_(since (db myconn) {:millis mytimestamp})
     #_(history (db myconn))))

(def publics
  {'as-of as-of
   'connect connect
   'create-database create-database
   'database-exists? database-exists?
   'db db
   'delete-database delete-database
   'metrics metrics
   'pull pull
   'pull-many pull-many
   'q q
   'since since
   'transact transact})

(defn lookup [var]
  (get publics (symbol (name var))))

(def describe-map
  (mapv (fn [k] {"name" (name k)})
        (keys publics)))

(defn run-pod [_args]
  (loop []
    (let [message (try (read stdin)
                       (catch java.io.EOFException _
                         ::EOF))]
      (when-not (identical? ::EOF message)
        (let [op (get message "op")
              op (read-string op)
              op (keyword op)
              id (some-> (get message "id")
                         read-string)
              id (or id "unknown")]
          (case op
            :describe (do (write-err {"format" "transit+json"
                                      "namespaces" [{"name" "datahike.pod"
                                                     "vars" describe-map}]
                                      "id" id
                                      "ops" {"shutdown" {}}})
                          (write {"format" "transit+json"
                                  "namespaces" [{"name" "datahike.pod"
                                                 "vars" describe-map}]
                                  "id" id
                                  "ops" {"shutdown" {}}})
                          (recur))
            :invoke (do (try
                          (let [var (-> (get message "var")
                                        read-string
                                        symbol)
                                args (get message "args")
                                args (read-string args)
                                args (read-transit args)]
                            (if-let [f (lookup var)]
                              (do (debug f)
                                  (debug args)
                                  (let [result (apply f args)
                                        _ (debug result)
                                        value (write-transit result)
                                        reply {"value" value
                                               "id" id
                                               "status" ["done"]}]
                                    (write-err reply)
                                    (write stdout reply)))
                              (throw (ex-info (str "Var not found: " var) {}))))
                          (catch Throwable e
                            (debug e)
                            (let [reply {"ex-message" (ex-message e)
                                         "ex-data" (write-transit
                                                    (assoc (ex-data e)
                                                           :type (str (class e))))
                                         "id" id
                                         "status" ["done" "error"]}]
                              (write-err reply)
                              (write stdout reply))))
                        (recur))
            :shutdown (System/exit 0)
            (do
              (let [reply {"ex-message" "Unknown op"
                           "ex-data" (pr-str {:op op})
                           "id" id
                           "status" ["done" "error"]}]
                (write-err reply)
                (write stdout reply))
              (recur))))))))

(comment
  (def id "foo")

  (require '[babashka.pods :as pods])
  (pods/load-pod "./dhi"))
