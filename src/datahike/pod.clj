(ns ^:no-doc datahike.pod
  (:refer-clojure :exclude [read read-string])
  (:require [bencode.core :as bencode]
            [clojure.java.io :as io]
            [cognitect.transit :as transit]
            [datahike.api :as d])
  (:import [java.io PushbackInputStream]))

(set! *warn-on-reflection* true)

(def stdin (PushbackInputStream. System/in))
(def stdout System/out)
(def stderr System/err)

(def debug? false)

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

(defn connect
  ([] (connect {}))
  ([config]
   (let [conn-hash (hash config)
         conn-id (str "conn:" conn-hash)]
     (swap! conns assoc conn-id (d/connect config))
     conn-id)))

(defn db [conn-id]
  (let [conn (get @conns conn-id)
        {:keys [max-eid max-tx] :as db} (d/db conn)
        db-id (str "db:" max-tx max-eid)]
    (swap! dbs assoc db-id db)
    db-id))

(defn db-with [db tx-data]
  (let [db (get @dbs db)
        {:keys [max-eid max-tx] :as with} (d/db-with db tx-data)
        db-id (str "with:" max-tx max-eid)]
    (swap! dbs assoc db-id with)
    db-id))

(defn as-of [db time-point]
  (let [ddb (get @dbs db)
        db-id (str "asof:" (hash [db time-point]))]
    (swap! dbs assoc db-id (d/as-of ddb time-point))
    db-id))

(defn since [db time-point]
  (let [ddb (get @dbs db)
        db-id (str "since:" (hash [db time-point]))]
    (swap! dbs assoc db-id (d/since ddb time-point))
    db-id))

(defn history [db]
  (let [db (get @dbs db)
        db-id (str "historical:" (hash db))]
    (swap! dbs assoc db-id (d/history db))
    db-id))

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
                (#{:db :asof :historical :since :with})))
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

(defn datoms
  ([db arg-map]
   (let [db (get @dbs db)]
     (->> (d/datoms db arg-map)
          (map seq))))
  ([db index & components]
   (let [db (get @dbs db)]
     (->> (d/datoms db {:index index :components components})
          (map seq)))))

(defn entity [db eid]
  (let [db (get @dbs db)]
    (write (str "OFOOOOOOOO: : " db " " eid))
    (d/entity db eid)))

(defn transact [conn arg-map]
  (let [c (get @conns conn)
        {:keys [db-before db-after tx-meta tx-data tempids]} (d/transact c arg-map)]
    (-> {:tempids tempids}
        (assoc :db-before (select-keys db-before [:max-tx :max-eid]))
        (assoc :db-after (select-keys db-after [:max-tx :max-eid]))
        (assoc :tx-meta tx-meta)
        (assoc :tx-data (map seq tx-data)))))

(defn metrics [db]
  (let [db (get @dbs db)]
    (d/metrics db)))

(defn schema [db]
  (let [db (get @dbs db)]
    (d/schema db)))

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
  (java.util.Date.)
  (as-of (db myconn) (java.util.Date. mytimestamp))
  (transact myconn {:tx-data [{:db/id 3 :age 25}]})
  (d/transact (get @conns myconn) [{:name "FOO"  :age "BAR"}])
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
     #_(as-of (db myconn) {:tx-id 536870916})
     (as-of (db myconn) {:millis mytimestamp})
     #_(since (db myconn) {:tx-id 536870914})
     (since (db myconn) {:millis mytimestamp})
     #_(history (db myconn)))
  (datoms (db myconn) {:index :eavt :components [2]})
  (datoms (as-of (db myconn) {:tx-id 536870916})
          {:index :eavt :components [2]})
  (transact myconn {:tx-data [{:db/ident :name :db/valueType :db.type/string :db/unique :db.unique/identity :db/index true :db/cardinality :db.cardinality/one}
                              {:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]})
  (schema (db myconn))
  (let [with-db (db-with (db myconn) [{:name "FOO"  :age "13"}])]
    (q '[:find (pull ?e [*])
         :where
         [?e :name _]]
       with-db))
  (entity (db myconn) 5))

(def publics
  {'as-of as-of
   'connect connect
   'create-database create-database
   'database-exists? database-exists?
   'db db
   'db-with db-with
   'delete-database delete-database
   'history history
   'metrics metrics
   'pull pull
   'pull-many pull-many
   'q q
   'datoms datoms
   'entity entity
   'since since
   'schema schema
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
            :describe (do (write {"format" "transit+json"
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
                              (write stdout reply))))
                        (recur))
            :shutdown (System/exit 0)
            (do
              (let [reply {"ex-message" "Unknown op"
                           "ex-data" (pr-str {:op op})
                           "id" id
                           "status" ["done" "error"]}]
                (write stdout reply))
              (recur))))))))

(comment
  (def id "foo")

  (require '[babashka.pods :as pods])
  (pods/load-pod "./dhi"))
