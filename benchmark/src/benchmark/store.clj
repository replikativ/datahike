(ns benchmark.store
  (:require [clojure.edn :as edn]
            [clj-http.client :as client]
            [clojure.string :refer [join]]
            [clojure.pprint :refer [pprint]]
            [benchmark.config :as c]))

;; Remote server output

(defrecord RemoteDB [baseurl token dbname])

(def schema
  [{:db/ident :dh-config
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :dh-config/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :dh-config/index
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :dh-config/keep-history?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}
   {:db/ident :dh-config/schema-flexibility
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :dh-config/backend
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident :time
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/isComponent true}
   {:db/ident :time/mean
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :time/median
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :time/std
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :time/std
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :time/count
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident :function
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident :db-size
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident :execution
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/isComponent true}
   {:db/ident :execution/tx-size
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :execution/data-type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :execution/data-in-db?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident :tag                                          ;; for branch identifier
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}])


(defn parse-body [{:keys [body] :as _response}]
  (if-not (empty? body)
    (edn/read-string body)
    ""))

(defn db-request
  ([db method route] (db-request db method route nil))
  ([db method route body]
   (-> (client/request (merge {:url (str (:baseurl db) "/" route)
                               :method method
                               :content-type "application/edn"
                               :accept "application/edn"}
                              (when (or (= method :post) body)
                                {:body (str body)})
                              {:headers {:authorization (str "token " (:token db))
                                         :db-name (:dbname db)}}))
       parse-body)))

(defn transact-data [db tx-data]
  (db-request db :post "transact" {:tx-data tx-data}))

(defn get-datoms [db]
  (db-request db :post "datoms" {:index :eavt}))

(defn get-schema [db]
  (db-request db :get "schema"))

(defn transact-results [db results]
  (transact-data db results))

(defn transact-missing-schema [db]
  (let [current-schema (get-schema db)
        defined-attribs (vals current-schema)
        missing-schema (filterv (fn [entity] (not-any? #(= % (:db/ident entity))
                                                       defined-attribs))
                                schema)]
    (when (not-empty missing-schema)
      (transact-data db missing-schema))))

(defn add-ns-to-keys [current-ns hmap]
  (reduce-kv (fn [m k v]
               (let [next-key (if (= current-ns "") k (keyword (str current-ns "/" (name k))))
                     next-ns (name k)
                     next-val (cond
                                (map? v)
                                (add-ns-to-keys next-ns v)

                                (and (vector? v) (map? (first v)))
                                (mapv (partial add-ns-to-keys next-ns) v)

                                :else v)]
                 (assoc m next-key next-val)))
             {}
             hmap))

(defn server-description [options]
  (select-keys options [:db-server-url :db-token :db-name]))

;; file output

(defn save-measurements-to-file [paths measurements]
  (let [filename (first paths)]
    (try
      (if (pos? (count paths))
        (do (spit filename measurements)
            (println "Measurements successfully saved to" filename))
        (println measurements))
      (catch Exception e
        (println measurements)
        (println (str "Something went wrong while trying to save measurements to file " filename ":"))
        (.printStackTrace e)))))

;; multi method implementations

(defmulti save
          (fn [options _paths _measurements] (:output-format options)))

(defmethod save :default  [_options _paths measurements]
  (println "No output format given!")
  (println "Using standard output channel...")
  (pprint measurements))

(defmethod save "edn"  [_options paths measurements]
  (save-measurements-to-file paths measurements))

(defmethod save "csv"  [_options paths measurements]
  (let [col-paths (map :path c/csv-cols)
        titles (map :title c/csv-cols)
        csv (str (join "\t" titles) "\n"
                 (join "\n" (for [result (sort-by (apply juxt (map (fn [path] (fn [measurement] (get-in measurement path)))
                                                                   col-paths))
                                                  measurements)]
                              (join "\t" (map (fn [path] (get-in result path ""))
                                              col-paths)))))]
    (save-measurements-to-file paths csv)))

(defmethod save "remote-db"  [options _paths measurements]
  (let [rdb (apply ->RemoteDB (server-description options))
        db-entry (mapv #(->> (dissoc % :context)
                             (merge (:context %))
                             (add-ns-to-keys ""))
                       measurements)]
    (println "Database used:" rdb)
    (try
      (transact-missing-schema rdb)
      (transact-results rdb db-entry)
      (println "Successfully saved measurements to remote database.")
      (catch Exception e
        (println measurements)
        (println (str "Something went wrong while trying to save measurements to remote database:"))
        (.printStackTrace e)))))
