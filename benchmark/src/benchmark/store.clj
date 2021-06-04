(ns benchmark.store
  (:require [clojure.edn :as edn]
            [clj-http.client :as client]))

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


(defn parse-body [{:keys [body] :as response}]
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

(defn list-databases [db]
  (db-request db :get "databases"))

(defn get-datoms [db]
  (db-request db :post "datoms" {:index :eavt}))

(defn request-data [db q]
  (let [query (if (map? q) q {:query q})]
    (db-request db :post "q" query)))

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
