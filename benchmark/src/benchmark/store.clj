(ns benchmark.store
  (:require [clojure.edn :as edn]
            [clj-http.client :as client]))

(defrecord RemoteDB [baseurl token dbname])

(def schema
  [{:db/ident :db-config
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :mean-time
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :function
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :db-size
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :tx-size
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :tag                                          ;; for branch identifier
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}
   {:db/ident :index
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :keep-history?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}
   {:db/ident :schema-flexibility
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :dh-backend
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}])


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

(defn db-config-eid
  "Get existing entity ID for database configuration or transact config and get ID from new entry"
  [db {:keys [dh-backend index keep-history? schema-flexibility] :as db-config}]
  (let [query {:query '[:find ?e
                        :in $ ?b ?i ?h ?s
                        :where
                        [?e :dh-backend ?b]
                        [?e :index ?i]
                        [?e :keep-history? ?h]
                        [?e :schema-flexibility ?s]]
               :args [dh-backend index keep-history? schema-flexibility]}
        existing-eid (ffirst (request-data db query))
        eid (if (nil? existing-eid)
              (first (second (:tx-data (transact-data db [db-config])))) ;; get new eid
              existing-eid)]
    eid))

(defn transact-results [db results]
  (let [config-mapping (memoize db-config-eid)
        tx-data (time (map #(update % :db-config (partial config-mapping db))
                           results))]
    (transact-data db tx-data)))

(defn transact-missing-schema [db]
  (let [current-schema (get-schema db)
        defined-attribs (vals current-schema)
        missing-schema (filterv (fn [entity] (not-any? #(= % (:db/ident entity))
                                                       defined-attribs))
                                schema)]
    (when (not-empty missing-schema)
      (transact-data db missing-schema))))
