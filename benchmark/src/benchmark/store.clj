(ns benchmark.store
  (:require [clojure.edn :as edn]
            [clj-http.client :as client]))

(defrecord RemoteDB [baseurl token dbname])

(def schema
  [{:db/ident :db
    :db/valueType :db.type/ref                                   ;; TODO: unique database configs
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
   {:db/ident :tag                                          ;; for branch identifier, TODO: name differently?
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
    :db/cardinality :db.cardinality/one}
   #_{:db/ident :date                                         ;; TODO: save date directly?
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :foo
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}])


(defn parse-body [{:keys [body] :as response}]
  (println "res" response)
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

(defn- list-databases [db]
  (db-request db :get "databases"))

(defn- get-datoms [db]
  (db-request db :post "datoms" {:index :eavt}))

(defn- request-data [db q]
  (db-request db :post "q" {:query q}))

(defn- get-schema [db]
  (db-request db :get "schema"))

(defn transact-missing-schema [db]
  (let [current-schema (get-schema db)
        defined-attribs (vals current-schema)
        missing-schema (filterv (fn [entity] (not-any? #(= % (:db/ident entity))
                                                       defined-attribs))
                                schema)]
    (when (not-empty missing-schema)
      (transact-data db missing-schema))))


#_(def db (RemoteDB. "http://localhost:3001" "test-token" "benchmarks"))

#_(println "db" (list-databases db))

#_(println "schema" (transact-missing-schema db))
#_(println "datoms" (get-datoms db))
#_(println "datoms2" (request-data db '[:find ?e ?a ?v :where [?e ?a ?v]]))
#_(println "dbs" (request-data  db '[:find ?e ?a ?v :where [?e :db ?v]]))

#_(println "backends" (request-data db '[:find ?e ?v ?t :where [?e :dh-backend ?v ?t]]))



;; docker run -d --name datahike-server -p 3001:3000 -e DATAHIKE_SERVER_TOKEN=test-token -e DATAHIKE_SCHEMA_FLEXIBILITY=write -e DATAHIKE_STORE_BACKEND=file -e DATAHIKE_NAME=benchmarks -e DATAHIKE_STORE_PATH=/opt/datahike-server/benchmarks replikativ/datahike-server:snapshot
