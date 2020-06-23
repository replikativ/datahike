(ns datahike.client
  (:require [clj-http.client :as c]))

(defn parse-body [{:keys [body]}]
  (if-not (empty? body)
    (read-string body)
    ""))

(defn api-request
  ([method url]
   (api-request method url nil nil))
  ([method url data]
   (api-request method url data nil))
  ([method url data opts]
   (-> (c/request (merge {:url url
                          :method method
                          :content-type "application/edn"
                          :accept "application/edn"}
                         (when (or (= method :post) data)
                           {:body (str data)})
                         opts))
       parse-body)))

(comment
  (api-request :get "databases")

  (api-request :post "transact"
               {:tx-data [{:db/ident :name
                           :db/cardinality :db.cardinality/one
                           :db/valueType :db.type/string}]}
               {:headers {:db-name "users"}})


  (api-request :post "transact"
               {:tx-data [{:name "Konrad"}]}
               {:headers {:db-name "users"}})

  (api-request :post "datoms"
               {:index :eavt}
               {:headers {:db-name "users"}})

  (api-request :post "q"
               {:query '[:find ?e ?n :where [?e :name ?n]]}
               {:headers {:db-name "users"}}))


(defprotocol IConnection
  (transact [conn tx-data])
  (db [conn]))

(defrecord RemoteConnection [client]
  IConnection
  (transact
    [conn tx-data]
    (api-request :post (str (-> client :config :url) "/transact") {:tx-data tx-data} {:headers {:db-name "users"}}))
  (db
    [conn]
    (api-request :get (str (-> client :config :url) "/db") {:headers {:db-name "users"}})))


(defprotocol IClient
  (list-databases [client])
  (connect [client]))

(defrecord Client [config connection]
  IClient
  (list-databases
    [client]
    (api-request :get (str (:url config) "/databases")))
  (connect
    [client]
    (map->RemoteConnection {:client client})))

(defn client [cfg]
  (map->Client {:config cfg :connections (atom {})}))

(comment
  (def cl (client {:url "http://localhost:3000"}))

  (list-databases cl)

  (def conn (connect cl))

  (def db (db conn))

  (transact conn [{:name "Alice"}]))