(ns datahike.test.http.writer-test
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.http.server :refer [start-server stop-server]]
   [datahike.http.writer]
   [datahike.api :as d]))

(deftest test-http-writer
  (testing "Testing distributed datahike.http.writer implementation."
    (let [port 31283
          server (start-server {:port     port
                                :join?    false
                                :dev-mode false
                                :token    "securerandompassword"})]
      (try
        (let [schema [{:db/ident       :name
                       :db/cardinality :db.cardinality/one
                       :db/index       true
                       :db/unique      :db.unique/identity
                       :db/valueType   :db.type/string}
                      {:db/ident       :sibling
                       :db/cardinality :db.cardinality/many
                       :db/valueType   :db.type/ref}
                      {:db/ident       :age
                       :db/cardinality :db.cardinality/one
                       :db/valueType   :db.type/long}]
              cfg    {:store              {:backend :mem :id "distributed_writer"}
                      :keep-history?      true
                      :schema-flexibility :write
                      :attribute-refs?    true
                      :writer             {:backend :datahike-server
                                           :url     (str "http://localhost:" port)
                                           :token   "securerandompassword"}}
              conn   (do
                       (d/delete-database cfg)
                       (d/create-database cfg)
                       (d/connect cfg))]
          (d/transact conn schema)
          (d/transact conn [{:name "Alice"
                             :age  25}])

          (is (= #{[25 "Alice"]}
                 (d/q '[:find ?a ?v
                        :in $ ?a
                        :where
                        [?e :name ?v]
                        [?e :age ?a]]
                      @conn
                      25)))
          (d/delete-database cfg))
        (finally
          (stop-server server))))))
