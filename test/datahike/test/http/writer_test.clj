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
        (let [cfg    {:store              {:backend :mem :id "distributed_writer"}
                      :keep-history?      true
                      :schema-flexibility :read
                      :writer             {:backend :datahike-server
                                           :url     (str "http://localhost:" port)
                                           :token   "securerandompassword"}}
              conn   (do
                       (d/delete-database cfg)
                       (d/create-database cfg)
                       (d/connect cfg))]

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

          (d/transact conn [{:name "Peter"
                             :age  18}])
          (is (= #{[18 "Peter"]}
                 (d/q '[:find ?a ?v
                        :in $ ?a
                        :where
                        [?e :name ?v]
                        [?e :age ?a]]
                      @conn
                      18)))

          (d/delete-database cfg))
        (finally
          (stop-server server))))))

(deftest test-http-writer-failure-without-server
  (testing "Db creation fails without writer connection."
    (let [port   38217
          cfg    {:store              {:backend :mem :id "distributed_writer_create_db"}
                  :keep-history?      true
                  :schema-flexibility :read
                  :writer             {:backend :datahike-server
                                       :url     (str "http://localhost:" port)
                                       :token   "securerandompassword"}}]
      (is (thrown? Exception
                   (do
                     (d/delete-database cfg)
                     (d/create-database cfg)
                     (d/connect cfg)))))
    (testing "Transact fails without writer connection."
      (let [port 38217
            cfg  {:store              {:backend :mem :id "distributed_writer_transact"}
                  :keep-history?      true
                  :schema-flexibility :read
                  :writer             {:backend :datahike-server
                                       :url     (str "http://localhost:" port)
                                       :token   "securerandompassword"}}
            server-cfg {:store              {:backend :mem :id "distributed_writer_transact"}
                        :keep-history?      true
                        :schema-flexibility :read}]
        ;; make sure the database exists before testing transact
        (do (d/delete-database server-cfg)
            (d/create-database server-cfg))
        (is (thrown? Exception
             (d/transact (d/connect cfg)
                         [{:name "Should fail."}])))))))
