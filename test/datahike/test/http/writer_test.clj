(ns datahike.test.http.writer-test
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.connections :as connections]
   [datahike.http.client :as client]
   [datahike.http.server :refer [start-server stop-server]]
   [datahike.http.writer :as http-writer]
   [datahike.writer :as writer]
   [datahike.api :as d]))

(def remote-store-id #uuid "19000000-0000-0000-0000-000000000019")

(def remote-writer-config
  {:store  {:backend :memory :id remote-store-id}
   :writer {:backend :datahike-server
            :url     "http://localhost:38217"
            :token   "securerandompassword"}})

(deftest http-writer-refresh-capabilities
  (let [w (http-writer/->DatahikeServerWriter nil nil)]
    (is (false? (writer/streaming? w)))
    (is (true? (writer/refresh-on-deref? w))
        "HTTP does not push remote head updates into the connection")))

(deftest successful-remote-delete-invalidates-local-store-connections
  (let [deleted-db-branch (atom :connected)
        deleted-feature-branch (atom :connected)
        other-store-id #uuid "20000000-0000-0000-0000-000000000020"
        other-store-connection (atom :connected)
        registry (atom {[remote-store-id :db] {:conn deleted-db-branch :count 1}
                        [remote-store-id :feature] {:conn deleted-feature-branch :count 1}
                        [other-store-id :db] {:conn other-store-connection :count 1}})]
    (binding [connections/*connections* registry]
      (with-redefs [client/request-cbor (constantly {:remote-peer :server
                                                     :deleted? true})]
        (is (= {:deleted? true}
               @(writer/delete-database remote-writer-config))))
      (is (= :released @deleted-db-branch))
      (is (= :released @deleted-feature-branch))
      (is (= {[other-store-id :db] {:conn other-store-connection :count 1}}
             @registry)))))

(deftest failed-remote-delete-preserves-local-store-connections
  (let [connection (atom :connected)
        registry (atom {[remote-store-id :db] {:conn connection :count 1}})]
    (binding [connections/*connections* registry]
      (with-redefs [client/request-cbor (fn [& _]
                                          (throw (ex-info "remote delete failed" {})))]
        (is (thrown-with-msg? Exception #"remote delete failed"
                              @(writer/delete-database remote-writer-config))))
      (is (= :connected @connection))
      (is (= connection
             (get-in @registry [[remote-store-id :db] :conn]))))))

(deftest test-http-writer
  (testing "Testing distributed datahike.http.writer implementation."
    (let [port  31283
          server (start-server {:port     port
                                :join?    false
                                :dev-mode false
                                :token    "securerandompassword"})]
      (try
        (let [cfg    {:store              {:backend :file
                                           :path  "/tmp/distributed_writer"
                                           :id #uuid "17100000-0000-0000-0000-000000000001"}
                      :keep-history?      true
                      :schema-flexibility :read
                      :writer             {:backend :datahike-server
                                           :url     (str "http://localhost:" port)
                                           :token   "securerandompassword"}}
              conn   (do
                       (when (d/database-exists? cfg)
                         (d/delete-database cfg))
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
          cfg    {:store              {:backend :memory :id #uuid "00170000-0000-0000-0000-000000000017"}
                  :keep-history?      true
                  :schema-flexibility :read
                  :writer             {:backend :datahike-server
                                       :url     (str "http://localhost:" port)
                                       :token   "securerandompassword"}}]
      (is (thrown? Exception
                   (do
                     (d/delete-database cfg)
                     (d/create-database cfg)
                     (d/connect cfg))))))
  (testing "Transact fails without writer connection."
    (let [port 38217
          cfg  {:store              {:backend :memory :id #uuid "00180000-0000-0000-0000-000000000018"}
                :keep-history?      true
                :schema-flexibility :read
                :writer             {:backend :datahike-server
                                     :url     (str "http://localhost:" port)
                                     :token   "securerandompassword"}}
          server-cfg {:store              {:backend :memory :id #uuid "00180000-0000-0000-0000-000000000018"}
                      :keep-history?      true
                      :schema-flexibility :read}]
        ;; make sure the database exists before testing transact
      (do (d/delete-database server-cfg)
          (d/create-database server-cfg))
      (is (thrown? Exception
                   (d/transact (d/connect cfg)
                               [{:name "Should fail."}]))))))
