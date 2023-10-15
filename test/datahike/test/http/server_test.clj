(ns datahike.test.http.server-test
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.http.server :refer [start-server stop-server]]
   [datahike.http.client :as api]))

(defn run-server-tests [server-config client-config]
  (let [{:keys [format]} client-config
        server (start-server server-config)]
    (try
      (let [new-config (api/create-database {:schema-flexibility :read
                                             :remote-peer        client-config})
            _          (is (map? new-config))

            conn                                 (api/connect new-config)
            {:keys [db-before db-after tx-data]} (api/transact conn [{:name "Peter" :age 42}])

            _ (is (seq tx-data))

            _ (is (not= (:commit-id db-before)
                        (:commit-id db-after)))

            test-db @conn
            _       (is (= test-db (api/db conn) db-after))

            query '[:find ?n ?a
                    :in $
                    :where
                    [$ ?e :age ?a]
                    [$ ?e :name ?n]]

            _ (is (= (api/q query test-db)
                     #{["Peter" 42]}))

            _ (is (map? (api/query-stats query test-db)))

            _ (is (= (api/pull test-db '[:*] 1)
                     {:db/id 1, :age 42, :name "Peter"}))

            _ (is (= 3 (count (api/datoms test-db :eavt))))

            _ (is (= 3 (count (api/seek-datoms test-db :eavt))))

            _ (is (map? (api/metrics test-db)))

            _ (is (map? (api/schema test-db)))

            _ (is (map? (api/reverse-schema test-db)))

            _ (is (map? (api/entity test-db 1)))

            _ (when-not (= format :edn)
                (is (= test-db (api/entity-db (api/entity test-db 1)))))

            _ (is (instance? datahike.remote.RemoteSinceDB (api/since test-db (java.util.Date.))))

            _ (is (instance? datahike.remote.RemoteAsOfDB (api/as-of test-db (java.util.Date.))))

            _ (is (nil? (api/release conn)))

            _ (is (nil? (api/delete-database new-config)))

            _ (is (false? (api/database-exists? new-config)))

            new-config (api/create-database {:schema-flexibility :write
                                             :remote-peer        client-config})

            conn (api/connect new-config)

            schema [{:db/ident :name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident :age
                     :db/valueType :db.type/number
                     :db/cardinality :db.cardinality/one}]

            _ (api/transact conn schema)

            _ (api/transact conn [{:name "Peter" :age 42}])

            test-db @conn
            _ (is (= (api/q query test-db)
                     #{["Peter" 42]}))]

        (is (nil? (api/release conn)))
        (is (nil? (api/delete-database new-config)))
        (stop-server server))
      (finally
        (stop-server server)))))

(deftest test-server
  (testing "Test transit binding."
    (let [port 23189]
      (run-server-tests {:port     port
                         :join?    false
                         :dev-mode false
                         :token    "securerandompassword"}
                        {:backend :datahike-server
                         :url    (str "http://localhost:" port)
                         :token  "securerandompassword"
                         :format :transit})))
  (testing "Test edn binding."
    (let [port 23190]
      (run-server-tests {:port     port
                         :join?    false
                         :dev-mode false
                         :token    "securerandompassword"}
                        {:backend :datahike-server
                         :url    (str "http://localhost:" port)
                         :token  "securerandompassword"
                         :format :edn})))
  (testing "Test JSON binding."
    (let [port 23191]
      (run-server-tests {:port     port
                         :join?    false
                         :dev-mode false
                         :token    "securerandompassword"}
                        {:backend :datahike-server
                         :url     (str "http://localhost:" port)
                         :token   "securerandompassword"
                         :format  :json}))))

(deftest test-authentication
  (testing "Password tokens must match."
    (let [port   23194
          server (start-server {:port     port
                                :join?    false
                                :dev-mode false
                                :token    "securerandompassword"})]
      (try
        (is (thrown-with-msg? Exception #"Exceptional status code: 401"
                              (api/create-database {:schema-flexibility :read
                                                    :remote-peer        {:backend :datahike-server
                                                                         :url    (str "http://localhost:" port)
                                                                         :token  "wrong"
                                                                         :format :edn}})))
        (finally
          (stop-server server)))))
  (testing "Dev-mode overrides password authentication."
    (let [port   23195
          server (start-server {:port     port
                                :join?    false
                                :dev-mode true
                                :token    "securerandompassword"})]
      (try
        (is (map? (api/create-database {:schema-flexibility :read
                                        :remote-peer        {:backend :datahike-server
                                                             :url    (str "http://localhost:" port)
                                                             :token  "wrong"
                                                             :format :edn}})))
        (finally
          (stop-server server))))))

(deftest test-json-interface
  (testing "Direct JSON interaction"
    (let [port   23196
          server (start-server {:port     port
                                :join?    false
                                :dev-mode true
                                :token    "securerandompassword"})
          remote {:backend :datahike-server
                  :url     (str "http://localhost:" port)
                  :token   "securerandompassword"
                  :format  :json}]
      (try
        (let [raw-cfg  (api/request-json-raw :post "create-database" remote
                                             "[\"{:schema-flexibility :read}\"]")
              raw-conn (api/request-json-raw :post "connect" remote
                                             (str "[" raw-cfg "]"))
              _        (api/request-json-raw :post "transact" remote
                                             (str "[" raw-conn ", [{\"name\": \"Peter\", \"age\": 42}]]"))
              raw-db   (api/request-json-raw :post "db" remote
                                             (str "[" raw-conn "]"))]
          (is (= (api/request-json-raw :get "q" remote
                                       (str "[\"[:find ?n ?a :in $1 :where [$1 ?e :age ?a] [$1 ?e :name ?n]]\","
                                            raw-db "]"))
                 "[\"!set\",[[\"Peter\",42]]]")))
        (finally
          (stop-server server))))))
