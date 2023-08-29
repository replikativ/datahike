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

            _ (is (neg-int? (api/tempid test-db)))

            _ (is (nil? (api/release conn)))

            _ (is (nil? (api/delete-database new-config)))

            _ (is (false? (api/database-exists? new-config)))]

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
  (testing "Test transit binding."
    (let [port 23190]
      (run-server-tests {:port     port
                         :join?    false
                         :dev-mode false
                         :token    "securerandompassword"}
                        {:backend :datahike-server
                         :url    (str "http://localhost:" port)
                         :token  "securerandompassword"
                         :format :edn}))))

(deftest test-authentication
  (testing "Password tokens must match."
    (let [port   23191
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
    (let [port   23192
          server (start-server {:port     port
                                :join?    false
                                :dev-mode true
                                :token    "securerandompassword"})]
      (try
        (is (map? (api/create-database {:schema-flexibility :read
                                        :remote-peer        {:backend :datahike-server
                                                             :url    (str "http://localhost:" port)
                                                             :token  "wrong"
                                                             :format :edn}})) )
        (finally
          (stop-server server))))))
