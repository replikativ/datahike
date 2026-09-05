(ns datahike.test.http.server-test
  (:require
   [babashka.http-client :as http]
   [clojure.string :as str]
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.http.client :as api]
   [datahike.http.routes :as routes]
   [datahike.http.server :as server :refer [start-server stop-server]]
   [datahike.query.resolve :as qr]
   [datahike.http.system :as system]))

(defn- local-port [^org.eclipse.jetty.server.Server instance]
  (.getLocalPort ^org.eclipse.jetty.server.ServerConnector
   (first (.getConnectors instance))))

(defn- wait-until [pred]
  (loop [attempts 200]
    (cond
      (pred) true
      (zero? attempts) false
      :else (do (Thread/sleep 10) (recur (dec attempts))))))

(defn run-server-tests [server-config client-config]
  (let [{:keys [format]} client-config
        server (start-server server-config)]
    (try
      (let [new-config (api/create-database {:store {:backend :memory :id #uuid "de110000-0000-0000-0000-000000000001"}
                                             :schema-flexibility :read
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

            new-config (api/create-database {:store {:backend :memory :id #uuid "de110000-0000-0000-0000-000000000002"}
                                             :schema-flexibility :write
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
                     #{["Peter" 42]}))

            swagger-response (http/request {:method :get
                                            :uri    (str (:url client-config) "/swagger.json")
                                            :as     :stream})
            swagger-body     (slurp (:body swagger-response))
            _                (is (= 200 (:status swagger-response)))
            _                (is (str/includes? swagger-body "\"swagger\":\"2.0\""))
            _                (is (str/includes? swagger-body "\"paths\""))]

        (is (nil? (api/release conn)))
        (is (nil? (api/delete-database new-config)))
        (stop-server server))
      (finally
        (stop-server server)))))

(deftest unsafe-bind-is-refused-before-jetty
  ;; The invalid port makes this safe even if the guard regresses: Jetty cannot
  ;; leave a listening server behind. The asserted error type proves the bind
  ;; guard, rather than Jetty's validation, ran first.
  (let [error (try
                (start-server {:host "0.0.0.0" :port -1 :join? false})
                nil
                (catch clojure.lang.ExceptionInfo e e))]
    (is (= :datahike.http/unsafe-bind (:type (ex-data error))))))

(deftest invalid-shutdown-timeout-is-refused-before-jetty
  (let [error (try
                (start-server {:host "127.0.0.1"
                               :port -1
                               :join? false
                               :shutdown-timeout-ms -1})
                nil
                (catch clojure.lang.ExceptionInfo e e))]
    (is (= :datahike.http/invalid-shutdown-timeout (:type (ex-data error))))))

(deftest standalone-uses-virtual-threads-for-streaming-on-jdk-21
  (when (>= (.feature (Runtime/version)) 21)
    (let [instance (start-server {:host "127.0.0.1"
                                  :port 0
                                  :join? false
                                  :metrics false
                                  :max-threads 17})
          pool (.getThreadPool ^org.eclipse.jetty.server.Server instance)
          executor (.getVirtualThreadsExecutor
                    ^org.eclipse.jetty.util.thread.QueuedThreadPool pool)]
      (try
        (is (instance? org.eclipse.jetty.util.thread.QueuedThreadPool pool))
        (is (= 17 (.getMaxThreads
                   ^org.eclipse.jetty.util.thread.QueuedThreadPool pool)))
        (is (some? executor))
        (finally
          (stop-server instance)))
      (is (.isShutdown ^java.util.concurrent.ExecutorService executor)
          "the owned virtual-thread executor is closed with the server"))))

(deftest graceful-stop-drains-active-requests-and-cleans-up-once
  (let [entered       (promise)
        finish        (promise)
        releases      (atom 0)
        system-closes (atom 0)
        configured-timeout (promise)
        instance      (atom nil)]
    (with-redefs [server/app
                  (fn [config _connections _nrepl-status]
                    (with-meta
                      (fn [request]
                        (if (= "/slow" (:uri request))
                          (do (deliver entered true)
                              @finish
                              {:status 200 :body "done"})
                          {:status 200 :body "unexpected"}))
                      {::server/config config}))
                  routes/release-all! (fn [_] (swap! releases inc))
                  system/close! (fn [_] (swap! system-closes inc))]
      (try
        (reset! instance
                (start-server {:host "127.0.0.1"
                               :port 0
                               :join? false
                               :metrics false
                               :shutdown-timeout-ms 5000
                               :configurator #(deliver configured-timeout
                                                       (.getStopTimeout
                                                        ^org.eclipse.jetty.server.Server %))}))
        (is (= 5000 (deref configured-timeout 1000 ::timeout))
            "the graceful timeout is installed before a custom configurator runs")
        (let [connector (first (.getConnectors ^org.eclipse.jetty.server.Server @instance))
              request   (future
                          (http/request {:method :get
                                         :uri (str "http://127.0.0.1:"
                                                   (local-port @instance)
                                                   "/slow")}))]
          (is (= true (deref entered 2000 ::timeout)))
          (let [stopping (future (stop-server @instance))]
            (is (wait-until #(.isShutdown ^org.eclipse.jetty.server.AbstractConnector connector))
                "the connector stops accepting before the active request finishes")
            (is (not (realized? stopping))
                "server stop waits for the active request")
            (deliver finish true)
            (is (= 200 (:status (deref request 2000 {:status ::timeout}))))
            (is (nil? (deref stopping 2000 ::timeout)))
            (stop-server @instance)
            (is (= 1 @releases))
            (is (= 1 @system-closes))))
        (finally
          (deliver finish true)
          (when @instance (stop-server @instance)))))))

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
                         :format  :json})))
  ;; The same suite over `application/cbor`. It is the only binding that needs
  ;; no server-side help to carry Clojure types: `support-embedded-edn-in-json`
  ;; and `json/xf-data-for-tx` are gated on the JSON content-type, so a query
  ;; whose keywords survive here proves the codec carried them rather than the
  ;; schema having re-derived them.
  (testing "Test CBOR binding."
    (let [port 23192]
      (run-server-tests {:port     port
                         :join?    false
                         :dev-mode false
                         :token    "securerandompassword"}
                        {:backend :datahike-server
                         :url     (str "http://localhost:" port)
                         :token   "securerandompassword"
                         :format  :cbor}))))

(deftest test-authentication
  (testing "Password tokens must match."
    (let [port   23194
          server (start-server {:port     port
                                :join?    false
                                :dev-mode false
                                :token    "securerandompassword"})]
      (try
        (is (thrown-with-msg? Exception #"Exceptional status code: 401"
                              (api/create-database {:store {:backend :memory :id #uuid "de110000-0000-0000-0000-000000000003"}
                                                    :schema-flexibility :read
                                                    :remote-peer        {:backend :datahike-server
                                                                         :url    (str "http://localhost:" port)
                                                                         :token  "wrong"
                                                                         :format :edn}})))
        (finally
          (stop-server server)))))
  (testing "Dev-mode overrides password authentication."
    (let [port   23195
          server (start-server {:port     port
                                :host     "127.0.0.1"
                                :join?    false
                                :dev-mode true
                                :token    "securerandompassword"})
          cfg {:store {:backend :memory :id #uuid "de110000-0000-0000-0000-000000000004"}
               :schema-flexibility :read
               :remote-peer        {:backend :datahike-server
                                    :url    (str "http://127.0.0.1:" port)
                                    :token  "wrong"
                                    :format :edn}}]
      (try
        (api/delete-database cfg)  ; Clean up any existing database
        (is (map? (api/create-database cfg)))
        (finally
          (api/delete-database cfg)
          (stop-server server))))))

(deftest test-json-interface
  (testing "Direct JSON interaction"
    (let [port   23196
          server (start-server {:port     port
                                :host     "127.0.0.1"
                                :join?    false
                                :dev-mode true
                                :token    "securerandompassword"})
          remote {:backend :datahike-server
                  :url     (str "http://127.0.0.1:" port)
                  :token   "securerandompassword"
                  :format  :json}
          _ (try (api/request-json-raw :post "delete-database" remote
                                       "[\"{:store {:backend :memory :id #uuid \\\"23196000-0000-0000-0000-000000000001\\\"}}\"]")
                 (catch Exception _))]  ; Ignore if database doesn't exist
      (try
        (let [raw-cfg  (api/request-json-raw :post "create-database" remote
                                             "[\"{:store {:backend :memory :id #uuid \\\"23196000-0000-0000-0000-000000000001\\\"} :schema-flexibility :read}\"]")
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
          (try (api/request-json-raw :post "delete-database" remote
                                     "[\"{:store {:backend :memory :id #uuid \\\"23196000-0000-0000-0000-000000000001\\\"}}\"]")
               (catch Exception _))
          (stop-server server))))))

(deftest test-query-functions-are-safe-by-default
  (let [port   23197
        before qr/*symbol-resolver*
        server (start-server {:port port :host "127.0.0.1" :join? false :dev-mode false
                              :token "securerandompassword"})
        remote {:backend :datahike-server :url (str "http://localhost:" port)
                :token "securerandompassword" :format :transit}]
    (try
      (let [cfg  (api/create-database {:store {:backend :memory :id #uuid "de110000-0000-0000-0000-000000000007"}
                                       :schema-flexibility :read
                                       :remote-peer        remote})
            conn (api/connect cfg)
            _    (api/transact conn [{:name "Peter"}])
            db   @conn]
        (testing "the curated functions and a subquery work over the wire"
          (is (= #{["PETER"]}
                 (api/q '[:find ?u :where [_ :name ?n] [(clojure.string/upper-case ?n) ?u]] db)))
          (is (= #{[1]}
                 (api/q '[:find ?c :where [(datahike.api/q [:find (count ?e) . :where [?e :name _]] $) ?c]] db))))
        (testing "the process's own resolver is untouched while the server runs"
          (is (identical? before qr/*symbol-resolver*)))
        (testing "evaluation, I/O, vars and reflection are refused"
          (doseq [q ['[:find ?x :where [(load-string "(+ 1 2)") ?x]]
                     '[:find ?x :where [(slurp "/etc/hostname") ?x]]
                     '[:find ?x :where [_ :name ?n] [(.getBytes ?n) ?x]]
                     '[:find ?x :where [_ :name ?n] [(clojure.core/requiring-resolve (quote clojure.core/inc)) ?x]]]]
            (is (thrown? Exception (api/q q db)) (pr-str q)))))
      (finally
        (stop-server server)))
    (is (identical? before qr/*symbol-resolver*) "the server never touches the process's own resolver"))
  (testing "an unknown mode is refused at start"
    (is (thrown-with-msg? Exception #"safe or :permissive"
                          (start-server {:port 23198 :host "127.0.0.1" :join? false :dev-mode false
                                         :token "securerandompassword" :query-functions :anything})))))

(deftest test-create-database-policy
  (let [remote (fn [port] {:backend :datahike-server :url (str "http://localhost:" port)
                           :token "securerandompassword" :format :transit})
        create (fn [port store] (api/create-database {:store store :schema-flexibility :read
                                                      :remote-peer (remote port)}))]
    (testing "an allow-list of backends"
      (let [port 23199
            server (start-server {:port port :host "127.0.0.1" :join? false :dev-mode false
                                  :token "securerandompassword"
                                  :create-database {:backends #{:memory}}})]
        (try
          (is (map? (create port {:backend :memory :id #uuid "de110000-0000-0000-0000-000000000011"})))
          (is (thrown-with-msg? Exception #"403"
                                (create port {:backend :file :path "/tmp/anywhere" :id #uuid "de110000-0000-0000-0000-000000000012"}))
              "a backend outside the set is refused before anything is created")
          (finally (stop-server server)))))
    (testing "the server chooses the store below its root"
      (let [port 23200
            root (str (System/getProperty "java.io.tmpdir") "/dh-policy-" (random-uuid))
            server (start-server {:port port :host "127.0.0.1" :join? false :dev-mode false
                                  :token "securerandompassword"
                                  :create-database {:store {:backend :file :path root}}})]
        (try
          (let [id  #uuid "de110000-0000-0000-0000-000000000013"
                cfg (create port {:backend :memory :id id})]
            (is (= :file (get-in cfg [:store :backend])) "the client's backend is not what gets created")
            (is (= id (get-in cfg [:store :id])) "the client's id is kept")
            (is (.startsWith ^String (get-in cfg [:store :path]) root) "the path is the server's"))
          (let [cfg (create port {:backend :file :path "/etc" :id #uuid "de110000-0000-0000-0000-000000000014"})]
            (is (.startsWith ^String (get-in cfg [:store :path]) root) "a client path is ignored"))
          (finally (stop-server server)))))
    (testing "a policy the server cannot act on is refused at start"
      (is (thrown-with-msg? Exception #":create-database"
                            (start-server {:port 23201 :host "127.0.0.1" :join? false :dev-mode false
                                           :token "securerandompassword"
                                           :create-database {:store {:backend :s3}}}))))))
