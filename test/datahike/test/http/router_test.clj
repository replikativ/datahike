(ns datahike.test.http.router-test
  (:require
   [clojure.test :refer :all]
   [clojure.set :as set]
   [datahike.http.router :as router]
   [datahike.http.server :as server]
   [datahike.http.client :as client]
   [datahike.store :as ds]
   [datahike.api.specification :refer [api-specification]]
   [datahike.api :as d]
   [ring.adapter.jetty :as jetty]))

(deftest test-route-generation
  (testing "API routes are generated correctly"
    (let [routes (router/create-routes :format :raw :include-writers? false)]
      (is (seq routes) "Routes should not be empty")

      ;; Check for essential routes
      (let [route-paths (set (map :path routes))]
        (is (contains? route-paths "/create-database"))
        (is (contains? route-paths "/delete-database"))
        (is (contains? route-paths "/connect"))
        (is (contains? route-paths "/transact"))
        (is (contains? route-paths "/q"))
        (is (contains? route-paths "/db"))
        (is (contains? route-paths "/pull")))

      ;; Check route structure
      (doseq [route routes]
        (is (contains? route :path))
        (is (contains? route :method))
        (is (contains? route :handler))
        (is (contains? route :name))
        (is (fn? (:handler route)))
        (is (#{:get :post} (:method route)))))))

(deftest test-writer-routes
  (testing "Writer routes are generated when requested"
    (let [routes (router/generate-writer-routes)]
      (is (= 3 (count routes)) "Should have 3 writer routes")

      (let [route-paths (set (map :path routes))]
        (is (contains? route-paths "/create-database-writer"))
        (is (contains? route-paths "/delete-database-writer"))
        (is (contains? route-paths "/transact!-writer"))))))

(deftest test-reitit-format
  (testing "Routes can be converted to Reitit format"
    (let [routes (router/create-routes :format :reitit :include-writers? false)]
      (is (vector? routes))
      (is (seq routes))

      ;; Check Reitit structure
      (doseq [route routes]
        (is (vector? route))
        (is (string? (first route)))
        (is (map? (second route)))))))

(deftest test-ring-handler
  (testing "Ring handler processes requests correctly"
    (let [handler (router/create-ring-handler :include-writers? false)]

      ;; Test 404 for unknown route
      (let [response (handler {:uri "/unknown" :request-method :get})]
        (is (= 404 (:status response))))

      ;; Test that database-exists? route exists
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        ;; Will return false or error, but should not be 404
        (is (not= 404 (:status response)))))))

(deftest test-handler-wrapper
  (testing "Handler wrapper extracts parameters correctly"
    (let [routes (router/create-routes :format :raw)
          create-db-route (first (filter #(= "/create-database" (:path %)) routes))
          handler (:handler create-db-route)
          test-id-1 (str "test-db-" (System/currentTimeMillis))
          test-id-2 (str "test-db-" (System/currentTimeMillis) "-2")
          test-id-3 (str "test-db-" (System/currentTimeMillis) "-3")]

      ;; Test with vector format (original format)
      (let [response (handler {:body [{:store {:backend :mem
                                               :id test-id-1}}]})]
        (is (= 200 (:status response)))
        (d/delete-database {:store {:backend :mem :id test-id-1}}))

      ;; Test with map format (for compatibility)
      (let [response (handler {:body-params {:config {:store {:backend :mem
                                                              :id test-id-2}}
                                             :args []}})]
        (is (= 200 (:status response)))
        (d/delete-database {:store {:backend :mem :id test-id-2}}))

      ;; Test with Reitit parameters format
      (let [response (handler {:parameters {:body [{:store {:backend :mem
                                                            :id test-id-3}}]}})]
        (is (= 200 (:status response)))
        (d/delete-database {:store {:backend :mem :id test-id-3}})))))

(deftest test-router-works-without-server
  (testing "Router can create routes and handlers without server"
    ;; The important thing is that we can use the router functionality
    ;; without needing the server namespace
    (is (fn? (router/create-ring-handler))
        "Should be able to create ring handler")
    (is (seq (router/create-routes))
        "Should be able to create routes")

    ;; Test that the router namespace itself is loaded
    (is (resolve 'datahike.http.router/create-routes)
        "Router namespace should be loaded")

    ;; Verify we can handle a request without a server
    (let [handler (router/create-ring-handler :config {:dev-mode true})]
      (is (map? (handler {:uri "/unknown" :request-method :get}))
          "Handler should return a response map"))))

(deftest test-middleware-application
  (testing "Middleware can be applied to handlers"
    (let [call-count (atom 0)
          test-middleware (fn [handler]
                            (fn [request]
                              (swap! call-count inc)
                              (handler request)))
          handler (router/create-ring-handler
                   :middleware [test-middleware])]

      (handler {:uri "/unknown" :request-method :get})
      (is (= 1 @call-count) "Middleware should be called"))))

(deftest test-authentication
  (testing "Token authentication works as expected"
    (let [config {:token "test-token"
                  :dev-mode false}
          handler (router/create-ring-handler :config config)]

      ;; Test without token - should fail
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (= 401 (:status response)) "Should require authentication")
        (is (= {:error "Not authorized"} (read-string (:body response)))))

      ;; Test with wrong token - should fail
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :headers {"authorization" "token wrong-token"}
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (= 401 (:status response)) "Wrong token should fail"))

      ;; Test with correct token - should succeed
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :headers {"authorization" "token test-token"}
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 401 (:status response)) "Correct token should succeed"))

      ;; Test with Bearer format - should also work
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :headers {"authorization" "Bearer test-token"}
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 401 (:status response)) "Bearer format should work")))))

(deftest test-dev-mode
  (testing "Dev mode bypasses authentication"
    (let [config {:token "test-token"
                  :dev-mode true}  ; Dev mode enabled
          handler (router/create-ring-handler :config config)]

      ;; Even without token, should succeed in dev mode
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 401 (:status response)) "Dev mode should bypass auth")))))

(deftest test-prefix-routing
  (testing "Prefix parameter correctly prepends to all routes"
    (let [handler (router/create-ring-handler
                   :config {:dev-mode true}  ; Skip auth for simplicity
                   :prefix "/datahike")]

      ;; Test that route without prefix returns 404
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (= 404 (:status response)) "Route without prefix should not exist"))

      ;; Test that route with prefix works
      (let [response (handler {:uri "/datahike/database-exists"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 404 (:status response)) "Route with prefix should work"))))

  (testing "Nested prefix works correctly"
    (let [handler (router/create-ring-handler
                   :config {:dev-mode true}
                   :prefix "/api/v1/db")]

      ;; Test with nested prefix
      (let [response (handler {:uri "/api/v1/db/database-exists"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 404 (:status response)) "Nested prefix should work"))))

  (testing "Empty prefix works (routes at root)"
    (let [handler (router/create-ring-handler
                   :config {:dev-mode true})]  ; No prefix specified

      ;; Test at root level
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 404 (:status response)) "Routes should work at root with no prefix")))))

;; Integration test
(deftest test-embedded-usage
  (testing "Routes work in embedded mode"
    (let [cfg {:store {:backend :mem
                       :id "embedded-test"}}
          handler (router/create-ring-handler :include-writers? false)]

      ;; Create database
      (d/create-database cfg)

      ;; Test database-exists? through handler
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :body [cfg]})]
        (is (= 200 (:status response)))
        (is (true? (read-string (:body response)))))

      ;; Clean up
      (d/delete-database cfg))))

(deftest test-content-negotiation
  (testing "Router supports multiple serialization formats"
    (let [cfg {:store {:backend :mem :id "content-test"}}
          handler (router/create-ring-handler :include-writers? false)]

      ;; Create database for testing
      (d/create-database cfg)

      ;; Test EDN format (default)
      (let [response (handler {:uri "/database-exists"
                               :request-method :post
                               :headers {"content-type" "application/edn"
                                        "accept" "application/edn"}
                               :body (pr-str [cfg])})]
        (is (= 200 (:status response)))
        (is (= "application/edn" (get-in response [:headers "Content-Type"])))
        (is (string? (:body response)))
        (is (true? (read-string (:body response)))))

      ;; Test Transit+JSON format
      (let [out (java.io.ByteArrayOutputStream.)
            writer (cognitect.transit/writer out :json {:handlers datahike.transit/write-handlers})
            _ (cognitect.transit/write writer [cfg])
            response (handler {:uri "/database-exists"
                               :request-method :post
                               :headers {"content-type" "application/transit+json"
                                        "accept" "application/transit+json"}
                               :body (.toByteArray out)})]
        (is (= 200 (:status response)) "Transit request should succeed")
        (when (not= 200 (:status response))
          (println "Transit error:" (:body response)))
        (is (= "application/transit+json" (get-in response [:headers "Content-Type"])))
        (is (bytes? (:body response)))
        (let [in (java.io.ByteArrayInputStream. (:body response))
              reader (cognitect.transit/reader in :json {:handlers datahike.transit/read-handlers})]
          (is (true? (cognitect.transit/read reader)))))

      ;; Test JSON format - use the datahike JSON mapper
      (let [json-body (jsonista.core/write-value-as-bytes [cfg] datahike.json/mapper)
            response (handler {:uri "/database-exists"
                               :request-method :post
                               :headers {"content-type" "application/json"
                                        "accept" "application/json"}
                               :body json-body})]
        (is (= 200 (:status response)) "JSON request should succeed")
        (when (not= 200 (:status response))
          (println "JSON error body:" (jsonista.core/read-value (:body response) datahike.json/mapper)))
        (is (= "application/json" (get-in response [:headers "Content-Type"])))
        (is (bytes? (:body response)))
        (is (true? (jsonista.core/read-value (:body response) datahike.json/mapper))))

      ;; Clean up
      (d/delete-database cfg))))

;; Compatibility test with server
(deftest test-router-matches-server-routes
  (testing "Router routes match server routes exactly"
    (let [config {}
          ;; Get routes from server (uses eval approach)
          server-routes (server/create-routes config)

          ;; Extract route info from server (Reitit format)
          server-route-info (into #{}
                                  (map (fn [[path route-data]]
                                         (let [method (if (:get route-data) :get :post)]
                                           {:path path
                                            :method method
                                            :operation-id (get-in route-data [method :operationId])}))
                                       server-routes))

          ;; Get routes from router (raw format)
          router-routes (router/generate-api-routes)

          ;; Extract route info from router
          router-route-info (into #{}
                                  (map (fn [{:keys [path method name]}]
                                         {:path path
                                          :method method
                                          :operation-id (str (clojure.core/name name))})
                                       router-routes))]

      ;; Check counts match
      (is (= (count server-route-info) (count router-route-info))
          (str "Route counts should match. Server: " (count server-route-info)
               ", Router: " (count router-route-info)))

      ;; Check all server routes exist in router
      (is (set/subset? server-route-info router-route-info)
          "All server routes should exist in router routes")

      ;; Check all router routes exist in server
      (is (set/subset? router-route-info server-route-info)
          "All router routes should exist in server routes")

      ;; Verify they are identical
      (is (= server-route-info router-route-info)
          "Server and router routes should be identical")))

  (testing "Writer routes match between router and server"
    (let [server-connections (atom {})
          ;; Get writer routes from server
          server-writer-routes (server/internal-writer-routes server-connections)

          ;; Extract info from server writer routes
          server-writer-info (into #{}
                                   (map (fn [[path route-data]]
                                          {:path path
                                           :method :post
                                           :operation-id (get-in route-data [:post :operationId])})
                                        server-writer-routes))

          ;; Get writer routes from router
          router-writer-routes (router/generate-writer-routes)

          ;; Extract info from router writer routes
          router-writer-info (into #{}
                                   (map (fn [{:keys [path method name]}]
                                          {:path path
                                           :method method
                                           :operation-id (clojure.core/name name)})
                                        router-writer-routes))]

      ;; Check counts match
      (is (= 3 (count server-writer-info) (count router-writer-info))
          "Should have 3 writer routes in both")

      ;; Check paths match
      (is (= (set (map :path server-writer-info))
             (set (map :path router-writer-info)))
          "Writer route paths should match"))))

(deftest test-connection-registry-sync
  (testing "Connection registry maintains consistency across all connection types"
    (let [test-port 3030
          db-path (str "/tmp/router-test-sync-" (System/currentTimeMillis))
          handler (router/create-ring-handler :config {:dev-mode true})
          server (jetty/run-jetty handler {:port test-port :join? false})]

      (try
        (Thread/sleep 500) ; Let server start

        ;; Define configs for different connection types
        (let [http-client-cfg {:store {:backend :file :path db-path}
                               :remote-peer {:backend :datahike-server
                                           :url (str "http://localhost:" test-port)}
                               :allow-unsafe-config true}
              writer-cfg {:store {:backend :file :path db-path}
                         :writer {:backend :self}
                         :allow-unsafe-config true}
              remote-peer-cfg {:store {:backend :file :path db-path}
                              :remote-peer {:backend :datahike-server
                                          :url (str "http://localhost:" test-port)}
                              :allow-unsafe-config true}
              local-cfg {:store {:backend :file :path db-path}
                        :allow-unsafe-config true}]

          (try
            ;; Clean up
            (try (client/delete-database http-client-cfg) (catch Exception _))
            (router/clear-connections!)

            ;; Phase 1: HTTP CLIENT creates DB and writes
            (client/create-database http-client-cfg)
            (let [http-conn (client/connect http-client-cfg)]

              (client/transact http-conn [{:db/ident :test/source
                                          :db/valueType :db.type/string
                                          :db/cardinality :db.cardinality/one}
                                         {:db/ident :test/value
                                          :db/valueType :db.type/long
                                          :db/cardinality :db.cardinality/one}])

              (client/transact http-conn [{:test/source "http-client" :test/value 1}])

              ;; Phase 2: MAIN PROCESS accesses router connection
              (let [main-conn (router/get-connection local-cfg)]
                (is (some? main-conn) "Should retrieve connection from router registry")

                (let [main-db @main-conn
                      main-data (d/q '[:find ?source ?val
                                      :where [?e :test/source ?source]
                                             [?e :test/value ?val]]
                                    main-db)]
                  (is (= #{["http-client" 1]} main-data)
                      "MAIN should see HTTP CLIENT data"))

                ;; Phase 3: WRITER writes
                (let [writer-conn (d/connect writer-cfg)]
                  (d/transact writer-conn [{:test/source "writer" :test/value 2}])

                  ;; Verify MAIN sees WRITER data
                  (let [main-db-2 @main-conn
                        main-data-2 (d/q '[:find ?source ?val
                                          :where [?e :test/source ?source]
                                                 [?e :test/value ?val]]
                                        main-db-2)]
                    (is (= 2 (count main-data-2))
                        "MAIN should see both HTTP CLIENT and WRITER data"))

                  ;; Verify HTTP CLIENT sees WRITER data
                  (let [http-db-2 (client/db http-conn)
                        http-data-2 (client/q '[:find ?source ?val
                                               :where [?e :test/source ?source]
                                                      [?e :test/value ?val]]
                                             http-db-2)]
                    (is (= 2 (count http-data-2))
                        "HTTP CLIENT should see WRITER data"))

                  ;; Phase 4: REMOTE-PEER writes
                  (let [remote-peer-conn (d/connect remote-peer-cfg)]
                    (d/transact remote-peer-conn [{:test/source "remote-peer" :test/value 3}])

                    ;; Verify all processes see all data
                    (let [main-db-3 @main-conn
                          http-db-3 (client/db http-conn)
                          writer-db-3 @writer-conn
                          remote-db-3 @remote-peer-conn

                          main-data (d/q '[:find ?source ?val
                                          :where [?e :test/source ?source]
                                                 [?e :test/value ?val]]
                                        main-db-3)
                          http-data (client/q '[:find ?source ?val
                                               :where [?e :test/source ?source]
                                                      [?e :test/value ?val]]
                                             http-db-3)
                          writer-data (d/q '[:find ?source ?val
                                            :where [?e :test/source ?source]
                                                   [?e :test/value ?val]]
                                          writer-db-3)
                          remote-data (d/q '[:find ?source ?val
                                            :where [?e :test/source ?source]
                                                   [?e :test/value ?val]]
                                          remote-db-3)]

                      (is (= 3 (count main-data)) "MAIN should see 3 records")
                      (is (= 3 (count http-data)) "HTTP CLIENT should see 3 records")
                      (is (= 3 (count writer-data)) "WRITER should see 3 records")
                      (is (= 3 (count remote-data)) "REMOTE-PEER should see 3 records")

                      ;; All should see identical data
                      (is (= main-data http-data writer-data remote-data)
                          "All connection types should see identical data"))

                    ;; Phase 5: Cross-process writes
                    (d/transact main-conn [{:test/source "main" :test/value 100}])
                    (client/transact http-conn [{:test/source "http-round2" :test/value 200}])
                    (d/transact writer-conn [{:test/source "writer-round2" :test/value 300}])
                    (d/transact remote-peer-conn [{:test/source "remote-round2" :test/value 400}])

                    ;; Final verification: all see all 7 records
                    (let [final-main (d/q '[:find ?source :where [_ :test/source ?source]] @main-conn)
                          final-http (client/q '[:find ?source :where [_ :test/source ?source]]
                                              (client/db http-conn))
                          final-writer (d/q '[:find ?source :where [_ :test/source ?source]] @writer-conn)
                          final-remote (d/q '[:find ?source :where [_ :test/source ?source]] @remote-peer-conn)]

                      (is (= 7 (count final-main)) "MAIN should see all 7 records")
                      (is (= 7 (count final-http)) "HTTP CLIENT should see all 7 records")
                      (is (= 7 (count final-writer)) "WRITER should see all 7 records")
                      (is (= 7 (count final-remote)) "REMOTE-PEER should see all 7 records")

                      (is (= final-main final-http final-writer final-remote)
                          "All processes maintain perfect consistency"))

                    ;; Test store identity vector access
                    (let [store-id (ds/store-identity (:store local-cfg))
                          conn-by-id (router/get-connection store-id)]
                      (is (some? conn-by-id) "Should retrieve connection by store identity vector")
                      (is (identical? main-conn conn-by-id)
                          "Both access methods should return same connection object"))))))

            (finally
              (try (client/delete-database http-client-cfg) (catch Exception _))
              (router/clear-connections!))))

        (finally
          (.stop server))))))