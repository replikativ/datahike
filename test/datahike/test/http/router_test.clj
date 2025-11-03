(ns datahike.test.http.router-test
  (:require
   [clojure.test :refer :all]
   [datahike.http.router :as router]
   [datahike.api :as d]))

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
      (let [response (handler {:uri "/database-exists?"
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
      (let [response (handler {:uri "/database-exists?"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (= 401 (:status response)) "Should require authentication")
        (is (= {:error "Not authorized"} (:body response))))

      ;; Test with wrong token - should fail
      (let [response (handler {:uri "/database-exists?"
                               :request-method :post
                               :headers {"authorization" "token wrong-token"}
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (= 401 (:status response)) "Wrong token should fail"))

      ;; Test with correct token - should succeed
      (let [response (handler {:uri "/database-exists?"
                               :request-method :post
                               :headers {"authorization" "token test-token"}
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 401 (:status response)) "Correct token should succeed"))

      ;; Test with Bearer format - should also work
      (let [response (handler {:uri "/database-exists?"
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
      (let [response (handler {:uri "/database-exists?"
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
      (let [response (handler {:uri "/database-exists?"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (= 404 (:status response)) "Route without prefix should not exist"))

      ;; Test that route with prefix works
      (let [response (handler {:uri "/datahike/database-exists?"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 404 (:status response)) "Route with prefix should work"))))

  (testing "Nested prefix works correctly"
    (let [handler (router/create-ring-handler
                   :config {:dev-mode true}
                   :prefix "/api/v1/db")]

      ;; Test with nested prefix
      (let [response (handler {:uri "/api/v1/db/database-exists?"
                               :request-method :post
                               :body [{:store {:backend :mem
                                               :id "test"}}]})]
        (is (not= 404 (:status response)) "Nested prefix should work"))))

  (testing "Empty prefix works (routes at root)"
    (let [handler (router/create-ring-handler
                   :config {:dev-mode true})]  ; No prefix specified

      ;; Test at root level
      (let [response (handler {:uri "/database-exists?"
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
      (let [response (handler {:uri "/database-exists?"
                               :request-method :post
                               :body [cfg]})]
        (is (= 200 (:status response)))
        (is (true? (:body response))))

      ;; Clean up
      (d/delete-database cfg))))