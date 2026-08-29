(ns datahike.test.http.health-test
  (:require [babashka.http-client :as http]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.connections :as connections]
            [datahike.connector :as connector]
            [datahike.http.permissions :as permissions]
            [datahike.http.server :as server]
            [datahike.http.system :as system]
            [konserve.core :as k]))

(def token "health-test-token")

(defn- request [handler uri]
  (handler {:request-method :get :uri uri :headers {}}))

(defn- body [response]
  (slurp (:body response)))

(deftest health-endpoints-are-public-and-plain-text
  (let [handler (server/app {:token token :metrics false} (atom {}))
        live    (request handler "/health/live")
        ready   (request handler "/health/ready")]
    (testing "orchestrators do not need API credentials"
      (is (= 200 (:status live)))
      (is (= 200 (:status ready))))
    (is (= "text/plain;charset=utf-8" (get-in live [:headers "content-type"])))
    (is (= "live\n" (body live)))
    (is (= "ready\n" (body ready)))))

(deftest liveness-is-usable-over-http-without-credentials
  (let [instance (server/start-server {:port 0 :join? false :token token :metrics false})]
    (try
      (let [connector (first (.getConnectors instance))
            port      (.getLocalPort ^org.eclipse.jetty.server.ServerConnector connector)
            response  (http/get (str "http://localhost:" port "/health/live")
                                {:throw false :as :string})]
        (is (= 200 (:status response)))
        (is (= "text/plain;charset=utf-8" (get-in response [:headers "content-type"])))
        (is (= "live\n" (:body response))))
      (finally
        (server/stop-server instance)))))

(deftest readiness-checks-live-and-system-stores
  (let [data-id   #uuid "7bea093b-af2e-49a2-a02e-7cf4be87a77b"
        data-conn (connector/conn-from-db {:store ::data-store})
        auth-conn (connector/conn-from-db {:store ::auth-store})
        connections (atom {[data-id :db] {:conn data-conn :count 1}})
        probed      (atom [])]
    (with-redefs [system/configure
                  #(assoc % system/conn-key auth-conn)
                  permissions/configure identity
                  connector/deref-conn
                  (fn [_]
                    (throw (ex-info "readiness must not refresh a connection" {})))
                  k/get
                  (fn [store key not-found opts]
                    (swap! probed conj [store key not-found opts])
                    nil)]
      (let [handler  (server/app {:token token :metrics false :system-db {}} connections)
            response (request handler "/health/ready")]
        (is (= 200 (:status response)))
        (is (= #{::data-store ::auth-store} (set (map first @probed))))
        (is (every? #(= {:sync? true} (nth % 3)) @probed))))))

(deftest readiness-probes-a-real-datahike-store
  (let [config {:store {:backend :memory
                        :id      #uuid "cf570585-d8b7-4fcd-8fab-d4bb866eb255"}}
        registry (atom {})]
    (binding [connections/*connections* registry]
      (try
        (d/create-database config)
        (let [conn     (d/connect config)
              handler  (server/app {:token token :metrics false} registry)
              response (request handler "/health/ready")]
          (is (= 200 (:status response)))
          (is (= "ready\n" (body response)))
          (d/release conn))
        (finally
          (when (d/database-exists? config)
            (d/delete-database config)))))))

(deftest readiness-checks-all-stores-and-hides-failure-details
  (let [healthy-id #uuid "df41d1cc-0ef3-4894-b00f-77334d09c3b0"
        broken-id  #uuid "d5190805-a171-4eb4-83a8-d184f9a82a43"
        connections (atom {[healthy-id :db] {:conn (connector/conn-from-db {:store ::healthy}) :count 1}
                           [broken-id :db]  {:conn (connector/conn-from-db {:store ::broken}) :count 1}
                           [(random-uuid) :db] {:conn nil :count 0}})
        probed      (atom [])]
    (with-redefs [k/get
                  (fn [store _key _not-found _opts]
                    (swap! probed conj store)
                    (when (= ::broken store)
                      (throw (ex-info "secret backend failure" {:credentials "must-not-leak"}))))]
      (let [handler  (server/app {:token token :metrics false} connections)
            response (request handler "/health/ready")]
        (is (= 503 (:status response)))
        (is (= "not ready\n" (body response)))
        (is (= #{::healthy ::broken} (set @probed))
            "a failed store does not prevent the remaining probes")
        (is (not (re-find #"secret|credentials" (body response))))))))
