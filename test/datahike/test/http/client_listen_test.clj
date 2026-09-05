(ns datahike.test.http.client-listen-test
  "The JVM thin client's SSE change listener."
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.connections :refer [*connections*]]
   [datahike.datom :as datom]
   [datahike.http.client :as client]
   [datahike.http.routes :as routes]
   [ring.adapter.jetty :refer [run-jetty]])
  (:import
   [datahike.remote RemoteDB]
   [java.net ServerSocket]))

(def ^:private token "client-listen-test-token")

(defn- free-port []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn- take-with-timeout [ch timeout-ms]
  (let [timeout (async/timeout timeout-ms)
        [value port] (async/alts!! [ch timeout])]
    (if (= port timeout) ::timeout value)))

(defn- test-system []
  (let [port (free-port)
        store-id (random-uuid)
        cfg {:store {:backend :memory :id store-id} :schema-flexibility :read}
        connections (atom {})
        handler (routes/handler {:token token} {:connections connections})
        attempts (atom 0)
        app (fn [request]
              (when (= "/listen" (:uri request))
                (swap! attempts inc))
              (handler request))
        server (run-jetty app {:host "127.0.0.1" :port port :join? false})
        peer {:backend :datahike-server
              :url (str "http://127.0.0.1:" port)
              :token token
              :format :cbor}]
    (binding [*connections* connections]
      (d/create-database cfg))
    {:attempts attempts
     :cfg cfg
     :connections connections
     :handler handler
     :peer peer
     :server server}))

(defn- stop-system! [{:keys [cfg connections handler peer server]}]
  (try
    (client/delete-database (assoc cfg :remote-peer peer))
    (catch Throwable _))
  (.stop server)
  (routes/release-all! handler)
  (binding [*connections* connections]
    (try
      (d/delete-database cfg)
      (catch Throwable _))))

(deftest jvm-client-listens-unlistens-and-sees-deletion
  (let [{:keys [cfg peer] :as system} (test-system)
        conn (client/connect (assoc cfg :remote-peer peer))
        events (async/chan 10)]
    (try
      (let [key (client/listen conn #(async/put! events %))]
        (testing "the first callback is a resync with a usable remote DB"
          (let [resync (take-with-timeout events 5000)]
            (is (not= ::timeout resync))
            (is (= true (:resync resync)))
            (is (instance? RemoteDB (:db-after resync)))))

        (testing "a client transaction delivers datoms and a queryable db-after"
          (client/transact conn [{:name "Ada"}])
          (let [report (take-with-timeout events 5000)]
            (is (not= ::timeout report))
            (is (seq (:tx-data report)))
            (is (every? datom/datom? (:tx-data report)))
            (is (= #{["Ada"]}
                   (client/q '[:find ?name :where [?e :name ?name]]
                             (:db-after report))))))

        (testing "unlisten stops delivery"
          (client/unlisten conn key)
          (client/transact conn [{:name "Grace"}])
          (is (= ::timeout (take-with-timeout events 500)))))

      (testing "database deletion is terminal"
        (client/listen conn ::deletion #(async/put! events %))
        (is (= true (:resync (take-with-timeout events 5000))))
        (client/release conn)
        (client/delete-database (assoc cfg :remote-peer peer))
        (is (= {:deleted true} (take-with-timeout events 5000))))
      (finally
        (client/unlisten conn ::deletion)
        (try (client/release conn) (catch Throwable _))
        (stop-system! system)))))

(deftest authentication-failure-is-terminal
  (let [{:keys [attempts cfg peer] :as system} (test-system)
        bad-peer (assoc peer :token "wrong")
        conn (client/connect (assoc cfg :remote-peer peer))
        events (async/chan 10)]
    (try
      (client/listen (assoc conn :remote-peer bad-peer) ::bad-token
                     #(async/put! events %))
      (let [failure (take-with-timeout events 5000)
            attempts-after-error @attempts]
        (is (instance? clojure.lang.ExceptionInfo (:error failure)))
        (is (= 401 (:status failure)))
        (is (= ::timeout (take-with-timeout events 750)))
        (is (= attempts-after-error @attempts)
            "a terminal authentication failure is not retried"))
      (finally
        (client/unlisten (assoc conn :remote-peer bad-peer) ::bad-token)
        (try (client/release conn) (catch Throwable _))
        (stop-system! system)))))
