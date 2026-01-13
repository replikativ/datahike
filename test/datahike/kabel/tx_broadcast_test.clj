(ns datahike.kabel.tx-broadcast-test
  "Tests for tx-report broadcasting via kabel.pubsub."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.kabel.tx-broadcast :as tx-broadcast]
            [kabel.peer :as peer]
            [kabel.http-kit :refer [create-http-kit-handler!]]
            [kabel.pubsub :as pubsub]
            [superv.async :refer [<?? S go-try]]
            [clojure.core.async :refer [<!! >!! chan timeout alts!! promise-chan put!]]))

(def server-id #uuid "10000000-0000-0000-0000-000000000001")
(def client-id #uuid "20000000-0000-0000-0000-000000000002")
(def store-id #uuid "30000000-0000-0000-0000-000000000003")

(defn- get-free-port []
  (let [socket (java.net.ServerSocket. 0)]
    (try
      (.getLocalPort socket)
      (finally
        (.close socket)))))

(deftest test-tx-report-topic-naming
  (testing "tx-report-topic creates correct keyword with scope- prefix"
    (is (= (keyword "tx-report" "scope-30000000-0000-0000-0000-000000000003")
           (tx-broadcast/tx-report-topic store-id)))
    (is (= :tx-report/scope-my-scope
           (tx-broadcast/tx-report-topic "my-scope")))))

(deftest test-tx-report-broadcasting
  (testing "Server publishes tx-report and client receives it"
    (let [port (get-free-port)
          url (str "ws://localhost:" port)
          received-reports (atom [])
          ready-ch (promise-chan)

          ;; Create server peer with pubsub middleware
          handler (create-http-kit-handler! S url server-id)
          server (peer/server-peer S handler server-id
                                   (pubsub/make-pubsub-peer-middleware {})
                                   identity)
          _ (<?? S (peer/start server))

          ;; Register tx-report topic
          _ (tx-broadcast/register-tx-report-topic! server store-id)

          ;; Create client peer with pubsub middleware
          client (peer/client-peer S client-id
                                   (pubsub/make-pubsub-peer-middleware {})
                                   identity)
          _ (<?? S (peer/connect S client url))

          ;; Give connection time to establish
          _ (<?? S (timeout 200))

          ;; Subscribe to tx-reports
          on-tx-report (fn [payload]
                         (swap! received-reports conj payload)
                         (put! ready-ch :received))
          _ (<?? S (tx-broadcast/subscribe-tx-reports! client store-id on-tx-report))

          ;; Give subscription time to establish
          _ (<?? S (timeout 200))

          ;; Create mock tx-report
          mock-tx-report {:db-before {:max-tx 0}
                          :db-after {:max-tx 1}
                          :tx-data [[:db/add 1 :name "Alice"]]}
          request-id #uuid "40000000-0000-0000-0000-000000000004"]

      ;; Publish tx-report from server
      (<?? S (tx-broadcast/publish-tx-report! server store-id mock-tx-report request-id))

      ;; Wait for client to receive (with timeout)
      (let [[_ ch] (alts!! [ready-ch (timeout 5000)])]
        (is (= ready-ch ch) "Client should receive tx-report"))

      ;; Verify received data
      (is (= 1 (count @received-reports)))
      (let [received (first @received-reports)]
        (is (= store-id (:store-id received)))
        (is (= request-id (:request-id received)))
        (is (= mock-tx-report (:tx-report received))))

      ;; Cleanup
      (<?? S (tx-broadcast/unsubscribe-tx-reports! client store-id))
      (<?? S (peer/stop server)))))

(deftest test-deduplication-handler
  (testing "make-tx-report-handler skips own transactions"
    (let [pending-ids (atom #{#uuid "11111111-0000-0000-0000-000000000001"})
          received (atom [])
          handler (tx-broadcast/make-tx-report-handler
                   pending-ids
                   (fn [tx-report] (swap! received conj tx-report)))

          tx-report {:db-after {:max-tx 42}}]

      ;; Own transaction - should be skipped
      (handler {:tx-report tx-report
                :request-id #uuid "11111111-0000-0000-0000-000000000001"})
      (is (empty? @received) "Own transaction should be skipped")
      (is (empty? @pending-ids) "request-id should be removed from pending")

      ;; Remote transaction - should be processed
      (handler {:tx-report tx-report
                :request-id #uuid "22222222-0000-0000-0000-000000000002"})
      (is (= 1 (count @received)) "Remote transaction should be processed")
      (is (= tx-report (first @received))))))
