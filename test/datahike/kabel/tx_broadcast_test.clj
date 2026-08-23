(ns datahike.kabel.tx-broadcast-test
  "Tests for tx-report broadcasting via kabel.pubsub."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.kabel.tx-broadcast :as tx-broadcast]
            [datahike.kabel.cbor-handlers :as ch]
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
          ;; The datahike CBOR middleware, not `identity` — which is what a real
          ;; server runs, and it is the codec that projects a TxReport's live
          ;; DBs to stored form on the way out. With the default EDN serializer
          ;; the record went out as an unreadable tagged literal, which is a
          ;; property of the stub rather than of the system.
          server (peer/server-peer S handler server-id
                                   (pubsub/make-pubsub-peer-middleware {})
                                   ch/datahike-cbor-middleware)
          _ (<?? S (peer/start server))

          ;; Register tx-report topic
          _ (tx-broadcast/register-tx-report-topic! server store-id)

          ;; Create client peer with pubsub middleware
          client (peer/client-peer S client-id
                                   (pubsub/make-pubsub-peer-middleware {})
                                   ch/datahike-cbor-middleware)
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

          ;; A real TxReport, not a map with the right keys. The publish site
          ;; only broadcasts to the tx-report topic when the value IS one —
          ;; because the server's single dispatch handler also routes
          ;; `gc-storage!` and the secondary-index ops through here, and those
          ;; are not transactions. The stub's `:db-before`/`:db-after` stay
          ;; fake maps rather than real DBs on purpose: the codec projection is
          ;; `dbu/db?`-guarded, so it leaves a non-DB alone, and that guard is
          ;; worth exercising.
          mock-tx-report (datahike.db.TxReport.
                          {:max-tx 0} {:max-tx 1}
                          [[:db/add 1 :name "Alice"]] {} nil)
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
        ;; Compared as a MAP: a TxReport is read back as a plain map by design
        ;; (see `datahike.cbor`), and a record is never `=` to a map, so the
        ;; projection is the honest expectation for what crossed the wire.
        (is (= (into {} mock-tx-report) (:tx-report received))))

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

(deftest an-op-that-made-no-transaction-is-not-broadcast-as-one
  (testing "the server has ONE global dispatch handler, so the publish site is
            handed whatever the op returned — and `default-write-fn-map` holds
            three ops that produce no tx-report. `gc-storage!` returns a SET of
            freed keys, which threw outright (`dissoc` on a set is a
            ClassCastException), so `d/gc-storage` over a kabel peer died at
            the broadcast. The two secondary-index ops return status MAPS,
            which failed the other way: they went out on the tx-report topic
            for a subscriber to read as transactions."
    (let [store-id (random-uuid)]
      (doseq [[label result] [["gc-storage! — a set of freed keys" (set (repeatedly 2 random-uuid))]
                              ["build-secondary-index! — a status map" {:attr :foo/bar :status :done}]
                              ["install-secondary-index! — a status map" {:attr :foo/bar :installed? true}]]]
        (testing label
          (let [published (atom [])]
            (with-redefs [pubsub/publish! (fn [_peer _topic payload]
                                            (swap! published conj payload)
                                            :published)]
              (let [res (<?? S (tx-broadcast/publish-tx-report! nil store-id result nil))]
                (is (empty? @published)
                    (str "nothing may go out on the tx-report topic for this op, but got: "
                         (pr-str @published)))
                (is (= :not-a-tx-report (:skipped res))
                    "and the skip is reported rather than silent")
                (is (zero? (:sent-count res)))))))))))

(deftest a-broadcast-does-not-carry-the-import-id-map
  (testing ":migration is an import's source-id -> target-id map, threaded from
            batch to batch on the tx-report because the writer owns the db and
            the caller cannot reach into its loop. The RETURN to the calling
            peer needs it — that is how the next batch gets its mapping — but a
            SUBSCRIBER does not: nothing reads it off a broadcast. Under the
            default `:eids :allocate` it holds one entry per source entity, and
            it accumulates across batches, so leaving it in fans an import's
            bookkeeping out to every peer: measured at 119 KB of wire for 20 000
            entities."
    (let [captured (atom nil)
          migration {:eids {1 100, 2 200, 3 300} :tids {536870913 536870913}}
          ;; A real TxReport, not a map stub. The publish path now relies on
          ;; `dissoc` leaving the RECORD intact so the codec's type-keyed tag-27
          ;; handler still fires on the payload — a map stub cannot show that,
          ;; and `:migration` is an extension key rather than one of the five
          ;; fields, which is exactly why the dissoc is safe.
          rep (assoc (datahike.db.TxReport. nil nil [] {} nil) :migration migration)]

      (with-redefs [pubsub/publish! (fn [_peer _topic payload]
                                      (reset! captured payload)
                                      :published)]
        (tx-broadcast/publish-tx-report! nil store-id rep nil))

      (testing "stripped from what goes out to subscribers"
        (is (some? @captured) "the stub must have been reached")
        (is (not (contains? (:tx-report @captured) :migration))
            (str "the broadcast payload still carries the id map: "
                 (pr-str (:migration (:tx-report @captured))))))

      (testing "and everything else a subscriber does read survives"
        (is (contains? (:tx-report @captured) :tx-data))
        (is (= store-id (:store-id @captured))))

      (testing "while the report itself keeps it — strip it on the RETURN too and
                a remote import loses its mapping between batches. Asserted on
                the record rather than on a projection because there is no
                longer a projection here: stripping the live DBs is the codec's
                job, done by the tag-27 handler `datahike.cbor` registers for
                the TxReport CLASS. Projecting at the publish site instead meant
                projecting every writer op's result, and `gc-storage!` returns a
                set."
        (is (= migration (:migration rep)))
        (is (instance? datahike.db.TxReport (dissoc rep :migration))
            "and `dissoc` on an extension key leaves a TxReport, so the codec's
             type-keyed handler still fires on the payload")))))
