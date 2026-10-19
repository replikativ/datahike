(ns datahike.kabel.cleanup-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as a]
            [datahike.kabel.cleanup :as cleanup]
            [datahike.kabel.writer :as writer]
            [datahike.kabel.connector :as connector]
            [datahike.cbor :as cbor]
            [datahike.store :as datahike-store]
            [konserve.store :as store]))

(defn- ready [value]
  (let [ch (a/promise-chan)]
    (if (some? value) (a/put! ch value) (a/close! ch))
    ch))
(defn- take! [ch]
  (let [[value port] (a/alts!! [ch (a/timeout 3000)])]
    (when-not (= port ch) (throw (ex-info "Cleanup test event did not arrive" {})))
    value))

(deftest failed-drain-retains-store-and-exact-owner-until-explicit-recovery
  (let [owner (cleanup/owner) strategy (Object.) native-store (Object.)
        error (ex-info "Constructor failed" {:type :fixture})
        drain-error (ex-info "Drain unavailable" {}) attempts (atom 0) events (atom [])]
    (swap! (:state owner) assoc :strategy strategy :store native-store :store-config {:backend :memory}
           :peer :peer :topic :topic :retire-initial! #(swap! events conj :fenced))
    (with-redefs [writer/await-topic-release! (fn [peer topic timeout-ms expected]
                                                (is (= [:peer :topic 5000] [peer topic timeout-ms]))
                                                (is (identical? strategy expected))
                                                (swap! events conj :drain)
                                                (ready (if (= 1 (swap! attempts inc)) drain-error true)))
                  store/release-store (fn [_ actual _]
                                        (is (identical? native-store actual))
                                        (swap! events conj :released) (ready nil))
                  cbor/unregister-store! (fn [& _] (throw (AssertionError. "Shared storage must not be unregistered")))]
      (let [failure (take! (cleanup/failure! owner error))]
        (is (= :fixture (:type (ex-data failure))))
        (is (not (cleanup/confirmed? failure)))
        (is (= [:fenced :drain] @events))
        (is (identical? native-store (:store @(:state owner))))
        (is (true? (take! (cleanup/retry! failure))))
        (is (= [:fenced :drain :fenced :drain :released] @events))
        (is (true? (take! (cleanup/retry! failure))))
        (is (= 2 @attempts))
        (is (= 1 (count (filter #{:released} @events))))))))

(deftest store-release-retry-does-not-retire-todays-topic-again
  (let [owner (cleanup/owner) drains (atom 0) releases (atom 0)
        failure (ex-info "Store release failed" {})]
    (swap! (:state owner) assoc :strategy :original :peer :peer :topic :topic :store :store :store-config {})
    (with-redefs [writer/await-topic-release! (fn [& _] (swap! drains inc) (ready true))
                  store/release-store (fn [& _] (ready (when (= 1 (swap! releases inc)) failure)))]
      (let [error (take! (cleanup/failure! owner (ex-info "Open failed" {})))]
        (is (not (cleanup/confirmed? error)))
        (is (true? (take! (cleanup/retry! error))))
        (is (= 1 @drains))
        (is (= 2 @releases))))))

(deftest pre-resource-failure-positively-confirms-empty-cleanup
  (let [error (take! (cleanup/failure! (cleanup/owner) (ex-info "Invalid config" {:type :bad-config})))]
    (is (cleanup/confirmed? error))
    (is (= :bad-config (:type (ex-data error))))))

(defn- fixture-config []
  {:store {:backend :memory :id (random-uuid)}
   :writer {:backend :kabel :local-peer (atom {}) :peer-id (random-uuid)}
   :schema-flexibility :write :keep-history? true})

(deftest constructor-captures-raw-resource-before-handler-wrapping
  (let [config (fixture-config) raw-store (Object.) released (atom [])
        cause (ex-info "Handler wrapping failed" {:type ::wrapper-failed})]
    (with-redefs [store/store-exists? (fn [& _] (ready false))
                  store/create-store (fn [& _] (ready raw-store))
                  datahike-store/add-cache-and-handlers (fn [& _] (throw cause))
                  store/release-store (fn [_ actual _] (swap! released conj actual) (ready nil))]
      (let [failure (take! (connector/connect-kabel config {:sync? false}))]
        (is (= ::wrapper-failed (:type (ex-data failure))))
        (is (connector/cleanup-confirmed? failure))
        (is (= 1 (count @released)))
        (is (identical? raw-store (first @released)))))))

(deftest actual-constructor-retains-native-recovery-through-async-error-wrapping
  (let [config (fixture-config) raw-store (Object.) releases (atom 0)
        cause (ex-info "Handler wrapping failed" {:type ::wrapper-failed})
        release-error (ex-info "Native release unavailable" {})]
    (with-redefs [store/store-exists? (fn [& _] (ready false))
                  store/create-store (fn [& _] (ready raw-store))
                  datahike-store/add-cache-and-handlers (fn [& _] (throw cause))
                  store/release-store (fn [_ actual _]
                                        (is (identical? raw-store actual))
                                        (ready (when (= 1 (swap! releases inc)) release-error)))]
      (let [failure (take! (connector/connect-kabel config {:sync? false}))]
        (is (= ::wrapper-failed (:type (ex-data failure))))
        (is (not (connector/cleanup-confirmed? failure)))
        (is (true? (take! (connector/recover-connect! failure))))
        (is (= 2 @releases))
        (is (true? (take! (connector/recover-connect! failure))))
        (is (= 2 @releases))))))
