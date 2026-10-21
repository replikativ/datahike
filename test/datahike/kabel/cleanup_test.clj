(ns datahike.kabel.cleanup-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as a]
            [datahike.kabel.cleanup :as cleanup]
            [datahike.kabel.writer :as writer]
            [datahike.kabel.connector :as connector]
            [datahike.cbor :as cbor]
            [datahike.store :as datahike-store]
            [datahike.store-registry :as registry]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]
            [datahike.connections :as connections]
            [datahike.connector :as native-connector]
            [konserve-sync.pubsub :as sync]
            [kabel.pubsub :as pubsub]
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
           :drain! #(writer/await-topic-release! :peer :topic 5000 strategy)
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
    (swap! (:state owner) assoc :drain! #(writer/await-topic-release! :peer :topic 5000 :original))
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

(deftest registration-and-native-close-follow-exact-drain
  (let [id (random-uuid) store-object (Object.) owner (cleanup/owner)
        lease (registry/acquire! id store-object) drain (a/promise-chan)
        failed (ex-info "Drain failed" {}) next-drain (atom drain) events (atom [])]
    (swap! (:state owner) assoc :registration lease :store store-object :store-config {:id id}
           :drain! #(do (swap! events conj :fenced) @next-drain))
    (with-redefs [store/release-store (fn [& _]
                                        (is (nil? (nodes/registered-storage id)))
                                        (swap! events conj :closed) (ready nil))]
      (let [result (cleanup/cleanup! owner)]
        (is (identical? result (cleanup/cleanup! owner)))
        (is (identical? store-object (nodes/registered-storage id)))
        (is (= [:fenced] @events))
        (a/put! drain failed)
        (is (= (ex-message failed) (ex-message (take! result))))
        (is (identical? store-object (nodes/registered-storage id)))
        (is (not-any? #{:closed} @events))
        (reset! next-drain (ready true))
        (is (true? (take! (cleanup/cleanup! owner))))
        (is (nil? (nodes/registered-storage id)))
        (is (= [:fenced :fenced :closed] @events))
        (is (true? (take! (cleanup/cleanup! owner))))
        (is (= [:fenced :fenced :closed] @events))
        (is (nil? (:store @(:state owner))))))))

(deftest synchronous-fencing-errors-are-delivered-and-retryable
  (doseq [callback [:retire-initial! :drain!]]
    (let [owner (cleanup/owner) calls (atom 0) failure (ex-info "Synchronous fence failure" {})]
      (swap! (:state owner) assoc callback
             #(if (= 1 (swap! calls inc)) (throw failure) (ready true)))
      (let [result (cleanup/cleanup! owner)]
        (is (identical? failure (take! result)))
        (is (= :error (get-in @(:state owner) [:cleanup :phase])))
        (is (true? (take! (cleanup/cleanup! owner))))
        (is (= 2 @calls))))))

(deftest false-and-closed-drains-do-not-release-registration-or-store
  (doseq [unconfirmed [nil false]]
    (let [id (random-uuid) object (Object.) owner (cleanup/owner)
          lease (registry/acquire! id object)]
      (swap! (:state owner) assoc :registration lease :store object :store-config {:id id}
             :drain! #(ready unconfirmed))
      (with-redefs [store/release-store (fn [& _] (throw (AssertionError. "Unconfirmed store close")))]
        (is (= :datahike.kabel/unconfirmed-drain
               (:type (ex-data (take! (cleanup/cleanup! owner))))))
        (is (identical? object (nodes/registered-storage id))))
      (swap! (:state owner) assoc :drain! #(ready true))
      (with-redefs [store/release-store (fn [& _] (ready nil))]
        (is (true? (take! (cleanup/cleanup! owner))))))))

(deftest malformed-synchronous-drain-results-fail-closed
  (doseq [unconfirmed [nil false true (Object.)]]
    (let [id (random-uuid) object (Object.) owner (cleanup/owner)]
      (swap! (:state owner) assoc :registration (registry/acquire! id object)
             :store object :store-config {:id id} :drain! (constantly unconfirmed))
      (with-redefs [store/release-store (fn [& _] (throw (AssertionError. "Unconfirmed store close")))]
        (is (= :datahike.kabel/unconfirmed-drain
               (:type (ex-data (take! (cleanup/cleanup! owner))))))
        (is (identical? object (nodes/registered-storage id))))
      (swap! (:state owner) assoc :drain! #(ready true))
      (with-redefs [store/release-store (fn [& _] (ready nil))]
        (is (true? (take! (cleanup/cleanup! owner))))))))

(deftest one-throwing-watch-cannot-strand-other-invalidated-owners
  (binding [connections/*connections* (atom {})]
    (let [id (random-uuid) calls (atom #{})
          conns (mapv (fn [branch]
                        (let [state (atom {:branch branch}) conn (native-connector/->Connection state)]
                          (alter-meta! state assoc :datahike/retire!
                                       #(do (swap! calls conj branch) (ready true)))
                          (add-watch state ::broken-observer
                                     (fn [& _] (throw (ex-info "Broken observation watch" {}))))
                          (swap! connections/*connections* assoc [id branch] {:conn conn :count 1})
                          conn))
                      [:a :b])]
      (is (= :datahike/retirement-errors
             (:type (ex-data (try (connections/invalidate-store-connections! id)
                                  (catch Exception error error))))))
      (is (= #{:a :b} @calls))
      (is (every? #(= :released @(:wrapped-atom %)) conns))
      (is (empty? @connections/*connections*)))))

(deftest failed-constructor-releases-its-registration-after-pre-subscribe-failure
  (let [config (fixture-config) id (get-in config [:store :id]) object (Object.) releases (atom 0)]
    (with-redefs [store/store-exists? (fn [& _] (ready false))
                  store/create-store (fn [& _] (ready object))
                  datahike-store/add-cache-and-handlers (fn [& _] object)
                  datahike-store/ready-store (fn [& _] (ready true))
                  sync/store-sync-strategy (fn [& _]
                                             (is (identical? object (nodes/registered-storage id)))
                                             (throw (ex-info "Strategy setup failed" {:type ::strategy-failed})))
                  store/release-store (fn [& _]
                                        (is (nil? (nodes/registered-storage id)))
                                        (swap! releases inc) (ready nil))]
      (let [failure (take! (connector/connect-kabel config {:sync? false}))]
        (is (= ::strategy-failed (:type (ex-data failure))))
        (is (connector/cleanup-confirmed? failure))
        (is (= 1 @releases))
        (is (nil? (nodes/registered-storage id)))))))

(deftest refused-initial-subscription-drains-only-its-captured-strategy
  (let [config (fixture-config) id (get-in config [:store :id]) object (Object.)
        strategy (Object.) drained (atom [])]
    (with-redefs [store/store-exists? (fn [& _] (ready false))
                  store/create-store (fn [& _] (ready object))
                  datahike-store/add-cache-and-handlers (fn [& _] object)
                  datahike-store/ready-store (fn [& _] (ready true))
                  sync/store-sync-strategy (fn [& _] strategy)
                  pubsub/subscribe! (fn [& _] (ready {:error (ex-info "Subscription refused" {:type ::refused})}))
                  writer/await-topic-release! (fn [_ topic _ expected]
                                                (is (= id topic))
                                                (is (identical? strategy expected))
                                                (is (identical? object (nodes/registered-storage id)))
                                                (swap! drained conj expected) (ready true))
                  store/release-store (fn [& _]
                                        (is (nil? (nodes/registered-storage id))) (ready nil))]
      (let [failure (take! (connector/connect-kabel config {:sync? false}))]
        (is (= ::refused (:type (ex-data failure))))
        (is (connector/cleanup-confirmed? failure))
        (is (= [strategy] @drained))
        (is (nil? (nodes/registered-storage id)))))))

(deftest deletion-and-invalidation-retire-the-exact-connection-owner
  (doseq [invalidate? [false true]]
    (binding [connections/*connections* (atom {})]
      (let [id (random-uuid) conn-id [id :db] state (atom {:config {:store {:id id}}})
            conn (native-connector/->Connection state) calls (atom 0)]
        (alter-meta! state assoc :datahike/retire!
                     #(do (is (= :released @state)) (swap! calls inc) (ready true)))
        (swap! connections/*connections* assoc conn-id {:conn conn :count 1})
        (if invalidate?
          (connections/invalidate-store-connections! id)
          (connections/delete-connection! conn-id))
        (is (= 1 @calls))
        (is (= :released @state))
        (connections/delete-connection! conn-id)
        (is (= 1 @calls))))))

(deftest generic-release-never-closes-a-kabel-store-before-owned-cleanup
  (binding [connections/*connections* (atom {})]
    (let [id (random-uuid) config {:backend :memory :id id}
          conn-id [(datahike-store/store-identity config) :db]
          state (atom {:config {:store config :branch :db} :store :native-store})
          conn (native-connector/->Connection state) retired (a/promise-chan) calls (atom 0)]
      (alter-meta! state assoc :datahike/retire! #(do (swap! calls inc) retired))
      (swap! connections/*connections* assoc conn-id {:conn conn :count 1})
      (with-redefs [store/release-store (fn [& _] (throw (AssertionError. "Premature native close")))]
        (is (nil? (native-connector/release conn)))
        (is (= 1 @calls))
        (is (identical? retired (connector/await-release! conn)))))))

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
