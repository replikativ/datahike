(ns datahike.kabel.subscription-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as a]
            [datahike.kabel.subscription :as sub]
            [kabel.pubsub :as pubsub]
            [konserve-sync.pubsub :as sync]))

(defn- take! [ch]
  (let [[value port] (a/alts!! [ch (a/timeout 2000)])]
    (when-not (= ch port) (throw (ex-info "Test operation did not complete" {})))
    value))

(defn- fixture [f]
  (let [registry (atom {:topic {:strategy (Object.) :generation (random-uuid)}})
        started (a/chan 8) published (atom []) releases (atom 0)
        retired (atom false) store (Object.)]
    (with-redefs [pubsub/subscription (fn [_ topic] (get @registry topic))
                  sync/store-sync-strategy (fn [store opts] {:store store :opts opts})
                  pubsub/subscribe! (fn [_ topics {:keys [strategies] :as opts}]
                                      (let [strategy (get strategies :topic)]
                                        (swap! registry assoc :topic {:strategy strategy :generation (random-uuid)})
                                        (a/put! started {:strategy strategy :opts opts})
                                        (a/go {:ok topics})))]
      (let [controller (sub/controller
                        {:peer :peer :topic :topic :store store :branch :db :walk-fn identity
                         :strategy (get-in @registry [:topic :strategy])
                         :retire-initial! #(reset! retired true)
                         :publish! #(swap! published conj %)
                         :release! (fn [_ _]
                                     (swap! releases inc)
                                     (swap! registry dissoc :topic)
                                     (a/go true))})]
        (try (f {:controller controller :started started :published published
                 :registry registry :releases releases :retired retired :store store})
             (finally (take! (sub/close! controller))))))))

(deftest controller-never-adopts-a-replacement-between-initial-drain-and-handoff
  (let [original (Object.) replacement (Object.)]
    (with-redefs [pubsub/subscription (fn [& _] {:strategy replacement})]
      (is (= :datahike.kabel/subscription-conflict
             (:type (ex-data
                     (try (sub/controller {:peer :peer :topic :topic :strategy original})
                          (catch clojure.lang.ExceptionInfo error error)))))))))

(deftest shutdown-joins-once-and-never-acts-on-a-later-replacement
  (fixture
   (fn [{:keys [controller registry releases]}]
     (let [closed (sub/close! controller)]
       (is (identical? closed (sub/close! controller)))
       (is (true? (take! closed)))
       (let [replacement (Object.)]
         (swap! registry assoc :topic {:strategy replacement :generation (random-uuid)})
         (is (identical? closed (sub/close! controller)))
         (is (true? (take! (sub/close! controller))))
         (is (= 1 @releases))
         (is (identical? replacement (get-in @registry [:topic :strategy]))))))))

(deftest failed-shutdown-drain-can-be-explicitly-retried
  (fixture
   (fn [{:keys [controller registry]}]
     (let [attempts (atom 0) error (ex-info "Temporary drain failure" {})
           controller (assoc controller :release! (fn [_ _]
                                                    (if (= 1 (swap! attempts inc))
                                                      (a/go error)
                                                      (do (swap! registry dissoc :topic) (a/go true)))))
           failed (sub/close! controller)]
       (is (= (ex-message error) (ex-message (take! failed))))
       (let [retried (sub/close! controller)]
         (is (not (identical? failed retried)))
         (is (true? (take! retried)))
         (is (identical? retried (sub/close! controller))))
       (is (= 2 @attempts))))))

(defn- head! [op head]
  ((get-in op [:strategy :opts :on-key-update]) :db head :assoc))

(defn- drain! [op]
  ((get-in op [:opts :on-handshake-complete]) :topic))

(deftest epoch-deduplication-head-before-drain-and-live-updates
  (fixture
   (fn [{:keys [controller started published releases retired store]}]
     (let [epoch (random-uuid) result (sub/subscribe! controller {:epoch epoch})
           op (take! started)]
       (is @retired)
       (is (= 1 @releases))
       (is (identical? store (get-in op [:strategy :store])))
       (is (identical? result (sub/subscribe! controller {:epoch epoch})))
       (head! op {:max-tx 5 :commit :first})
       (is (empty? @published))
       (is (nil? (a/poll! result)))
       (drain! op)
       (is (= {:status :ready :epoch epoch} (take! result)))
       (is (= [{:max-tx 5 :commit :first}] @published))
        ;; A branch replacement can have the same/lower transaction number.
       (head! op {:max-tx 3 :commit :replacement})
       (is (= :replacement (:commit (last @published))))
       (is (identical? result (sub/subscribe! controller {:epoch epoch})))))))

(deftest initial-retirement-and-publication-use-the-same-reentrant-monitor
  (fixture
   (fn [{:keys [controller started]}]
     (let [monitor (Object.)
           controller (assoc controller :lock monitor
                             :retire-initial! #(is (Thread/holdsLock monitor))
                             :publish! (fn [_] (is (Thread/holdsLock monitor))))
           result (locking monitor
                     ;; Model an initial publication's synchronous DB watch.
                    (sub/subscribe! controller {:epoch (random-uuid)}))
           op (take! started)]
       (head! op {:commit :shared-monitor}) (drain! op)
       (is (= :ready (:status (take! result))))
       (is (true? (take! (sub/close! controller))))))))

(deftest invalid-options-never-start-a-subscription
  (fixture
   (fn [{:keys [controller started]}]
     (doseq [opts [{:epoch (random-uuid) :handshake-timeout-ms nil}
                   {:epoch (random-uuid) :handshake-timeout-ms 0}
                   {:epoch (random-uuid) :unknown true}]]
       (is (= :datahike.kabel/invalid-sync-options
              (:type (ex-data (take! (sub/subscribe! controller opts)))))))
     (is (nil? (a/poll! started))))))

(deftest new-epoch-fences-old-updates-and-cancels-old-waiter
  (fixture
   (fn [{:keys [controller started published releases]}]
     (let [old-result (sub/subscribe! controller {:epoch (random-uuid)})
           old (take! started)
           epoch (random-uuid)
           result (sub/subscribe! controller {:epoch epoch})
           current (take! started)]
       (is (= :datahike.kabel/sync-cancelled (:type (ex-data (take! old-result)))))
       (head! old {:commit :stale}) (drain! old)
       (is (empty? @published))
       (head! current {:commit :current}) (drain! current)
       (is (= :ready (:status (take! result))))
       (is (= [{:commit :current}] @published))
       (is (= 2 @releases))))))

(deftest missing-head-holds-without-publishing-and-new-epoch-can-retry
  (fixture
   (fn [{:keys [controller started published]}]
     (let [result (sub/subscribe! controller {:epoch (random-uuid)}) op (take! started)]
       (drain! op)
       (is (= :kabel-no-head (:type (ex-data (take! result)))))
       (head! op {:commit :late})
       (is (empty? @published))
       (let [retry (sub/subscribe! controller {:epoch (random-uuid)}) op (take! started)]
         (head! op {:commit :fresh}) (drain! op)
         (is (= :ready (:status (take! retry)))))))))

(deftest shutdown-fences-pending-and-late-ready-callbacks
  (fixture
   (fn [{:keys [controller started published registry]}]
     (let [result (sub/subscribe! controller {:epoch (random-uuid)}) op (take! started)]
       (is (true? (take! (sub/close! controller))))
       (is (= :datahike.kabel/sync-cancelled (:type (ex-data (take! result)))))
       (head! op {:commit :after-release}) (drain! op)
       (is (empty? @published))
       (is (empty? @registry))
       (is (= :datahike.kabel/sync-cancelled
              (:type (ex-data (take! (sub/subscribe! controller {:epoch (random-uuid)}))))))))))

(deftest cannot-unsubscribe-a-replacement-owner
  (fixture
   (fn [{:keys [controller registry releases]}]
     (let [other {:strategy (Object.) :generation (random-uuid)}]
       (swap! registry assoc :topic other)
       (is (= :datahike.kabel/subscription-conflict
              (:type (ex-data (take! (sub/subscribe! controller {:epoch (random-uuid)}))))))
       (is (= other (:topic @registry)))
       (is (zero? @releases))))))

(deftest close-from-a-synchronous-db-watch-cannot-report-ready
  (fixture
   (fn [{:keys [controller started published]}]
     (let [controller (assoc controller :publish! (fn [head]
                                                    (swap! published conj head)
                                                    (sub/close! controller)))
           result (sub/subscribe! controller {:epoch (random-uuid)}) op (take! started)]
       (head! op {:commit :observed}) (drain! op)
       (is (= :datahike.kabel/sync-cancelled (:type (ex-data (take! result)))))
       (is (:closed? @(:state controller)))
       (is (not= :ready (get-in @(:state controller) [:operation :phase])))))))

(deftest newer-epoch-from-a-db-watch-stays-pending-until-its-own-drain
  (fixture
   (fn [{:keys [controller started published]}]
     (let [next-result (atom nil) next-epoch (random-uuid)
           reentrant (assoc controller :publish!
                            (fn [head]
                              (swap! published conj head)
                              (reset! next-result (sub/subscribe! controller {:epoch next-epoch}))))
           result (sub/subscribe! reentrant {:epoch (random-uuid)}) op (take! started)]
       (head! op {:commit :first}) (drain! op)
       (is (= :datahike.kabel/sync-cancelled (:type (ex-data (take! result)))))
       (let [next-op (take! started)]
         (is (= :pending (get-in @(:state controller) [:operation :phase])))
         (is (nil? (a/poll! @next-result)))
         (head! next-op {:commit :second}) (drain! next-op)
         (is (= {:status :ready :epoch next-epoch} (take! @next-result)))
         (is (= [:first :second] (mapv :commit @published))))))))

(deftest handshake-timeout-retains-last-db-and-explicit-new-epoch-retries
  (fixture
   (fn [{:keys [controller started published]}]
     (let [result (sub/subscribe! controller {:epoch (random-uuid) :handshake-timeout-ms 25})
           op (take! started)]
       (is (= :kabel-handshake-timeout (:type (ex-data (take! result)))))
       (head! op {:commit :too-late}) (drain! op)
       (is (empty? @published))
       (let [retry (sub/subscribe! controller {:epoch (random-uuid)}) op (take! started)]
         (head! op {:commit :fresh}) (drain! op)
         (is (= :ready (:status (take! retry)))))))))

(deftest timed-out-subscribe-constructor-retains-physical-gate-until-it-finishes
  (fixture
   (fn [{:keys [controller started]}]
     (let [subscribe! pubsub/subscribe! held (a/promise-chan) first? (atom true)]
       (with-redefs [pubsub/subscribe! (fn [& args]
                                         (let [result (apply subscribe! args)]
                                           (if (compare-and-set! first? true false) held result)))]
         (let [result (sub/subscribe! controller {:epoch (random-uuid) :handshake-timeout-ms 25})
               _ (take! started)]
           (is (= :kabel-handshake-timeout (:type (ex-data (take! result)))))
           (let [retry (sub/subscribe! controller {:epoch (random-uuid)})]
             (is (nil? (a/poll! started)))
             (a/put! held {:ok #{:topic}})
             (let [op (take! started)]
               (head! op {:commit :fresh}) (drain! op)
               (is (= :ready (:status (take! retry))))))))))))
