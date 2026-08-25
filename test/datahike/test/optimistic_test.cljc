(ns datahike.test.optimistic-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [clojure.core.async :as a :refer [<!]]
            [datahike.api :as d]
            [datahike.optimistic :as opt]
            [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]))

(defn- cfg []
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :write
   :keep-history? true})

(def schema
  [{:db/ident :entity/uuid :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :content :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(defn- setup []
  (a/go
    (let [c (cfg)
          _ #?(:clj (d/create-database c) :cljs (<! (d/create-database c)))
          conn #?(:clj (d/connect c) :cljs (<! (d/connect c {:sync? false})))
          id (random-uuid)
          _ (<! (d/transact! conn schema))
          _ (<! (d/transact! conn [{:entity/uuid id :content "a"}]))]
      {:cfg c :conn conn :id id})))

(defn- teardown [{:keys [cfg conn]} overlay]
  (opt/close! overlay)
  (d/release conn)
  #?(:clj (d/delete-database cfg) :cljs (d/delete-database cfg)))

(defn- value [db id]
  (:content (d/entity db [:entity/uuid id])))

(defn- eav-set [db]
  (into #{} (map (juxt :e :a :v)) (d/datoms db :eavt)))

(defn- apply-changes [view {:keys [added removed]}]
  (-> view
      (into (map (juxt :e :a :v)) added)
      (#(reduce disj % (map (juxt :e :a :v) removed)))))

(deftest-async writer-backed-happy-path
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        events (atom [])]
    (opt/listen! overlay ::events #(swap! events conj %))
    (let [{:keys [result]} (opt/transact! overlay
                                          [[:db/add [:entity/uuid id] :content "b"]])]
      (is (= "b" (value (opt/db overlay) id)))
      (is (= :committed (:status (<! result))))
      (is (= "b" (value @conn id)))
      (is (empty? (opt/pending overlay)))
      (is (seq @events))
      (is (every? (fn [[a b]] (= (:db-after a) (:db-before b)))
                  (partition 2 1 @events))))
    (teardown env overlay)))

(deftest-async validation-is-a-tagged-result
  (let [{:keys [conn] :as env} (<! (setup))
        overlay (opt/open conn)
        result (:result (opt/transact! overlay [{:content 42}]))
        reply (<! result)]
    (is (= :rejected (:status reply)))
    (is (some? (:error reply)))
    (is (empty? (opt/pending overlay)))
    (let [bad-predicate (<! (:result (opt/predict! overlay [{:content "valid"}] nil)))]
      (is (= :rejected (:status bad-predicate)))
      (is (= :optimistic/invalid-reconciliation-predicate
             (:type (ex-data (:error bad-predicate))))))
    (teardown env overlay)))

(deftest-async prediction-reconciles-on-foreign-base-advance
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        {:keys [result]} (opt/predict!
                          overlay
                          [[:db/add [:entity/uuid id] :content "b"]]
                          #(= "b" (value % id)))]
    (is (= "b" (value (opt/db overlay) id)))
    (<! (d/transact! conn [[:db/add [:entity/uuid id] :content "b"]]))
    (is (= :reconciled (:status (<! result))))
    (is (empty? (opt/pending overlay)))
    (is (= "b" (value (opt/db overlay) id)))
    (teardown env overlay)))

(deftest-async transaction-arg-map-survives-replay
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        statuses (atom [])
        {:keys [ov-id]} (opt/predict!
                         overlay
                         {:tx-data [[:db/add [:entity/uuid id] :content "b"]]
                          :tx-meta {:db/txInstant #inst "2026-01-01T00:00:00.000-00:00"}}
                         (constantly false))]
    (opt/listen-status! overlay ::arg-map #(swap! statuses conj %))
    (is (= "b" (value (opt/db overlay) id)))
    (<! (d/transact! conn [{:entity/uuid (random-uuid) :content "foreign"}]))
    (is (= "b" (value (opt/db overlay) id)))
    (is (false? (:conflicting? (first (opt/pending overlay)))))
    (is (not-any? #(= :conflicting (:status %)) @statuses))
    (opt/abandon! overlay ov-id :test)
    (teardown env overlay)))

(deftest-async reject-removes-a-prediction-immediately
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        {:keys [ov-id result]} (opt/predict! overlay
                                             [[:db/add [:entity/uuid id] :content "b"]]
                                             (constantly false))
        error (ex-info "denied" {:type :denied})]
    (is (= "b" (value (opt/db overlay) id)))
    (opt/reject! overlay ov-id error)
    (let [reply (<! result)]
      (is (= :rejected (:status reply)))
      (is (= error (:error reply))))
    (is (= "a" (value (opt/db overlay) id)))
    (teardown env overlay)))

(deftest-async ack-disables-the-pre-ack-timeout
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn {:prediction-timeout-ms 10
                                :reconciliation-timeout-ms 10})
        statuses (atom [])
        {:keys [ov-id result]} (opt/predict! overlay
                                             [[:db/add [:entity/uuid id] :content "b"]]
                                             (constantly false))]
    (opt/listen-status! overlay ::status #(swap! statuses conj %))
    (opt/ack! overlay ov-id {:accepted true})
    (opt/ack! overlay ov-id {:duplicate true})
    (is (= :accepted (:status (<! result))))
    (is (= 1 (count (filter #(= :acknowledged (:status %)) @statuses))))
    (opt/reject! overlay ov-id nil)
    (is (= 1 (count (opt/pending overlay))))
    (is (not-any? #(= :rejected (:status %)) @statuses))
    (is (opt/error? {:status :rejected :error nil}))
    (<! (a/timeout 1100))
    (is (= 1 (count (opt/pending overlay))))
    (is (= "b" (value (opt/db overlay) id)))
    (is (= 1 (count (filter #(= :reconciliation-stalled (:status %)) @statuses))))
    (teardown env overlay)))

(deftest-async unacknowledged-prediction-expires-with-tagged-result
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn {:prediction-timeout-ms 10})
        {:keys [result]} (opt/predict! overlay
                                       [[:db/add [:entity/uuid id] :content "b"]]
                                       (constantly false))
        reply (<! result)]
    (is (= :expired (:status reply)))
    (is (= :unknown (:outcome reply)))
    (is (= "a" (value (opt/db overlay) id)))
    (is (empty? (opt/pending overlay)))
    (teardown env overlay)))

(deftest-async base-mutation-never-emits-a-false-delta
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        events (atom [])]
    (opt/listen! overlay ::events #(swap! events conj %))
    (let [{:keys [ov-id]} (opt/predict! overlay
                                        [[:db/add [:entity/uuid id] :content "b"]]
                                        (constantly false))]
      (<! (d/transact! conn [[:db/add [:entity/uuid id] :content "c"]]))
      (let [base-event (some #(when (= :base-advanced (get-in % [:cause :type])) %) @events)]
        (is (some? base-event))
        (is (nil? (:changes base-event)) "base replacement is an honest invalidation")
        (is (= "b" (value (:db-after base-event) id))))
      (opt/abandon! overlay ov-id :test)
      (let [event (last @events)]
        (is (= "c" (value (:db-after event) id)))
        (is (= (eav-set (:db-after event))
               (apply-changes (eav-set (:db-before event)) (:changes event)))))
      (is (= "c" (value (opt/db overlay) id))))
    (teardown env overlay)))

(deftest-async revisions-and-reentrant-submission-are-serialized
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        events (atom [])
        submitted? (atom false)]
    (opt/listen! overlay ::events
                 (fn [event]
                   (swap! events conj event)
                   (when (compare-and-set! submitted? false true)
                     (opt/predict! overlay
                                   [[:db/add [:entity/uuid id] :content "c"]]
                                   (constantly false)))))
    (opt/predict! overlay [[:db/add [:entity/uuid id] :content "b"]]
                  (constantly false))
    (is (= [1 2] (mapv :revision @events)))
    (is (= (:db-after (first @events)) (:db-before (second @events))))
    (is (= "c" (value (opt/db overlay) id)))
    (teardown env overlay)))

(deftest-async listener-can-unlisten-itself
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        calls (atom 0)]
    (opt/listen! overlay ::once
                 (fn [_]
                   (swap! calls inc)
                   (opt/unlisten! overlay ::once)))
    (opt/predict! overlay [[:db/add [:entity/uuid id] :content "b"]]
                  (constantly false))
    (opt/predict! overlay [[:db/add [:entity/uuid id] :content "c"]]
                  (constantly false))
    (is (= 1 @calls))
    (teardown env overlay)))

(deftest-async reentrant-submission-obeys-queue-limit
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn {:max-queue 1})
        handles (atom nil)
        once? (atom false)]
    (opt/listen! overlay ::fill-queue
                 (fn [_]
                   (when (compare-and-set! once? false true)
                     ;; Lifecycle commands share the serialized queue but do
                     ;; not consume the user-submission capacity.
                     (opt/unlisten! overlay ::fill-queue)
                     (reset! handles
                             [(opt/predict! overlay
                                            [[:db/add [:entity/uuid id] :content "c"]]
                                            (constantly false))
                              (opt/predict! overlay
                                            [[:db/add [:entity/uuid id] :content "d"]]
                                            (constantly false))]))))
    (opt/predict! overlay [[:db/add [:entity/uuid id] :content "b"]]
                  (constantly false))
    (let [[accepted overloaded] @handles
          reply (<! (:result overloaded))]
      (is (= :rejected (:status reply)))
      (is (= :optimistic/overloaded (:type (ex-data (:error reply)))))
      (is (= (:ov-id accepted) (:ov-id (second (opt/pending overlay))))))
    (is (= 2 (count (opt/pending overlay))))
    (teardown env overlay)))

(deftest-async bulk-expiry-is-one-consistent-transition
  (let [{:keys [conn] :as env} (<! (setup))
        overlay (opt/open conn {:prediction-timeout-ms 10})
        events (atom [])
        one (opt/predict! overlay [{:entity/uuid (random-uuid) :content "one"}]
                          (constantly false))
        two (opt/predict! overlay [{:entity/uuid (random-uuid) :content "two"}]
                          (constantly false))]
    (opt/listen! overlay ::bulk #(swap! events conj %))
    (is (= :expired (:status (<! (:result one)))))
    (is (= :expired (:status (<! (:result two)))))
    (let [expiry-events (filter #(= :expired (get-in % [:cause :reason])) @events)
          event (first expiry-events)]
      (is (= 1 (count expiry-events)))
      (is (= 2 (count (get-in event [:cause :ov-ids]))))
      (is (= (eav-set (:db-after event))
             (apply-changes (eav-set (:db-before event)) (:changes event)))))
    (teardown env overlay)))

(deftest-async conflict-and-resolution-are-replayed-and-reported
  (let [{:keys [conn] :as env} (<! (setup))
        overlay (opt/open conn)
        statuses (atom [])
        left (random-uuid)
        right (random-uuid)]
    (<! (d/transact! conn
                     [{:db/ident :unique/code
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:entity/uuid left :content "left"}
                      {:entity/uuid right :content "right"}]))
    (opt/listen-status! overlay ::conflicts #(swap! statuses conj %))
    (opt/predict! overlay [[:db/add [:entity/uuid left] :unique/code "same"]]
                  (constantly false))
    (<! (d/transact! conn [[:db/add [:entity/uuid right] :unique/code "same"]]))
    (is (= right (:entity/uuid (d/entity (opt/db overlay) [:unique/code "same"]))))
    (is (some #(= :conflicting (:status %)) @statuses))
    (<! (d/transact! conn [[:db/retract [:entity/uuid right] :unique/code "same"]]))
    (is (= left (:entity/uuid (d/entity (opt/db overlay) [:unique/code "same"]))))
    (is (some #(= :applicable (:status %)) @statuses))
    (teardown env overlay)))

(deftest-async close-rolls-back-and-settles-pending
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        {:keys [result]} (opt/predict! overlay
                                       [[:db/add [:entity/uuid id] :content "b"]]
                                       (constantly false))]
    (opt/close! overlay)
    (is (= :abandoned (:status (<! result))))
    (is (= "a" (value (opt/db overlay) id)))
    (d/release conn)
    #?(:clj (d/delete-database (:cfg env)) :cljs (d/delete-database (:cfg env)))))

(deftest-async close-settles-a-reentrant-submit-queued-after-close
  (let [{:keys [conn id] :as env} (<! (setup))
        overlay (opt/open conn)
        first-handle (atom nil)
        late-handle (atom nil)
        once? (atom false)]
    (opt/listen! overlay ::close-from-listener
                 (fn [_]
                   (when (compare-and-set! once? false true)
                     (opt/close! overlay)
                     (reset! late-handle
                             (opt/predict! overlay
                                           [[:db/add [:entity/uuid id] :content "c"]]
                                           (constantly false))))))
    (reset! first-handle
            (opt/predict! overlay
                          [[:db/add [:entity/uuid id] :content "b"]]
                          (constantly false)))
    (is (= :abandoned (:status (<! (:result @first-handle)))))
    (let [reply (<! (:result @late-handle))]
      (is (= :rejected (:status reply)))
      (is (opt/error? reply))
      (is (= :optimistic/closed (:type (ex-data (:error reply))))))
    (is (= "a" (value (opt/db overlay) id)))
    (d/release conn)
    #?(:clj (d/delete-database (:cfg env)) :cljs (d/delete-database (:cfg env)))))

#?(:clj
   (deftest concurrent-submissions-have-one-ordered-timeline
     (let [{:keys [conn] :as env} (a/<!! (setup))
           overlay (opt/open conn)
           events (atom [])
           gate (java.util.concurrent.CountDownLatch. 1)
           workers (doall
                    (for [n (range 16)]
                      (future
                        (.await gate)
                        (opt/predict! overlay
                                      [{:entity/uuid (random-uuid)
                                        :content (str n)}]
                                      (constantly false)))))]
       (opt/listen! overlay ::concurrent #(swap! events conj %))
       (.countDown gate)
       (doseq [worker workers] @worker)
       (is (= (vec (range 1 17)) (mapv :revision @events)))
       (is (every? (fn [[a b]] (= (:db-after a) (:db-before b)))
                   (partition 2 1 @events)))
       (is (= 16 (count (opt/pending overlay))))
       (teardown env overlay))))
