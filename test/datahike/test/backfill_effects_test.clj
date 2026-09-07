(ns datahike.test.backfill-effects-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.backfill.effects :as effects]
            [datahike.core :as core]
            [datahike.db :as db]
            [datahike.index.interface :as di]
            [datahike.index.secondary :as sec]))

(defn- ident [database datom]
  (get (:ref-ident-map database) (:a datom) (:a datom)))

(defn- rows [database field attrs]
  (filter #(contains? attrs (ident database %)) (get database field)))

(defn- tree [database datoms temporal?]
  (reduce (fn [index datom]
            (if temporal?
              (di/-temporal-insert index datom :avet 0)
              (di/-insert index datom :avet 0)))
          (empty (:avet database)) datoms))

(defn- from-primary [database attrs]
  (cond-> {:current (tree database (rows database :aevt attrs) false)}
    (get-in database [:config :keep-history?])
    (assoc :temporal (tree database (rows database :temporal-aevt attrs) true))))

(defn- assert-primary-parity [database attrs candidate]
  (doseq [[family expected] (from-primary database attrs)]
    (is (= (mapv #(vec (seq %)) expected) (mapv #(vec (seq %)) (get candidate family)))
        (str "independent primary projection: " family))))

(defn- begin [database attrs]
  {:db (assoc database :avet-build {:id (random-uuid) :attrs attrs})
   :attrs attrs :trees (from-primary database attrs)})

(defn- step
  ([state tx] (step state tx #(d/with %1 %2)))
  ([{:keys [db attrs trees]} tx apply-tx]
   (let [clean (dissoc db :avet-build-effects)
         report (apply-tx clean tx)
         after (:db-after report)
         emitted (:avet-build-effects after)
         candidate (reduce effects/replay trees emitted)]
     (is (nil? (:avet-build-effects clean)) "input remains pure")
     (is (every? #(contains? attrs (ident after (nth % 2))) emitted)
         "only covered canonical attributes emit")
     (assert-primary-parity after attrs candidate)
     {:db after :attrs attrs :trees candidate :effects emitted :report report})))

(defn- base [refs? history? indexed?]
  (d/db-with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? refs? :keep-history? history?
                               :schema-flexibility :write})
             [{:db/ident :one :db/valueType :db.type/long
               :db/cardinality :db.cardinality/one :db/index indexed?}
              {:db/ident :many :db/valueType :db.type/long
               :db/cardinality :db.cardinality/many :db/index indexed?}
              {:db/ident :quiet :db/valueType :db.type/long :db/noHistory true
               :db/cardinality :db.cardinality/one :db/index indexed?}
              {:db/ident :existing :db/valueType :db.type/long :db/index true
               :db/cardinality :db.cardinality/one}
              {:db/ident :uncovered :db/valueType :db.type/long
               :db/cardinality :db.cardinality/one}]))

(deftest private-observer-is-pure-and-independent-from-build-capture
  (let [database (base false true false)
        observed (effects/observe database #{:one})
        row (datahike.datom/datom 10000 :one 1 100)
        emitted (effects/emit observed :one :current :insert row nil)]
    (is (effects/enabled? observed))
    (is (empty? (effects/observations observed)))
    (is (= [[:current :insert row nil]] (effects/observations emitted)))
    (is (nil? (:avet-build-effects emitted)))
    (is (nil? (:avet-build-invalidated? (effects/schema-change emitted))))
    (is (= database (effects/unobserve emitted)))
    (let [both (assoc observed :avet-build {:id (random-uuid) :attrs #{:one}})
          after (effects/emit both :one :current :insert row nil)]
      (is (= (:avet-build-effects after) (effects/observations after))))))

(deftest replay-matches-primary-and-native-avet-through-write-sequences
  (doseq [refs? [false true] history? [false true] indexed? [false true]]
    (testing (str {:refs refs? :history history? :indexed indexed?})
      (let [attrs #{:one :many :quiet :existing}
            snapshot (d/db-with (base refs? history? indexed?)
                                [{:db/id 10000 :one 1 :many [1 2] :quiet 1 :existing 1}])
            initial (begin snapshot attrs)
            txs [[[:db/add 10000 :one 2] [:db/add 10000 :quiet 2]
                  [:db/add 10000 :existing 2] [:db/add 10000 :many 3]]
                 [[:db/add 10000 :one 2] [:db/add 10000 :many 3]
                  [:db/retract 10000 :many 999]]
                 [[:db/retract 10000 :many 1] [:db/retract 10000 :one 2]]
                 [[:db/add 10000 :one 3] [:db/retract 10000 :one 3]
                  [:db/add 10000 :one 4]]
                 [[:db/add 10001 :one 8] [:db/add 10001 :many 9]]
                 [[:db.fn/retractEntity 10001]]]
            final (reduce (fn [state tx]
                            (let [next-state (step state tx)]
                              (when indexed?
                                (doseq [[family field] [[:current :avet] [:temporal :temporal-avet]]
                                        :when (get-in next-state [:trees family])]
                                  (is (= (mapv #(vec (seq %)) (get-in next-state [:trees family]))
                                         (mapv #(vec (seq %)) (rows (:db next-state) field attrs)))
                                      "replay matches independently maintained native AVET")))
                              next-state)) initial txs)]
        (is (some #(= :upsert (second %)) (:effects (step initial (first txs)))))
        (when history?
          (is (empty? (filter #(= :quiet (ident (:db final) %))
                              (get-in final [:trees :temporal])))
              "noHistory never emits historical effects"))))))

(deftest noops-and-uncovered-writes-do-not-produce-effects
  (let [snapshot (d/db-with (base false true false) [{:db/id 10000 :one 1 :many [2]}])
        initial (begin snapshot #{:one :many})]
    (doseq [tx [[[:db/add 10000 :one 1]] [[:db/add 10000 :many 2]]
                [[:db/retract 10000 :many 999]] [[:db/add 10000 :uncovered 5]]]]
      (is (empty? (:effects (step initial tx)))))
    (is (identical? snapshot (effects/emit snapshot :one :current :insert nil nil)))
    (is (nil? (:avet-build-effects (d/db-with snapshot [[:db/add 10000 :one 2]]))))))

(deftest purge-effects-cover-history-only-and-current-removal
  (doseq [refs? [false true]]
    (let [initial (begin (d/db-with (base refs? true false)
                                    [{:db/id 10000 :one 1 :many [1 2]}]) #{:one :many})
          changed (step initial [[:db/add 10000 :one 2]])
          history-only (step changed [[:db/purge 10000 :one 1]])
          attribute (step history-only [[:db.purge/attribute 10000 :many]])
          entity (step attribute [[:db.purge/entity 10000]])]
      (is (seq (:effects history-only)))
      (is (every? #(= [:temporal :remove] (subvec % 0 2)) (:effects history-only)))
      (is (some #(= [:current :remove] (subvec % 0 2)) (:effects attribute)))
      (is (empty? (get-in entity [:trees :current])))
      (is (empty? (get-in entity [:trees :temporal]))))))

(deftest derived-tuples-are-captured-with-canonical-idents
  (doseq [refs? [false true]]
    (let [database (d/db-with (base refs? true false)
                              [{:db/ident :pair :db/valueType :db.type/tuple
                                :db/tupleAttrs [:one :quiet]
                                :db/cardinality :db.cardinality/one}])
          initial (begin database #{:pair})
          inserted (step initial [{:db/id 10000 :one 1 :quiet 2}])
          changed (step inserted [[:db/add 10000 :quiet 3]])
          removed (step changed [[:db/retract 10000 :one 1]])]
      (is (seq (:effects inserted)))
      (is (= [[nil 3]] (mapv :v (get-in removed [:trees :current])))))))

(deftest direct-loads-share-primary-effect-hooks
  (doseq [refs? [false true] history? [false true]]
    (let [initial (begin (base refs? history? false) #{:one :many})
          loader #(core/load-entities-with %1 %2 nil nil)
          loaded (step initial [[9000 :one 1 100 true] [9000 :many 2 100 true]
                                [9000 :one 3 101 true] [9000 :many 2 101 false]] loader)]
      (is (seq (:effects loaded)))
      (is (= [3] (mapv :v (get-in loaded [:trees :current])))))))

(deftest tx-instant-upserts-have-no-temporal-effect
  (doseq [refs? [false true]]
    (let [initial (begin (base refs? true false) #{:db/txInstant})
          changed (step initial [{:db/id 10000 :one 1}])]
      (is (seq (:effects changed)))
      (is (every? #(= :current (first %)) (:effects changed))))))

(deftest conflicting-schema-needs-generation-invalidation
  ;; These internal hooks deliberately do not authorize publication after a
  ;; descriptor's schema changes. Capture cannot substitute for that guard.
  (let [snapshot (d/db-with (base false true true) [{:db/id 10000 :one 1}])
        active (assoc snapshot :avet-build {:id (random-uuid) :attrs #{:one}})
        after (d/db-with active [[:db/add [:db/ident :one] :db/index false]])]
    (is (not= (get-in active [:schema :one]) (get-in after [:schema :one])))
    (is (empty? (:avet-build-effects after)))
    (is (empty? (rows after :avet #{:one})))
    (is (seq (rows after :aevt #{:one})))
    (is (:avet-build-invalidated? after)
        "schema mutation invalidates the captured generation")))

(defrecord PrimaryHashRecorder [events]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{:body})
  (-transact [this event] (update this :events conj event))
  sec/IDurableSecondaryIndex
  (-sec-generation-key-map [_] {:type :test/effect-recorder :format-version 1
                                :storage-owner :external :root :complete})
  (-sec-prepare [_ _] (throw (AssertionError. "Pure effect test must not persist")))
  (-sec-restore [this _ _] this))

(deftest secondary-only-effects-contain-primary-hashes-not-full-values
  (doseq [refs? [false true]]
    (let [database (-> (d/db-with (base refs? true false)
                                  [{:db/ident :body :db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/one :db.secondary/only true}])
                       (assoc-in [:schema :idx/recorder] {:db.secondary/status :ready
                                                          :db.secondary/type :test/effect-recorder
                                                          :db.secondary/attrs [:body]})
                       (assoc-in [:rschema :db.secondary/index :body] #{:idx/recorder})
                       (assoc-in [:secondary-indices :idx/recorder] (->PrimaryHashRecorder [])))
          initial (begin database #{:body})
          apply-tx (fn [database tx]
                     ;; The fixture is immutable and records notifications only.
                     ;; This context permits its durable capability; no writer or
                     ;; prepare callback is invoked by these pure transactions.
                     (binding [sec/*durable-secondary-write-context* :commit]
                       (d/with database tx)))
          inserted (step initial [{:db/id 10000 :body "first full value"}] apply-tx)
          changed (step inserted [[:db/add 10000 :body "second full value"]] apply-tx)
          removed (step changed [[:db/retract 10000 :body "second full value"]] apply-tx)]
      (is (= [(sec/secondary-only-hash "first full value")]
             (mapv :v (get-in inserted [:trees :current]))))
      (is (= [(sec/secondary-only-hash "second full value")]
             (mapv :v (get-in changed [:trees :current]))))
      (is (empty? (get-in removed [:trees :current])))
      (is (every? #(not (contains? #{"first full value" "second full value"} (:v (nth % 2))))
                  (concat (:effects inserted) (:effects changed) (:effects removed))))
      (is (= "first full value"
             (get-in inserted [:db :secondary-indices :idx/recorder :events 0 :datom :v]))))))

(deftest full-family-candidate-combines-existing-avet-with-new-attribute
  (doseq [refs? [false true] history? [false true]]
    (let [snapshot (d/db-with (base refs? history? false)
                              [{:db/id 10000 :one 1 :existing 1}])
          attrs (into #{:one} (map #(ident snapshot %)) (:avet snapshot))
          candidate (cond-> {:current (tree snapshot
                                            (concat (:avet snapshot) (rows snapshot :aevt #{:one})) false)}
                      history? (assoc :temporal
                                      (tree snapshot
                                            (concat (:temporal-avet snapshot)
                                                    (rows snapshot :temporal-aevt #{:one})) true)))
          initial (assoc (begin snapshot attrs) :trees candidate)
          changed (step initial [[:db/add 10000 :existing 2] [:db/add 10000 :one 3]])
          removed (step changed [[:db/retract 10000 :existing 2] [:db/retract 10000 :one 3]])]
      (doseq [state [changed removed]
              [family native-field primary-field] [[:current :avet :aevt]
                                                   [:temporal :temporal-avet :temporal-aevt]]
              :when (or (= :current family) history?)]
        (let [database (:db state)
              expected (tree database (concat (get database native-field)
                                              (rows database primary-field #{:one}))
                             (= :temporal family))]
          (is (= (mapv #(vec (seq %)) expected)
                 (mapv #(vec (seq %)) (get-in state [:trees family])))
              "existing indexed attributes remain current in the replacement root"))))))

(deftest intermediate-schema-changes-invalidate-even-when-final-schema-matches
  (let [snapshot (d/db-with (base true true false)
                            [[:db/add [:db/ident :one] :db/doc "original"]])
        active (assoc snapshot :avet-build {:id (random-uuid) :attrs #{:one}})
        after (d/db-with active [[:db/add [:db/ident :one] :db/doc "temporary"]
                                 [:db/add [:db/ident :one] :db/doc "original"]])]
    (is (= (:schema snapshot) (:schema after)))
    (is (:avet-build-invalidated? after)))
  ;; Existing rename handling retains the temporary ident's schema entry after
  ;; a rename-and-rename-back. Do not assume final schema equality here; the
  ;; generation must be invalidated regardless of that separate schema defect.
  (let [snapshot (d/db-with (base true true false) [{:db/id 10000 :one 1}])
        active (assoc snapshot :avet-build {:id (random-uuid) :attrs #{:one}})
        after (d/db-with active [[:db/add [:db/ident :one] :db/ident :temporary-one]
                                 [:db/add [:db/ident :temporary-one] :db/ident :one]])]
    (is (:avet-build-invalidated? after))))

(deftest rejected-pure-transaction-does-not-leak-partial-effects
  (let [database (d/db-with (base false true true)
                            [[:db/add [:db/ident :one] :db/unique :db.unique/value]])
        snapshot (d/db-with database [{:db/id 10000 :one 1} {:db/id 10001 :one 2}])
        initial (begin snapshot #{:one})]
    (is (thrown? clojure.lang.ExceptionInfo
                 (d/with (:db initial) [[:db/add 10000 :one 3]
                                        [:db/add 10001 :one 3]])))
    (is (nil? (:avet-build-effects (:db initial))))
    (is (= [1 2] (mapv :v (get-in initial [:trees :current]))))
    (is (= [2 3] (mapv :v (get-in (step initial [[:db/add 10000 :one 3]])
                                  [:trees :current]))))))

(deftest history-cutoff-and-component-purge-share-effect-hooks
  (doseq [refs? [false true]]
    (let [initial (begin (d/db-with (base refs? true false)
                                    [{:db/id 10000 :one 1 :many [1 2]}]) #{:one :many})
          changed (step initial [[:db/add 10000 :one 2] [:db/retract 10000 :many 1]])
          purged (step changed [[:db.history.purge/before (java.util.Date. Long/MAX_VALUE)]])]
      (is (seq (:effects purged)))
      (is (every? #(= [:temporal :remove] (subvec % 0 2)) (:effects purged))))
    (let [database (d/db-with (base refs? true false)
                              [{:db/ident :child :db/valueType :db.type/ref
                                :db/cardinality :db.cardinality/one :db/isComponent true}])
          initial (begin (d/db-with database [{:db/id 10000 :one 1 :child 10001}
                                              {:db/id 10001 :one 2}]) #{:one :child})
          purged (step initial [[:db.purge/entity 10000]])]
      (is (empty? (get-in purged [:trees :current])))
      (is (empty? (get-in purged [:trees :temporal]))))))
