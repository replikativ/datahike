(ns datahike.test.secondary-vt-test
  "Tests for the bitemporal-v1 secondary-index protocol additions:

   - `ISecondaryIndex.-transact` receives `:tx-meta` on every datom —
     populated by `update-secondary-indices` from the in-progress db
     state (the tx-entity's `:db/txInstant` / `:db.valid/from` /
     `:db.valid/to` datoms, added pre-user-datoms by `flush-tx-meta`).
   - Adapters can opt into vt-pushdown via the optional
     `IValidTimeAware` protocol; non-implementers stay correct via the
     standard post-hoc AVET filter on `:db.valid/from`."
  (:require #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer [is deftest testing]])
            [datahike.api :as d]
            [datahike.index.secondary :as sec])
  #?(:clj (:import [java.util Date])))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

;; ============================================================================
;; A test adapter that records every -transact call's tx-meta
;; ============================================================================

(deftype RecordingIndex [seen]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{:emp/salary})
  (-transact [this tx-report]
    (swap! seen conj (select-keys tx-report [:tx-meta :added?]))
    this))

(deftest tx-meta-flows-into-secondary-transact
  ;; Hook in a tiny adapter that records every tx-report it sees, then
  ;; transact two vt-bearing txes and confirm tx-meta reached the
  ;; adapter on each user-datom write.
  (sec/register-index-type! ::recording
                            (fn [_config _db] (->RecordingIndex (atom []))))
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :emp/salary
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :idx/employees
                       :db.secondary/type ::recording
                       :db.secondary/attrs [:emp/salary]
                       :db.secondary/config {}
                       :db.secondary/status :ready}])
    (d/transact conn [{:db/id "datomic.tx"
                       :db.valid/from #inst "2024-01-01"
                       :db.valid/to   #inst "2024-07-01"}
                      {:db/id -1 :emp/salary 100000}])
    (d/transact conn [{:db/id "datomic.tx"
                       :db.valid/from #inst "2024-07-01"}
                      {:db/id -1 :emp/salary 110000}])
    ;; Let the async build-secondary-index! (kicked off when the schema
    ;; tx promotes the index to :building) finish replaying — its
    ;; tx-meta reconstruction (see writing.cljc) emits extra events
    ;; with tx-meta from EAVT lookups.
    (Thread/sleep 300)
    (let [idx (get-in (d/db conn) [:secondary-indices :idx/employees])
          seen @(.-seen idx)
          ;; The transactor may invoke `-transact` more than once per
          ;; user-datom (retract-old-then-add pattern under upsert); the
          ;; async build-secondary-index! may also replay user-datoms
          ;; with reconstructed tx-meta. We filter to the distinct set
          ;; of :db.valid/from values seen with a tx-meta payload.
          vt-events (filter #(get-in % [:tx-meta :db.valid/from]) seen)
          vt-froms (set (map #(get-in % [:tx-meta :db.valid/from]) vt-events))]
      (testing "the adapter saw at least one :transact call per user write"
        (is (>= (count seen) 2)))
      (testing "vt-bearing :transact calls covered both txes' tx-meta"
        (is (contains? vt-froms #inst "2024-01-01"))
        (is (contains? vt-froms #inst "2024-07-01"))))))

;; ============================================================================
;; Default fallback: ISecondaryIndex without IValidTimeAware stays correct
;; ============================================================================

(deftype StringIndex [content-map]
  ;; A tiny content index keyed by string-attr value → entity-set.
  sec/ISecondaryIndex
  (-search [_ query _entity-filter]
    ;; query is a string; returns the set of entities that have that string.
    (get @content-map query #{}))
  (-estimate [_ q] (count (get @content-map q #{})))
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{:emp/name})
  (-transact [this {:keys [^datahike.datom.Datom datom added?]}]
    (when added?
      (swap! content-map update (.-v datom) (fnil conj #{}) (.-e datom)))
    this))

(deftest non-vt-aware-index-is-vt-aware?-false
  ;; A plain ISecondaryIndex impl that doesn't implement
  ;; IValidTimeAware should be recognised by `sec/vt-aware?`.
  (let [idx (->StringIndex (atom {}))]
    (is (not (sec/vt-aware? idx)))))

;; ============================================================================
;; A vt-aware adapter — proves the protocol implements
;; ============================================================================

(deftype VtAwareIndex [content-map]
  sec/ISecondaryIndex
  (-search [_ q _] (get-in @content-map [q :any] #{}))
  (-estimate [_ q] (count (get-in @content-map [q :any] #{})))
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{:emp/name})
  (-transact [this {:keys [^datahike.datom.Datom datom added? tx-meta]}]
    (when added?
      (let [vf (or (:db.valid/from tx-meta) (:db/txInstant tx-meta))
            vt (or (:db.valid/to tx-meta) #inst "9999-12-31")]
        (swap! content-map update-in [(.-v datom) :any]
               (fnil conj #{}) (.-e datom))
        (swap! content-map update-in [(.-v datom) :windows]
               (fnil conj []) {:e (.-e datom) :vf vf :vt vt})))
    this)
  sec/IValidTimeAware
  (-search-at-vt [_ query _entity-filter valid-at-window]
    (let [windows (get-in @content-map [query :windows] [])
          at (if (vector? valid-at-window) (first valid-at-window) valid-at-window)]
      (into #{}
            (keep (fn [{:keys [e vf vt]}]
                    (when (and (not (.before ^Date at ^Date vf))
                               (.before ^Date at ^Date vt))
                      e)))
            windows))))

(deftest vt-aware-protocol-search-at-vt
  (let [idx (->VtAwareIndex (atom {}))]
    (testing "vt-aware? returns true for an IValidTimeAware impl"
      (is (sec/vt-aware? idx)))
    (testing "-search-at-vt filters by vt window"
      ;; Manually simulate two -transact calls on the same name with
      ;; different vt windows.
      (let [d1 (datahike.datom/datom 1 :emp/name "Bob" 100 true)
            d2 (datahike.datom/datom 2 :emp/name "Bob" 200 true)]
        (sec/-transact idx {:datom d1 :added? true
                            :tx-meta {:db.valid/from #inst "2024-01-01"
                                      :db.valid/to   #inst "2024-07-01"}})
        (sec/-transact idx {:datom d2 :added? true
                            :tx-meta {:db.valid/from #inst "2024-07-01"
                                      :db.valid/to   #inst "9999-12-31"}})
        (is (= #{1 2} (sec/-search idx "Bob" nil)))
        (is (= #{1} (sec/-search-at-vt idx "Bob" nil #inst "2024-04-15")))
        (is (= #{2} (sec/-search-at-vt idx "Bob" nil #inst "2024-09-15")))))))
