(ns datahike.test.model.invariant
  "Invariant checking framework for datahike model-based testing."
  (:require [clojure.set :as set]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.test.model.core :as model]))

(defprotocol PInvariant
  (check [this model-state actual-db])
  (invariant-name [_this]))

(defn check-all
  "Check all invariants against model and actual DB."
  [invariants model-state actual-db]
  (let [violations (keep #(check % model-state actual-db) invariants)]
    (if (empty? violations)
      {:valid? true}
      {:valid? false :violations (vec violations)})))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn- eav-triplet-set
  "Convert datoms to a set of [e a v] tuples, filtering to user-attrs."
  [user-attrs datoms]
  (into #{}
        (comp (filter (comp user-attrs :a))
              (map (juxt :e :a :v)))
        datoms))

;; =============================================================================
;; Index Sortedness Invariant
;; =============================================================================

(defrecord IndexSortedInvariant [index-type]
  PInvariant
  (check [_this _model-state actual-db]
    (let [actual-datoms (into [] (d/datoms actual-db index-type))
          cmp-fn (dd/index-type->cmp-quick index-type true)
          pairs (partition 2 1 actual-datoms)]
      (when-not (every?
                 (fn [[a b]] (not (pos? (cmp-fn a b))))
                 pairs)
        {:violation :index-unsorted
         :evidence {:index-type index-type
                    :sample (take 10 actual-datoms)}})))
  (invariant-name [_this]
    (str (name index-type) "-sorted")))

;; =============================================================================
;; Index Content Invariant
;; =============================================================================

(defrecord IndexContentInvariant [index-type user-attrs]
  PInvariant
  (check [_this model-state actual-db]
    (let [actual-datoms (eav-triplet-set user-attrs (d/datoms actual-db index-type))
          expected-datoms (case index-type
                            :eavt (set (model/compute-eavt model-state))
                            :aevt (set (model/compute-aevt model-state))
                            :avet (set (model/compute-avet model-state))
                            (throw (ex-info "Unknown index" {:index-type index-type})))
          missing (set/difference expected-datoms actual-datoms)
          extra (set/difference actual-datoms expected-datoms)]
      (when (or (seq missing) (seq extra))
        {:violation :index-content-mismatch
         :evidence {:index-type index-type
                    :missing (vec (take 10 missing))
                    :extra (vec (take 10 extra))}})))
  (invariant-name [_this]
    (str (name index-type) "-content")))

;; =============================================================================
;; Historical Consistency Invariant
;; =============================================================================

(defrecord HistoricalConsistencyInvariant [actual-tx-ids tx-offset user-attrs]
  PInvariant
  (check [_this model-state actual-db]
    (let [violations
          (for [actual-tx actual-tx-ids
                :let [model-tx (- actual-tx tx-offset)
                      expected (model/get-datoms-at-tx model-state model-tx)
                      actual (try
                               (eav-triplet-set user-attrs (d/datoms (d/as-of actual-db actual-tx) :eavt))
                               (catch Exception _ nil))
                      missing (when actual (set/difference expected actual))
                      extra (when actual (set/difference actual expected))]
                :when (and actual (or (seq missing) (seq extra)))]
            {:tx-id actual-tx
             :model-tx model-tx
             :missing (vec (take 5 missing))
             :extra (vec (take 5 extra))})]
      (when (seq violations)
        {:violation :historical-mismatch
         :evidence {:violations (vec violations)}})))
  (invariant-name [_this]
    "historical-consistency"))

;; =============================================================================
;; Constructor Functions
;; =============================================================================

(defn index-sorted
  "Create invariant that checks an index is sorted."
  [index-type]
  (->IndexSortedInvariant index-type))

(defn index-content
  "Create invariant that checks index content matches model."
  [index-type user-attrs]
  (->IndexContentInvariant index-type user-attrs))

(defn historical-consistency
  "Create invariant that checks historical states match as-of queries.
   
   actual-tx-ids: the actual transaction IDs in Datahike
   tx-offset: the offset to convert actual tx-ids to model tx-ids
   user-attrs: the set of user attributes to check"
  [actual-tx-ids tx-offset user-attrs]
  (->HistoricalConsistencyInvariant actual-tx-ids tx-offset user-attrs))

(defn all-index-invariants
  "Create all standard index invariants (sortedness + content for EAVT, AVET, AEVT)."
  [user-attrs]
  [(index-sorted :eavt)
   (index-sorted :avet)
   (index-sorted :aevt)
   (index-content :eavt user-attrs)
   (index-content :avet user-attrs)
   (index-content :aevt user-attrs)])