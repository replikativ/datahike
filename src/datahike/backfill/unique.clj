(ns ^:no-doc datahike.backfill.unique
  "Uniqueness checks for complete private current AVET roots. These checks do
   not constitute an activation certificate: the caller must bind the checked
   roots to an exact generation, schema and complete transaction cursor."
  (:require [datahike.constants :refer [e0 emax tx0 txmax]]
            [datahike.backfill.control :as control]
            [datahike.datom :as dd]
            [datahike.db.utils :as dbu]
            [datahike.index.interface :as di]))

(defn- duplicate! [ident value]
  (throw (ex-info "Cannot enable uniqueness: duplicate current value."
                  {:error :transact/schema :attribute ident :value value})))

(defn validate!
  "Scan requested attribute slices in native AVET order, retaining one previous
   datom. The complete current candidate is required; history is not checked."
  [database current idents]
  (doseq [ident idents]
    (let [a (dbu/attr-ref-or-ident database ident)]
      (loop [remaining (seq (di/-slice current (dd/datom e0 a nil tx0)
                                       (dd/datom emax a nil txmax) :avet))
             previous nil]
        (when-let [datom (first remaining)]
          (control/check!)
          (when (and previous (zero? (dd/compare-value (:v previous) (:v datom))))
            (duplicate! ident (:v datom)))
          (recur (next remaining) datom)))))
  nil)

(defn validate-effects!
  "After replaying ONE COMPLETE transaction on previously checked roots, check
   only its touched current values. No all-values set is retained; repeated
   values may be checked repeatedly. Work is bounded by the transaction effect
   count and native index seeks, not by total database size. Never call this
   between individual effects: temporary duplicates may be removed in the same
   transaction. unique-idents is a set of canonical attribute idents. The
   caller must bound the complete transaction's input size."
  [database current unique-idents effects]
  (doseq [[family _ datom old] effects
          :when (= family :current)
          changed [datom old]
          :when changed
          :let [ident (get (:ref-ident-map database) (:a changed) (:a changed))]
          :when (contains? unique-idents ident)]
    (control/check!)
    (let [a (:a changed)
          value (:v changed)
          matches (di/-slice current (dd/datom e0 a value tx0)
                             (dd/datom emax a value txmax) :avet)]
      (when (next (seq matches)) (duplicate! ident value))))
  nil)
