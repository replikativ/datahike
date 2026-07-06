(ns datahike.test.lru-weighted-property-test
  "Property-based coverage for datahike.lru/weighted-lru using only
   org.clojure/test.check (already a :test dependency). Replaces the
   hive-test trifecta: no external test framework, no golden file.

   The core is a REFERENCE-MODEL oracle: an independently written,
   obviously-correct list-based model of the weighted-LRU semantics.
   Every generated put-sequence is run through both the real cache and
   the model, and their surviving keys / values / total weight must
   agree. This catches LRU-ordering bugs that pure invariant checks
   (entry-count cap, weight budget, subset) would miss."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer [is deftest]])
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datahike.lru :as lru]))

;; --- reference model (LRU->MRU order vector + value map) --------------------

(defn- model-put [{:keys [order vals]} [k v]]
  {:order (conj (vec (remove #{k} order)) k)   ; move/insert k as most-recent
   :vals  (assoc vals k v)})

(defn- model-shrink [{:keys [order vals]} limit weight-limit weigh]
  (loop [order order, vals vals]
    (let [n  (count order)
          tw (reduce (fn [a k] (+ a (weigh (vals k)))) 0 order)
          over? (and (> n 1)
                     (or (> n limit)
                         (and (pos? weight-limit) (> tw weight-limit))))]
      (if over?
        (recur (subvec order 1) (dissoc vals (first order)))
        {:order order :vals vals}))))

(defn- run-model [limit weight-limit weigh puts]
  (reduce (fn [m kv] (model-shrink (model-put m kv) limit weight-limit weigh))
          {:order [] :vals {}}
          puts))

;; --- real cache state extraction --------------------------------------------

(defn- state [c] #?(:clj (.-state c) :cljs (.-state c)))

(defn- run-real [limit weight-limit weigh puts]
  (reduce (fn [c kv] (assoc c (first kv) (second kv)))
          (lru/weighted-lru limit weight-limit weigh)
          puts))

;; --- generators -------------------------------------------------------------

(def ^:private gen-input
  (gen/let [limit        (gen/choose 1 6)
            weight-limit (gen/choose 0 20)
            puts         (gen/vector
                          (gen/tuple gen/keyword
                                     (gen/vector gen/small-integer 0 6))
                          0 60)]
    {:limit limit :weight-limit weight-limit :puts puts}))

(def weigh count)

;; Mirror the impl: when the weight budget is disabled (weight-limit 0),
;; weighted-lru does not track weights at all, so total-weight stays 0.
(defn- eff-weigh [weight-limit v] (if (pos? weight-limit) (weigh v) 0))

;; --- properties -------------------------------------------------------------

(def prop-matches-model
  (prop/for-all [{:keys [limit weight-limit puts]} gen-input]
                (let [real  (run-real  limit weight-limit weigh puts)
                      st    (state real)
                      model (run-model limit weight-limit weigh puts)
                      kept  (set (:order model))]
                  (and
       ;; same surviving key set as the independently-derived model
                   (= kept (set (keys (:key-value st))))
       ;; every surviving key returns its last-put value, via ILookup
                   (every? (fn [k] (= (get (:vals model) k) (get real k))) kept)
       ;; bookkeeping consistency: tracked total-weight = recomputed weight
       ;; (0 when the budget is disabled, matching the impl's untracked path)
                   (= (:total-weight st)
                      (reduce (fn [a k] (+ a (eff-weigh weight-limit (get (:vals model) k)))) 0 kept))))))

(def prop-invariants
  (prop/for-all [{:keys [limit weight-limit puts]} gen-input]
                (let [st       (state (run-real limit weight-limit weigh puts))
                      n        (count (:key-value st))
                      put-keys (set (map first puts))]
                  (and (<= n (max 1 limit))                                   ; entry cap
                       (or (zero? weight-limit) (<= n 1)
                           (<= (:total-weight st) weight-limit))               ; weight budget
                       (every? put-keys (keys (:key-value st)))))))            ; kept ⊆ put

(deftest weighted-lru-matches-reference-model
  (let [{:keys [pass? fail]} (tc/quick-check 500 prop-matches-model)]
    (is pass? (str "counterexample: " (pr-str fail)))))

(deftest weighted-lru-invariants-hold
  (let [{:keys [pass? fail]} (tc/quick-check 500 prop-invariants)]
    (is pass? (str "counterexample: " (pr-str fail)))))
