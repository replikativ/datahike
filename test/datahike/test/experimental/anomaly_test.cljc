(ns datahike.test.experimental.anomaly-test
  (:require [clojure.test :refer [deftest testing is are]]
            [datahike.experimental.anomaly :as anomaly]
            [datahike.experimental.graph-util :as gu]))

(defn- seeded-points
  "n deterministic 2-D points with each coord in [lo, hi), driven by the seedable
   PRNG so the anomaly tests don't flake (clojure.core `rand` is unseeded)."
  [n seed lo hi]
  (loop [r (gu/rng seed) acc [] i 0]
    (if (= i n)
      acc
      (let [[r x] (gu/rng-next r)
            [r y] (gu/rng-next r)
            f (fn [u] (+ lo (* (- hi lo) u)))]
        (recur r (conj acc [(f x) (f y)]) (inc i))))))

;; =============================================================================
;; Unit Tests for Statistical Utilities
;; =============================================================================

(deftest column-ecdf-test
  (testing "ECDF for simple sorted input"
    (is (= [0.2 0.4 0.6 0.8 1.0]
           (anomaly/column-ecdf [1 2 3 4 5]))))

  (testing "ECDF handles ties correctly"
    ;; Ties should get the maximum rank
    (is (= [0.2 0.8 0.4 0.8 1.0]
           (anomaly/column-ecdf [1 3 2 3 5]))))

  (testing "ECDF handles all same values"
    (is (= [1.0 1.0 1.0]
           (anomaly/column-ecdf [5 5 5]))))

  (testing "ECDF with negative values"
    (is (= [0.2 0.4 0.6 0.8 1.0]
           (anomaly/column-ecdf [-2 -1 0 1 2])))))

(deftest skewness-test
  (testing "Symmetric distribution has zero skewness"
    (is (< (Math/abs (anomaly/skewness [1 2 3 4 5])) 0.001)))

  (testing "Right-skewed distribution has positive skewness"
    (is (pos? (anomaly/skewness [1 1 1 1 10]))))

  (testing "Left-skewed distribution has negative skewness"
    (is (neg? (anomaly/skewness [1 10 10 10 10]))))

  (testing "Constant values have zero skewness"
    (is (zero? (anomaly/skewness [5 5 5 5 5])))))

;; =============================================================================
;; Integration Tests for ECOD
;; =============================================================================

(deftest ecod-scores-test
  (testing "Outliers score higher than normal points"
    (let [normal (seeded-points 20 1 -0.5 0.5)
          outliers [[5.0 5.0] [-5.0 -5.0]]
          data (vec (concat normal outliers))
          scores (anomaly/ecod-scores data)
          normal-scores (take 20 scores)
          outlier-scores (drop 20 scores)]
      ;; Outliers should have higher average score
      (is (> (/ (reduce + outlier-scores) 2)
             (/ (reduce + normal-scores) 20)))))

  (testing "Matching PyOD on reference dataset"
    ;; This dataset was verified against PyOD
    (let [data [[0.5 0.5]
                [0.3 0.7]
                [-0.2 0.1]
                [0.1 -0.3]
                [-0.5 -0.5]
                [0.8 0.2]
                [-0.1 0.4]
                [0.4 -0.1]
                [5.0 5.0]
                [-4.0 -4.0]]
          scores (anomaly/ecod-scores data)
          ;; Expected scores from PyOD (verified)
          expected [2.4079 2.3026 1.8971 1.8971 3.2189
                    2.3026 1.8326 1.8326 4.6052 4.6052]]
      (doseq [[i [actual exp]] (map-indexed vector (map vector scores expected))]
        (is (< (Math/abs (- actual exp)) 0.001)
            (str "Score mismatch at index " i))))))

(deftest find-outliers-test
  (testing "top-k returns exactly k results"
    (let [data (seeded-points 50 2 0.0 1.0)
          result (anomaly/find-outliers data {:top-k 5})]
      (is (= 5 (count result)))))

  (testing "results are sorted by score descending"
    (let [data (seeded-points 50 3 0.0 1.0)
          result (anomaly/find-outliers data {:top-k 10})
          scores (map :score result)]
      (is (= scores (reverse (sort scores))))))

  (testing "threshold filters correctly"
    ;; Create clustered normal points around origin, outlier far away
    (let [normal (seeded-points 30 4 -0.5 0.5)  ;; [-0.5, 0.5] range
          outlier [[10.0 10.0]]  ;; Far outside
          data (vec (concat normal outlier))
          scores (anomaly/ecod-scores data)
          outlier-score (last scores)
          normal-max (apply max (butlast scores))
          ;; Outlier should score significantly higher
          _ (is (> outlier-score normal-max) "Outlier should score higher than all normal")
          ;; Set threshold between normal max and outlier
          threshold (/ (+ normal-max outlier-score) 2)
          result (anomaly/find-outliers data {:threshold threshold})]
      ;; Only the [10, 10] outlier should exceed threshold
      (is (= 1 (count result)))
      (is (= [10.0 10.0] (:point (first result))))))

  (testing "default contamination returns ~10%"
    (let [data (seeded-points 100 5 0.0 1.0)
          result (anomaly/find-outliers data)]
      (is (= 10 (count result))))))

(deftest ecod-explain-test
  (testing "explain returns per-dimension breakdown"
    (let [data [[0.0 0.0] [1.0 1.0] [5.0 5.0]]
          explanation (anomaly/ecod-explain data 2)]
      (is (= 2 (count explanation)))
      (is (every? #(contains? % :dimension) explanation))
      (is (every? #(contains? % :score) explanation))
      (is (every? #(contains? % :value) explanation))))

  (testing "sum of dimension scores equals total score"
    (let [data [[0.0 0.0] [1.0 1.0] [5.0 5.0]]
          total-score (nth (anomaly/ecod-scores data) 2)
          explanation (anomaly/ecod-explain data 2)
          sum-of-dims (reduce + (map :score explanation))]
      (is (< (Math/abs (- total-score sum-of-dims)) 0.001)))))

(deftest ecod-score-test
  (testing "scoring new extreme point"
    (let [normal-data (seeded-points 50 6 -0.5 0.5)
          extreme-point [10.0 10.0]
          score (anomaly/ecod-score normal-data extreme-point)
          normal-scores (anomaly/ecod-scores normal-data)
          max-normal (apply max normal-scores)]
      ;; Extreme point should score higher than any normal point
      (is (> score max-normal)))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest edge-cases-test
  (testing "single point"
    (let [scores (anomaly/ecod-scores [[1.0 2.0]])]
      (is (= 1 (count scores)))))

  (testing "two points"
    (let [scores (anomaly/ecod-scores [[0.0 0.0] [1.0 1.0]])]
      (is (= 2 (count scores)))))

  (testing "single dimension"
    (let [scores (anomaly/ecod-scores [[1.0] [2.0] [3.0] [10.0]])]
      (is (= 4 (count scores)))
      ;; 10.0 should be the outlier
      (is (= 3 (apply max-key #(nth scores %) (range 4))))))

  (testing "many dimensions"
    (let [data (vec (for [_ (range 20)]
                      (vec (repeatedly 10 rand))))
          scores (anomaly/ecod-scores data)]
      (is (= 20 (count scores))))))
