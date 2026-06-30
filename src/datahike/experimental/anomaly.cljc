(ns datahike.experimental.anomaly
  "Anomaly detection algorithms for Datahike.

   Implements ECOD (Empirical Cumulative Distribution-based Outlier Detection)
   from Li et al., IEEE TKDE 2022. https://arxiv.org/abs/2201.00382

   ECOD is parameter-free, interpretable, and efficient:
   - O(n log n) per dimension (sorting for ECDF)
   - No hyperparameters to tune
   - Scores are interpretable as tail probabilities"
  #?(:clj (:import [java.lang Math])))

;; =============================================================================
;; Statistical Utilities
;; =============================================================================

(defn- signum
  "Returns -1, 0, or 1 based on sign of x."
  [x]
  (cond
    (pos? x) 1.0
    (neg? x) -1.0
    :else 0.0))

(defn skewness
  "Compute Fisher-Pearson skewness coefficient.
   Positive = right-skewed (long tail to the right)
   Negative = left-skewed (long tail to the left)
   Zero = symmetric"
  [xs]
  (let [n (count xs)
        mean (/ (reduce + xs) n)
        diffs (map #(- % mean) xs)
        m2 (/ (reduce + (map #(* % %) diffs)) n)
        m3 (/ (reduce + (map #(* % % %) diffs)) n)
        std (Math/sqrt m2)]
    (if (zero? std)
      0.0
      (/ m3 (* std std std)))))

(defn column-ecdf
  "Compute empirical cumulative distribution function for a column.

   For each value x, ECDF(x) = (count of values <= x) / n
   Handles ties by assigning the maximum rank to all tied values.

   Returns a vector of CDF values in the same order as input."
  [xs]
  (let [n (count xs)
        ;; Create [index, value] pairs and sort by value
        indexed (map-indexed vector xs)
        sorted-by-val (sort-by second indexed)
        ;; Assign ranks, handling ties
        with-ranks (loop [remaining sorted-by-val
                          result []
                          rank 0]
                     (if (empty? remaining)
                       result
                       (let [current-val (second (first remaining))
                             ;; Find all items with same value (ties)
                             tie-group (take-while #(= (second %) current-val) remaining)
                             tie-count (count tie-group)
                             max-rank (+ rank tie-count)
                             ecdf-val (/ (double max-rank) n)]
                         (recur (drop tie-count remaining)
                                (into result (map #(vector (first %) ecdf-val) tie-group))
                                max-rank))))
        ;; Restore original order
        sorted-by-idx (sort-by first with-ranks)]
    (mapv second sorted-by-idx)))

;; =============================================================================
;; ECOD Algorithm
;; =============================================================================

(defn ecod-scores
  "Compute ECOD anomaly scores for a dataset.

   ECOD detects outliers by identifying points in the tails of distributions.
   For each dimension, it computes the empirical CDF and scores points based
   on how extreme they are (low probability = high score).

   Parameters:
   - data: vector of vectors, each inner vector is a sample's features
           All features must be numeric.

   Returns:
   - vector of anomaly scores (higher = more anomalous)

   Algorithm:
   1. For each dimension, compute left-tail and right-tail ECDFs
   2. Transform to scores via -log(ecdf)
   3. Use skewness to determine which tail is anomalous
   4. Take max across perspectives, sum across dimensions

   Reference: Li et al., 'ECOD: Unsupervised Outlier Detection Using
   Empirical Cumulative Distribution Functions', IEEE TKDE 2022"
  [data]
  (let [n-samples (count data)
        n-features (count (first data))

        ;; Transpose: extract columns (features) from rows (samples)
        columns (for [j (range n-features)]
                  (mapv #(double (nth % j)) data))

        ;; Pre-compute per-column statistics
        column-stats
        (for [col columns]
          (let [;; Left tail ECDF: low values get low CDF, high -log score
                ecdf-left (column-ecdf col)
                ;; Right tail ECDF: negate column so high values get low CDF
                ecdf-right (column-ecdf (mapv - col))
                ;; Sign of skewness determines which tail has outliers
                skew-sign (signum (skewness col))]
            {:ecdf-left ecdf-left
             :ecdf-right ecdf-right
             :skew-sign skew-sign}))

        ;; Score each sample
        sample-scores
        (for [i (range n-samples)]
          (reduce +
                  (for [j (range n-features)]
                    (let [{:keys [ecdf-left ecdf-right skew-sign]} (nth column-stats j)
                          el (nth ecdf-left i)
                          er (nth ecdf-right i)
                          ;; -log transform: low probability -> high score
                          ;; Clamp to avoid -log(0)
                          u-left (- (Math/log (max el 1e-10)))
                          u-right (- (Math/log (max er 1e-10)))
                          ;; Skewness-aware score (PyOD formula):
                          ;; - If right-skewed (sign=1): use right tail
                          ;; - If left-skewed (sign=-1): use left tail
                          ;; - If symmetric (sign=0): use both tails
                          u-skew (+ (* u-left -1.0 (signum (- skew-sign 1.0)))
                                    (* u-right (signum (+ skew-sign 1.0))))]
                      ;; Take maximum of all three perspectives
                      (max u-left u-right u-skew)))))]
    (vec sample-scores)))

(defn ecod-score
  "Score a single new point against a fitted ECOD model.

   Parameters:
   - training-data: the original training dataset (vector of vectors)
   - point: a single point to score (vector of features)

   Returns: anomaly score for the point

   Note: This recomputes ECDFs including the new point, which is the
   correct behavior for anomaly detection (the point should be compared
   against the full distribution)."
  [training-data point]
  (let [augmented (conj (vec training-data) point)
        scores (ecod-scores augmented)]
    (last scores)))

(defn ecod-explain
  "Explain which dimensions contribute most to a point's anomaly score.

   Parameters:
   - data: the dataset (vector of vectors)
   - point-idx: index of the point to explain

   Returns: vector of per-dimension scores, same length as feature count"
  [data point-idx]
  (let [n-features (count (first data))
        columns (for [j (range n-features)]
                  (mapv #(double (nth % j)) data))

        dim-scores
        (for [[j col] (map-indexed vector columns)]
          (let [ecdf-left (column-ecdf col)
                ecdf-right (column-ecdf (mapv - col))
                skew-sign (signum (skewness col))
                el (nth ecdf-left point-idx)
                er (nth ecdf-right point-idx)
                u-left (- (Math/log (max el 1e-10)))
                u-right (- (Math/log (max er 1e-10)))
                u-skew (+ (* u-left -1.0 (signum (- skew-sign 1.0)))
                          (* u-right (signum (+ skew-sign 1.0))))]
            {:dimension j
             :value (nth (nth data point-idx) j)
             :ecdf-left el
             :ecdf-right er
             :skewness skew-sign
             :score (max u-left u-right u-skew)}))]
    (vec dim-scores)))

(defn find-outliers
  "Find outliers in a dataset using ECOD.

   Parameters:
   - data: vector of vectors (samples x features)
   - opts: optional map with:
     - :threshold - score threshold (default: auto from contamination)
     - :contamination - expected fraction of outliers (default: 0.1)
     - :top-k - return top k outliers (alternative to threshold)

   Returns: vector of {:index, :score, :point} maps for detected outliers"
  ([data] (find-outliers data {}))
  ([data opts]
   (let [scores (ecod-scores data)
         indexed (map-indexed #(hash-map :index %1 :score %2 :point (nth data %1)) scores)
         sorted (reverse (sort-by :score indexed))]
     (cond
       (:top-k opts)
       (vec (take (:top-k opts) sorted))

       (:threshold opts)
       (vec (filter #(> (:score %) (:threshold opts)) sorted))

       :else
       (let [contamination (get opts :contamination 0.1)
             n-outliers (max 1 (int (* contamination (count data))))]
         (vec (take n-outliers sorted)))))))

(comment
  ;; Usage examples

  ;; Generate test data
  (def normal-data
    (repeatedly 50 #(vector (+ (* 2 (rand)) -1) (+ (* 2 (rand)) -1))))

  (def outliers
    [[5.0 5.0] [-4.0 4.0] [0.0 6.0]])

  (def test-data (vec (concat normal-data outliers)))

  ;; Compute scores
  (def scores (ecod-scores test-data))

  ;; Find top outliers
  (find-outliers test-data {:top-k 5})

  ;; Explain a specific point
  (ecod-explain test-data 52)

  ;; Score a new point against existing data
  (ecod-score test-data [10.0 10.0]))
