(ns datahike.test.proximum-cost-policy-test
  (:require
   [clojure.test :refer [deftest is]]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary.proximum]))

(deftest runtime-filter-cost-policy
  (let [search-results (ns-resolve 'datahike.index.secondary.proximum
                                   'search-results)
        prox-search (requiring-resolve 'proximum.core/search-filtered)
        prox-count (requiring-resolve 'proximum.core/count-vectors)
        strategies* (atom [])
        run! (fn [allowed query-spec]
               (search-results ::index query-spec
                               (es/entity-bitset-from-longs (range allowed))))]
    (with-redefs-fn
      {prox-count (constantly 100000)
       prox-search (fn [_idx _vector _k _filter opts]
                     (swap! strategies* conj (:filter-strategy opts))
                     [])}
      #(do
         ;; 1,000^2 <= 128 * 100,000: avoid ANN plus an exact fallback.
         (run! 1000 {:vector (float-array [1.0]) :k 128 :ef 40})
         ;; 10,000^2 > 128 * 100,000: retain filtered HNSW.
         (run! 10000 {:vector (float-array [1.0]) :k 128 :ef 40})
         ;; An explicit caller policy always wins over the adaptive default.
         (run! 1000 {:vector (float-array [1.0]) :k 128 :ef 40
                     :filter-strategy :hnsw})))
    (is (= [:exact nil :hnsw] @strategies*))))
