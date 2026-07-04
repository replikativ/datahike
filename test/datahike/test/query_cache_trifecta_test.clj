(ns datahike.test.query-cache-trifecta-test
  (:require
   [clojure.test.check.generators :as gen]
   [datahike.lru :as lru]
   [hive-test.trifecta :refer [deftrifecta]]))

(defn run-cache
  "Build a weighted-lru from a spec and return its observable state."
  [{:keys [limit weight-limit puts]}]
  (let [cache (reduce (fn [c [k v]] (assoc c k v))
                      (lru/weighted-lru limit weight-limit count)
                      puts)
        st    (.-state cache)]
    {:limit        limit
     :weight-limit weight-limit
     :put-keys     (vec (distinct (map first puts)))
     :kept-keys    (vec (sort (keys (:key-value st))))
     :entry-count  (count (:key-value st))
     :total-weight (:total-weight st)}))

(defn cache-invariants-hold?
  [{:keys [limit weight-limit put-keys kept-keys entry-count total-weight]}]
  (and (<= entry-count (max 1 limit))
       (or (zero? weight-limit)
           (<= entry-count 1)
           (<= total-weight weight-limit))
       (every? (set put-keys) kept-keys)
       (= entry-count (count kept-keys))))

(def gen-input
  (gen/let [limit        (gen/choose 1 6)
            weight-limit (gen/choose 0 20)
            puts         (gen/vector
                          (gen/tuple gen/keyword (gen/vector gen/small-integer 0 6))
                          0 40)]
    {:limit limit :weight-limit weight-limit :puts puts}))

(defn- distinct-keys [puts] (vec (distinct (map first puts))))

(defn mut-drop-weight-bound
  "Original bug: ignore the weight budget entirely (count-only LRU)."
  [input]
  (run-cache (assoc input :weight-limit 0)))

(defn mut-no-evict
  "Never evict — retain every key."
  [{:keys [limit weight-limit puts]}]
  (let [ks (distinct-keys puts)]
    {:limit limit :weight-limit weight-limit :put-keys ks
     :kept-keys (vec (sort ks)) :entry-count (count ks)
     :total-weight (reduce + 0 (map (comp count second) puts))}))

(defn mut-keep-oldest
  "Evict newest instead of oldest on overflow."
  [{:keys [limit weight-limit puts]}]
  (let [ks   (distinct-keys puts)
        kept (vec (take limit ks))]
    {:limit limit :weight-limit weight-limit :put-keys ks
     :kept-keys (vec (sort kept)) :entry-count (count kept) :total-weight 0}))

(deftrifecta weighted-lru-trifecta
  datahike.test.query-cache-trifecta-test/run-cache
  {:golden-path "test/golden/datahike/weighted-lru.edn"
   :cases       {:count-evicts   {:limit 2 :weight-limit 0
                                   :puts [[:a [0]] [:b [0]] [:c [0]]]}
                 :weight-evicts  {:limit 100 :weight-limit 5
                                  :puts [[:a [1 2 3]] [:b [4 5 6]]]}
                 :keep-newest    {:limit 100 :weight-limit 2
                                  :puts [[:big [1 2 3 4 5]]]}
                 :touch-protects {:limit 2 :weight-limit 0
                                  :puts [[:a [0]] [:b [0]] [:a [0]] [:c [0]]]}
                 :empty          {:limit 3 :weight-limit 5 :puts []}}
   :gen         gen-input
   :pred        cache-invariants-hold?
   :property-type :pred
   :num-tests   200
   :mutations   [["drop-weight-bound" mut-drop-weight-bound]
                 ["no-evict"          mut-no-evict]
                 ["keep-oldest"       mut-keep-oldest]]})
