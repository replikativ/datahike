(ns datahike.test.index-ordered-aggregate-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.index-ordered-aggregate :as ioa]))

(def schema
  {:l/k1 {}
   :l/k2 {}
   :l/key {:db/valueType :db.type/tuple
           :db/tupleAttrs [:l/k1 :l/k2]
           :db/unique :db.unique/identity}
   :l/nonunique {:db/index true}
   :l/payload {}
   :r/k1 {}
   :r/k2 {}
   :r/key {:db/valueType :db.type/tuple
           :db/tupleAttrs [:r/k1 :r/k2]
           :db/index true}
   :r/filter {:db/index true}})

(def write-schema
  (update-vals schema #(merge {:db/valueType :db.type/long
                               :db/cardinality :db.cardinality/one}
                              %)))

(defn- key-parts [k]
  [(quot k 100) (mod k 100)])

(defn- fixture-db [config]
  (let [left (mapv (fn [i]
                     (let [[k1 k2] (key-parts i)]
                       {:db/id (- (inc i))
                        :l/k1 k1 :l/k2 k2 :l/nonunique i :l/payload i}))
                   (range 1000))
        ;; Ten rows per key, with only keys 500..999 overlapping the producer.
        right (mapv (fn [i]
                      (let [k (+ 500 (quot i 10))
                            [k1 k2] (key-parts k)]
                        {:db/id (- (+ 1001 i))
                         :r/k1 k1 :r/k2 k2 :r/filter (mod i 20)}))
                    (range 10000))]
    (d/db-with (if (:attribute-refs? config)
                 (d/db-with (db/empty-db nil config)
                            (mapv (fn [[ident spec]]
                                    (assoc spec :db/ident ident))
                                  write-schema))
                 (db/empty-db schema config))
               (into left right))))

(def test-db (delay (fixture-db nil)))
(def attribute-ref-db
  (delay (fixture-db {:attribute-refs? true :schema-flexibility :write})))

(def filtered-count-query
  '[:find ?payload (count ?r)
    :in $ ?floor ?max-filter
    :where
    [?l :l/key ?key]
    [?l :l/payload ?payload]
    [(>= ?payload ?floor)]
    [?r :r/key ?key]
    [?r :r/filter ?filter]
    [(<= ?filter ?max-filter)]])

(def unsupported-count-query
  '[:find ?payload (count ?filter)
    :where
    [?l :l/key ?key]
    [?l :l/payload ?payload]
    [?r :r/key ?key]
    [?r :r/filter ?filter]])

(def nonunique-producer-query
  '[:find ?payload (count ?r)
    :where
    [?l :l/nonunique ?key]
    [?l :l/payload ?payload]
    [?r :r/key ?key]])

(defn- run-query
  ([query inputs enabled?]
   (run-query query inputs enabled? @test-db))
  ([query inputs enabled? database]
   (let [selected? (atom false)
         original ioa/execute
         result (binding [q/*disable-planner* false
                          q/*query-result-cache?* false
                          ioa/*enabled* enabled?]
                  (with-redefs [ioa/execute
                                (fn [& args]
                                  (let [result (apply original args)]
                                    (when result (reset! selected? true))
                                    result))]
                    (apply d/q query database inputs)))]
     {:result (set result) :selected? @selected?})))

(deftest ordered-count-aggregate-is-differentially-correct
  (doseq [[label inputs selected?]
          [[:partial-filter [250 9] true]
           [:empty-filter [250 -1] false]
           [:full-filter [250 19] true]
           [:empty-producer-range [2000 19] true]]]
    (testing (name label)
      (let [baseline (run-query filtered-count-query inputs false)
            ordered (run-query filtered-count-query inputs true)]
        (is (= (:result baseline) (:result ordered)))
        (is (= selected? (:selected? ordered)))))))

(deftest unsupported-count-basis-falls-back
  (let [baseline (run-query unsupported-count-query [] false)
        enabled (run-query unsupported-count-query [] true)]
    (is (= (:result baseline) (:result enabled)))
    (is (false? (:selected? enabled)))))

(deftest nonunique-producer-falls-back
  (let [baseline (run-query nonunique-producer-query [] false)
        enabled (run-query nonunique-producer-query [] true)]
    (is (= (:result baseline) (:result enabled)))
    (is (false? (:selected? enabled)))))

(deftest attribute-reference-indexes-are-supported
  (let [database @attribute-ref-db
        baseline (run-query filtered-count-query [250 9] false database)
        ordered (run-query filtered-count-query [250 9] true database)]
    (is (= (:result baseline) (:result ordered)))
    (is (true? (:selected? ordered)))))
