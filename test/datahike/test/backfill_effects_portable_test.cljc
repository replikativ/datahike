(ns datahike.test.backfill-effects-portable-test
  (:require #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])
            [datahike.api :as d]
            [datahike.backfill.effects :as effects]
            [datahike.datom :as dd]
            [datahike.db :as db]))

(defn- observed-add [source value]
  (effects/emit source :score :current :insert (dd/datom 10000 :score value 100) nil))

(defn- error-type [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs js/Error) e (:type (ex-data e)))))

#?(:cljs
   (deftest background-writes-are-explicitly-unsupported-on-cljs
     (is (= :avet-build-unsupported-platform
            (error-type #(d/begin-avet-build! nil {:score {:db/index true}}))))
     (is (= :avet-build-unsupported-platform
            (error-type #(d/cancel-avet-build! nil (random-uuid)))))))

(deftest observer-budgets-are-incremental-and-pure
  (let [source (effects/observe {} #{:score} {:max-effects 2})
        once (observed-add source 1)
        twice (observed-add once 2)]
    (is (= ::effects/observer-budget (error-type #(observed-add twice 3))))
    (is (empty? (effects/observations source)))
    (is (= 1 (count (effects/observations once))))
    (is (= 2 (count (effects/observations twice))))
    (is (= {} (effects/unobserve twice)))
    (is (= once (observed-add source 1))))
  (doseq [[options value]
          [[{:max-bytes 1024} (apply str (repeat 1024 "x"))]
           [{:max-depth 8} (nth (iterate vector 1) 10)]
           [{:max-nodes 20} (vec (range 100))]
           [{:max-bytes 1024} #?(:clj (byte-array 1024) :cljs (js/Uint8Array. 1024))]
           [{:max-bytes 1024} (with-meta [] {:large (apply str (repeat 1024 "x"))})]]]
    (let [source (effects/observe {} #{:score} options)]
      (is (= ::effects/observer-budget (error-type #(observed-add source value))))
      (is (empty? (effects/observations source)))))
  (doseq [options [true {:unknown 1} {:max-effects 0} {:max-bytes -1}
                   {:max-nodes 1.5} {:max-depth 65}]]
    (is (= ::effects/invalid-observer
           (error-type #(effects/observe {} #{:score} options)))))
  (let [source (effects/observe {} #{:score} {:max-bytes 1800})
        once (observed-add source 1)]
    (is (= ::effects/observer-budget (error-type #(observed-add once 2))))
    (is (= 1 (count (effects/observations once))))))

(deftest derived-effects-exhaust-budget-without-changing-source
  (let [source (d/db-with (db/empty-db nil {:index :datahike.index/persistent-set :schema-flexibility :write})
                          [{:db/ident :score :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :pair :db/valueType :db.type/tuple
                            :db/tupleAttrs [:score :missing]
                            :db/cardinality :db.cardinality/one}])
        observed (effects/observe source #{:score :pair} {:max-effects 1})]
    (is (= ::effects/observer-budget
           (error-type #(d/db-with observed [{:db/id 10000 :score 1}]))))
    (is (empty? (effects/observations observed)))
    (is (= (vec (:eavt source)) (vec (:eavt observed))))))

(deftest exact-effects-and-private-observer-are-portable
  (doseq [refs? [false true]]
    (let [source (d/db-with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? refs?
                                              :keep-history? true :schema-flexibility :write})
                            [{:db/ident :score :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}])
          active (assoc source :avet-build {:id (random-uuid) :attrs #{:score}})
          observed (effects/observe source #{:score})
          tx [{:db/id 10000 :score 4}]
          built (d/db-with active tx)
          watched (d/db-with observed tx)
          emitted (:avet-build-effects built)
          replayed (reduce effects/replay {:current (empty (:avet source))
                                           :temporal (empty (:temporal-avet source))} emitted)]
      (is (seq emitted))
      (is (= emitted (effects/observations watched)))
      (is (= [4] (mapv :v (:current replayed))))
      (is (nil? (:avet-build-effects active)))
      (is (empty? (effects/observations observed)))
      (is (not (effects/enabled? (effects/unobserve watched)))))))

(deftest schema-invalidation-remains-sticky-on-both-platforms
  (doseq [refs? [false true]]
    (let [source (d/db-with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? refs?
                                              :schema-flexibility :write})
                            [{:db/ident :score :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one :db/doc "original"}])
          active (assoc source :avet-build {:id (random-uuid) :attrs #{:score}})
          after (d/db-with active [[:db/add [:db/ident :score] :db/doc "temporary"]
                                   [:db/add [:db/ident :score] :db/doc "original"]])]
      (is (= (:schema source) (:schema after)))
      (is (:avet-build-invalidated? after))
      (is (nil? (:avet-build-invalidated? active))))))
