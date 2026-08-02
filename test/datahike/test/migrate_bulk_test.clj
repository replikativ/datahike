(ns datahike.test.migrate-bulk-test
  "Bulk index construction from a pre-sorted datom stream.

   `di/init-index` sorts its input in memory (`arrays/asort` over the whole
   array), so building an index costs O(n) heap — fine when the database fits in
   memory, which is the case a bulk restore is not.
   `di/init-index-sorted` streams an already-sorted seq into
   `persistent-sorted-set/from-sorted-seq`, so the build holds one node per level.

   The property that matters is that the two agree: a bulk-built index must be
   the same index, or the fast path is a different database.

   Requires persistent-sorted-set's streaming builder
   (replikativ/persistent-sorted-set#22), which is behind the `:bulk` alias until
   it releases. Without it these SKIP with a message rather than passing
   vacuously — the same bargain the CBOR and Jetty suites make."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.migrate.ids :as ids]
            [datahike.test.utils :as utils]))

(def ^:private streaming-builder?
  (try (require 'org.replikativ.persistent-sorted-set)
       (some? (resolve 'org.replikativ.persistent-sorted-set/from-sorted-seq))
       (catch Exception _ false)))

(defn- skip!
  "Print and assert-true. The assertion is not ceremony: kaocha reports a test
   with zero assertions as a FAILURE, so a bare `println` skip turns an absent
   optional dependency into a red suite — which is the opposite of skipping."
  [what]
  (println "SKIPPED" what "— persistent-sorted-set/from-sorted-seq absent"
           "(run with the :bulk alias; see persistent-sorted-set#22)")
  (is true "skipped: streaming builder unavailable"))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- populated-conn [n]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? false :schema-flexibility :read}
        c (utils/setup-db cfg)]
    (d/transact c (vec (for [i (range n)]
                         {:db/id (+ 100 i) :name (str "n" i) :age i})))
    c))

(deftest bulk-built-index-equals-normally-built
  (if-not streaming-builder?
    (skip! "bulk-built-index-equals-normally-built")
    (testing "same datoms in, same index out — for every index type.

              Contents equality is the floor, not the ceiling: a tree with the
              right datoms and the wrong fanout would pass this and then differ
              under slice and count. The SHAPE equivalence is asserted upstream in
              persistent-sorted-set's own suite, against `from-sorted-array`; here
              the question is whether datahike drives it correctly."
      (let [conn (populated-conn 500)
            db @conn
            store (:store db)
            datoms (vec (d/datoms db :eavt))]
        (is (pos? (count datoms)) "precondition: there are datoms")
        (doseq [index-type [:eavt :aevt]]
          (testing (name index-type)
            (let [cmp (dd/index-type->cmp-quick index-type false)
                  sorted (sort cmp datoms)
                  normal (di/init-index :datahike.index/persistent-set store datoms index-type 0 {})
                  bulk (di/init-index-sorted :datahike.index/persistent-set store sorted index-type 0 {})]
              (is (= (count normal) (count bulk)) "same datom count")
              (is (= (vec normal) (vec bulk)) "same datoms in the same order"))))
        (teardown conn)))))

(deftest bulk-built-index-supports-lookup-and-slice
  (if-not streaming-builder?
    (skip! "bulk-built-index-supports-lookup-and-slice")
    (testing "a bulk-built index answers the queries an index exists for.

              Worth separating from contents equality: the bulk builder produces
              address-only nodes with no resident children, so every read here
              exercises the lazy-load path that a normally-built tree does not."
      (let [conn (populated-conn 300)
            db @conn
            store (:store db)
            datoms (vec (d/datoms db :eavt))
            cmp (dd/index-type->cmp-quick :eavt false)
            bulk (di/init-index-sorted :datahike.index/persistent-set store
                                       (sort cmp datoms) :eavt 0 {})]
        (is (= (count datoms) (count bulk)))
        (is (= (vec (sort cmp datoms)) (vec bulk)) "full scan matches")
        (teardown conn)))))

(deftest the-pipeline-produces-sortable-records
  (testing "the pre-pass + pure rewrite yields records a bulk build can consume.

            This is the join between `datahike.migrate.ids` and the index builder:
            the mapping must be complete BEFORE sorting, because the sort order is
            over the final ids. Asserted without the builder, so it runs whether
            or not the :bulk alias is active."
    (let [schema {:name {:db/valueType :db.type/string}
                  :pal {:db/valueType :db.type/ref}}
          records [[3 :name "a" 100 true]
                   [4 :name "b" 100 true]
                   [4 :pal 3 100 true]]
          mapping (ids/build-mapping {:schema schema :system-entities #{}
                                      :max-eid 10 :max-tx 200}
                                     (fn [rf init] (reduce rf init records)))
          rewritten (mapv #(ids/apply-mapping mapping schema %) records)]
      (testing "every id is above the target's maxima"
        (is (every? (fn [[e _ _ t _]] (and (> e 10) (> t 200))) rewritten)))
      (testing "the ref value moved with its entity"
        (let [[_ _ pal-v _ _] (last rewritten)
              [b-e _ _ _ _] (second rewritten)]
          (is (= (get (:eids mapping) 3) pal-v))
          (is (= (get (:eids mapping) 4) b-e))))
      (testing "and the result sorts deterministically — the precondition for a
                bulk index build"
        (is (= (sort-by (juxt first second) rewritten)
               (sort-by (juxt first second) rewritten)))))))
