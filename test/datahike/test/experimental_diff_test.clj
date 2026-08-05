(ns datahike.test.experimental-diff-test
  "`datahike.experimental.diff` — what changed between two database values.

   The oracle for `tx-range` is datahike ITSELF: a transaction's `:tx-data` is
   what the transactor said it wrote, so a reconstruction from the indexes that
   disagrees with it is wrong by definition. Every shape that makes history
   non-obvious is in the window."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.experimental.diff :as xd]
            [datahike.versioning :as dv]
            [org.replikativ.persistent-sorted-set :as psset])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet]))

(defn- cfg [& {:keys [history? index] :or {history? true index :datahike.index/persistent-set}}]
  {:store {:backend :file
           :path (str (System/getProperty "java.io.tmpdir") "/dh-xdiff-" (java.util.UUID/randomUUID))
           :id (java.util.UUID/randomUUID)}
   :keep-history? history?
   :schema-flexibility :write
   :index index})

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/string :db/cardinality :db.cardinality/many}])

(defn- tuple [d] [(:e d) (:a d) (:v d) (:tx d) (:added d)])

(defmacro ^:private with-conn [[sym c] & body]
  `(let [c# ~c]
     (d/create-database c#)
     (let [~sym (d/connect c#)]
       (try ~@body (finally (d/release ~sym) (d/delete-database c#))))))

;; ---------------------------------------------------------------------------

(deftest tx-range-reproduces-what-the-transactor-reported
  (testing "every transaction in the window, datom for datom, against :tx-data"
    (with-conn [conn (cfg)]
      (d/transact conn schema)
      (d/transact conn [{:name "a" :age 1 :tag ["x" "y"]} {:name "b" :age 2}])
      (let [before  @conn
            reports [(d/transact conn [{:name "a" :age 99}])                  ; card-one overwrite
                     (d/transact conn [[:db/retractEntity [:name "b"]]])      ; whole entity
                     (d/transact conn [{:name "c" :age 3}])                   ; plain assertion
                     (d/transact conn [[:db/retract [:name "a"] :tag "x"]])   ; card-many retract
                     ;; asserted AND retracted inside ONE transaction: never
                     ;; reaches the current index at all
                     (d/transact conn [{:name "c" :age 4}
                                       [:db/retract [:name "c"] :age 4]])]
            after   @conn
            expected (into (sorted-map)
                           (for [r reports]
                             [(-> r :tx-data first :tx) (set (map tuple (:tx-data r)))]))
            actual   (into (sorted-map)
                           (for [{:keys [t data]} (xd/tx-range before after)]
                             [t (set (map tuple data))]))]
        (is (= 5 (count actual)) "one entry per transaction in the window")
        (is (= expected actual))))))

(deftest tx-range-is-ordered-and-half-open
  (testing "oldest first, db-before exclusive, db-after inclusive"
    (with-conn [conn (cfg)]
      (d/transact conn schema)
      (d/transact conn [{:name "a"}])
      (let [before @conn
            ts     (mapv #(-> (d/transact conn [{:name (str "e" %)}]) :tx-data first :tx)
                         (range 4))
            after  @conn
            got    (xd/tx-range before after)]
        (is (= ts (mapv :t got)) "in transaction order, and neither endpoint's own tx leaks in")
        (is (= [] (xd/tx-range after after)) "an empty window is empty, not everything")))))

(deftest diff-reports-what-entered-and-left-the-current-index
  (with-conn [conn (cfg)]
    (d/transact conn schema)
    (d/transact conn [{:name "a" :age 1} {:name "b" :age 2}])
    (let [before @conn
          _      (d/transact conn [{:name "a" :age 99}])
          _      (d/transact conn [[:db/retractEntity [:name "b"]]])
          after  @conn
          {:keys [added removed]} (xd/diff before after)
          present? (fn [db d] (boolean (seq (d/datoms db :eavt (:e d) (:a d) (:v d)))))]
      (is (seq added))
      (is (seq removed))
      (is (every? #(present? after %) added) "everything :added is in db-after")
      (is (not-any? #(present? before %) added) "and in none of db-before")
      (is (every? #(present? before %) removed) "everything :removed was in db-before")
      (is (not-any? #(present? after %) removed) "and is gone from db-after")
      (is (contains? (set (map (juxt :a :v) added)) [:age 99])
          "the new card-one value is an addition")
      (is (contains? (set (map (juxt :a :v) removed)) [:age 1])
          "the value it superseded is a removal"))))

(deftest diff-of-a-database-with-itself-is-empty
  (with-conn [conn (cfg)]
    (d/transact conn schema)
    (d/transact conn [{:name "a" :age 1}])
    (let [db @conn]
      (is (= {:added [] :removed []} (xd/diff db db))))))

;; ---------------------------------------------------------------------------
;; refusals — each one is a wrong answer this would otherwise give quietly

(deftest tx-range-refuses-without-history
  (testing "a retraction leaves no record of the tx that made it, so the window
            cannot be reconstructed — and reporting a partial window would be
            worse than refusing"
    (with-conn [conn (cfg :history? false)]
      (d/transact conn schema)
      (d/transact conn [{:name "a" :age 1}])
      (let [before @conn
            _      (d/transact conn [{:name "b"}])
            after  @conn]
        (is (= :diff/history-required
               (try (xd/tx-range before after) nil
                    (catch Exception e (:error (ex-data e))))))
        (testing "diff still works there"
          (is (seq (:added (xd/diff before after)))))))))

(deftest diff-refuses-a-view-rather-than-answering-nothing
  (testing "as-of / since / history / filter wrap an origin db and hold no
            indexes of their own, so diffing them would find no trees and report
            no changes"
    (with-conn [conn (cfg)]
      (d/transact conn schema)
      (d/transact conn [{:name "a" :age 1}])
      (let [before @conn
            _      (d/transact conn [{:name "b"}])
            after  @conn
            err    #(try (xd/diff %1 %2) nil (catch Exception e (:error (ex-data e))))]
        (is (= :diff/not-a-database (err (d/history before) (d/history after))))
        (is (= :diff/not-a-database (err before (d/as-of after (:max-tx after)))))
        (is (= :diff/not-a-database (err (d/since before (:max-tx before)) after)))))))

(deftest diff-refuses-reversed-arguments
  (with-conn [conn (cfg)]
    (d/transact conn schema)
    (let [before @conn
          _      (d/transact conn [{:name "a"}])
          after  @conn]
      (is (= :diff/reversed
             (try (xd/diff after before) nil
                  (catch Exception e (:error (ex-data e)))))))))

;; ---------------------------------------------------------------------------

(deftest diff-reads-the-delta-not-the-database
  (testing "THE reason this exists: the reader pays for the change, not for the
            database.

            The two db values come from `branch-history`, i.e. the COMMIT GRAPH,
            which is both the way a catch-up consumer would really get them and
            the only way to get them cold. Taking `@conn` before and after a
            transact instead measures nothing: `transact` walks the tree on its
            way in, so every node the diff wants is already resident and the read
            count is 0 whatever the algorithm does. Measured, at 5k / 20k / 80k
            datoms — 0 reads every time, and the assertion passed vacuously."
    (doseq [n [2000 20000]]
      (let [c (cfg)]
        (d/create-database c)
        (let [conn (d/connect c)]
          (d/transact conn schema)
          (d/transact conn (vec (for [i (range n)] {:name (str "e" i) :age i})))
          (d/transact conn [{:name "e0" :age 999999}])
          (d/release conn))
        (let [conn    (d/connect c)
              history (async/<!! (dv/branch-history conn))
              after   (first history)
              before  (second history)
              storage (.-_storage ^PersistentSortedSet (:eavt after))
              stats   (:stats storage)]
          (is (> (count history) 1) "the commit graph must have the two versions")
          (swap! stats assoc :reads 0)
          (let [{:keys [added removed]} (xd/diff before after)
                reads (:reads @stats)
                ;; how many nodes the tree HAS, counted the expensive way, so
                ;; the comparison is against the whole tree rather than against
                ;; whatever the last commit happened to write
                nodes (let [seen (volatile! 0)]
                        (psset/walk-addresses (:eavt after) (fn [_] (vswap! seen inc) true))
                        @seen)]
            ;; two additions, not one: the new :age plus the transaction entity's
            ;; own :db/txInstant, which is a datom like any other
            (is (= #{[:age 999999] [:db/txInstant]}
                   (set (map #(if (= :db/txInstant (:a %)) [(:a %)] [(:a %) (:v %)]) added)))
                "the new value and the transaction's own datom")
            (is (= [[:age 0]] (map (juxt :a :v) removed)) "the value it replaced")
            (is (> nodes 8) (str "n=" n ": the tree must be big enough for pruning to mean something"))
            (is (< reads (quot nodes 2))
                (str "n=" n ": read " reads " nodes of " nodes " for a one-datom delta")))
          (d/release conn))
        (d/delete-database c)))))
