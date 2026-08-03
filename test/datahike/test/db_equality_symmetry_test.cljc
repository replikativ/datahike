(ns datahike.test.db-equality-symmetry-test
  "`Object.equals` must be symmetric: `(.equals a b)` and `(.equals b a)` have to
   agree, for every pair, always. `#939` established the contract for DB and the
   views by stopping `.equals` from throwing; this covers the remaining half.

   `equiv-db` guarded on the type of `other` alone:

     (and (or (instance? DB other) (instance? FilteredDB other)) ...)

   so a HistoricalDB / AsOfDB / SinceDB passed the guard whenever it appeared as
   the LEFT argument, and went on to compare schema and datoms — which for a
   history view with no retractions are the same as its origin's. The result was
   `(= view db)` true and `(= db view)` false.

   Concretely that means a `java.util.HashSet` can hold both and report a size of
   2 while one of them insists they are the same value, and `contains?` answers
   differently depending on which one was inserted first."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(defn- with-conn [keep-history? f]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? keep-history? :schema-flexibility :write}
        conn (utils/setup-db cfg)]
    (try
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/unique :db.unique/identity}
                        {:db/ident :tag :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/many}])
      (f conn)
      (finally (d/release conn)))))

(defn- symmetric?
  "Both directions of `=` agree. Reported as a pair so a failure shows which way
   round it went rather than just `false`."
  [a b]
  [(boolean (= a b)) (boolean (= b a))])

;; ---------------------------------------------------------------------------

(deftest equality-is-symmetric-between-a-db-and-every-view
  (testing "no view may be content-equal to a DB in one direction only"
    (doseq [keep-history? [false true]]
      (with-conn keep-history?
        (fn [conn]
          (d/transact conn [{:db/id -1 :name "a" :tag :x}])
          (let [db @conn
                views (cond-> {"FilteredDB" (d/filter db (fn [_ _] true))}
                        keep-history?
                        (assoc "HistoricalDB" (d/history db)
                               "AsOfDB" (d/as-of db (java.util.Date.))
                               "SinceDB" (d/since db (java.util.Date. 0))))]
            (doseq [[label view] views]
              (testing (str label " (keep-history? " keep-history? ")")
                (let [[fwd bwd] (symmetric? db view)]
                  (is (= fwd bwd)
                      (str "= must agree in both directions, got (= db view)=" fwd
                           " (= view db)=" bwd)))
                (is (= (.equals ^Object db view) (.equals ^Object view db))
                    ".equals must agree in both directions")))))))))

(deftest a-view-is-equal-to-itself
  (testing "reflexivity — the case the identity check in `equiv-db` exists for,
            and which the guard must not break"
    (with-conn true
      (fn [conn]
        (d/transact conn [{:db/id -1 :name "a" :tag :x}])
        (let [db @conn]
          (doseq [[label v] {"db" db
                             "FilteredDB" (d/filter db (fn [_ _] true))
                             "HistoricalDB" (d/history db)
                             "AsOfDB" (d/as-of db (java.util.Date.))
                             "SinceDB" (d/since db (java.util.Date. 0))}]
            (testing label
              (is (= v v))
              (is (.equals ^Object v v)))))))))

(deftest views-behave-in-java-collections
  (testing "a HashSet holding a db and a view must not depend on insertion order.

            This is the user-visible shape of an asymmetric `equals`: with the
            bug, inserting the db first and the view second gives a different
            size than the reverse, because HashSet consults `equals` in one
            direction only."
    (with-conn true
      (fn [conn]
        (d/transact conn [{:db/id -1 :name "a" :tag :x}])
        (let [db @conn
              view (d/history db)
              size-of (fn [& xs]
                        (let [s (java.util.HashSet.)]
                          (doseq [x xs] (.add s x))
                          (.size s)))]
          (is (= (size-of db view) (size-of view db))
              "HashSet size must not depend on insertion order"))))))

(deftest a-db-is-not-equal-to-a-non-db
  (testing "the guard must still answer false rather than throwing, in both
            directions, for things that are not databases at all"
    (with-conn false
      (fn [conn]
        (let [db @conn]
          (is (false? (= db 42)))
          (is (false? (= 42 db)))
          (is (false? (= db nil)))
          (is (false? (.equals ^Object db "not a db")))
          (is (false? (java.util.Objects/equals db {}))))))))
