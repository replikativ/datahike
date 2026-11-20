(ns datahike.test.query-wildcard-point-test
  "Regression test for replikativ/datahike#1168.

   A single entity group whose scan is a ground-value AVET seek and whose merge
   pattern has `_` as its value, such as

     [:find ?s :in $ ?si :where [?s :stmt/item ?si] [?s :stmt/status _]]

   returned #{} since prepared execution became the default (#1167). The point
   fast paths read `_` as a ground value and compared every stored value with
   the symbol. `_` matches any value: it asks only that the attribute is present.

   The planner-off result is the oracle. The scalar, inlined and collection
   forms must all equal it."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]))

(def ^:private schema
  [{:db/ident :stmt/item :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :stmt/status :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/at :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? false}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn schema)
      conn)))

(defn- load!
  "n-items items; statement i belongs to item (mod i n-items); a statement has a
   status when (pred i). Returns [db first-item-eid]."
  [conn n-stmts n-items status?]
  (let [items (mapv (fn [k] {:db/id (- (inc k)) :item/at k}) (range n-items))
        stmts (mapv (fn [i]
                      (cond-> {:db/id (- (+ n-items i 1))
                               :stmt/item (- (inc (mod i n-items)))}
                        (status? i) (assoc :stmt/status :found)))
                    (range n-stmts))
        r (d/transact conn (into items stmts))]
    [(d/db conn) (get (:tempids r) -1)]))

(def ^:private scalar-q
  '[:find ?s :in $ ?si :where [?s :stmt/item ?si] [?s :stmt/status _]])

(def ^:private scalar-swapped-q
  '[:find ?s :in $ ?si :where [?s :stmt/status _] [?s :stmt/item ?si]])

(def ^:private coll-q
  '[:find ?s :in $ [?si ...] :where [?s :stmt/item ?si] [?s :stmt/status _]])

(defn- inlined-q [si]
  [:find '?s :where ['?s :stmt/item si] '[?s :stmt/status _]])

(defn- oracle [f]
  (binding [q/*disable-planner* true] (f)))

(defn- agree
  "Planner result for `f`, checked against the planner-off result."
  [f]
  (let [expected (oracle f)
        actual (f)]
    (is (= expected actual))
    actual))

(deftest wildcard-merge-after-ref-scan-test
  (testing "the reported shape: one item, three statements, all with a status"
    (let [[db si] (load! (fresh-conn) 3 1 (constantly true))]
      (is (= 3 (count (agree #(d/q scalar-q db si)))))
      (is (= 3 (count (agree #(d/q (inlined-q si) db)))))
      (is (= 3 (count (agree #(d/q coll-q db [si])))))
      (is (= 3 (count (agree #(d/q scalar-swapped-q db si)))))))
  (testing "one statement with one status"
    (let [[db si] (load! (fresh-conn) 1 1 (constantly true))]
      (is (= 1 (count (agree #(d/q scalar-q db si)))))
      (is (= 1 (count (agree #(d/q (inlined-q si) db)))))))
  (testing "ten statements over five items, all with a status"
    (let [[db si] (load! (fresh-conn) 10 5 (constantly true))]
      (is (= 2 (count (agree #(d/q scalar-q db si)))))
      (is (= 2 (count (agree #(d/q (inlined-q si) db)))))
      (is (= 10 (count (agree #(d/q coll-q db (d/q '[:find [?i ...] :where [?i :item/at _]] db))))))))
  (testing "six statements over two items, one with a status"
    (let [[db si] (load! (fresh-conn) 6 2 #{0})]
      (is (= 1 (count (agree #(d/q scalar-q db si)))))
      (is (= 1 (count (agree #(d/q (inlined-q si) db)))))))
  (testing "a statement without the merged attribute is dropped"
    (let [[db si] (load! (fresh-conn) 4 1 #{0 2})]
      (is (= 2 (count (agree #(d/q scalar-q db si)))))
      (is (= 2 (count (agree #(d/q (inlined-q si) db))))))))

(deftest wildcard-merge-with-other-merges-test
  (testing "`_` beside a variable merge and a ground merge"
    (let [[db si] (load! (fresh-conn) 3 1 (constantly true))
          q-var '[:find ?s ?x :in $ ?si
                  :where [?s :stmt/item ?si] [?s :stmt/status _] [?s :stmt/status ?x]]
          q-lit '[:find ?s :in $ ?si
                  :where [?s :stmt/item ?si] [?s :stmt/status _] [?s :stmt/status :found]]]
      (is (= 3 (count (agree #(d/q q-var db si)))))
      (is (= 3 (count (agree #(d/q q-lit db si))))))))
