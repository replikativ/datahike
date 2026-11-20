(ns datahike.test.query-driver-rotation-test
  "The execute-time choice of an entity group's driving pattern compares ROWS with ROWS.

   `parameterize-entity-group` may rotate a merge pattern into the scan slot when a live
   relation binds one of its vars, because a seek from a few bound keys can beat the
   planned scan. It scored the candidate by its number of probe KEYS and compared that
   with the planned scan's estimated ROWS. A value key is not a row: two values of a
   six-valued attribute select a third of the database. So a selective constant scan lost
   its slot to a two-element collection input (#1107: 40 ms a query on 300k entities,
   where the relational engine took 0.4 ms).

   The rotation is observed, not inferred: `execute-fused-scan-rel` reaches the
   probe-driven scan only for the pattern that ended up driving."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.query :as dq]
            [datahike.query.execute :as dqe]))

(def ^:private kinds ["k0" "k1" "k2" "k3" "k4" "k5"])
(def ^:private n 30000)

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :item/key :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :item/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :item/kind :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}])
      (doseq [b (partition-all 10000 (range n))]
        (d/transact conn (mapv (fn [i] {:item/key (str "id" i) :item/name (str "n" (mod i 2000))
                                        :item/kind (kinds (mod i 6))}) b)))
      [cfg conn])))

;; which attributes drove a probe-driven scan while `f` ran
(defn- probe-driven-attrs [f]
  (let [seen (atom [])
        orig @#'dqe/probe-driven-iterable]
    (with-redefs [dqe/probe-driven-iterable (fn [index resolved-a probe-set field]
                                              (swap! seen conj resolved-a)
                                              (orig index resolved-a probe-set field))]
      (let [r (f)] {:result r :attrs @seen}))))

(deftest a-selective-scan-keeps-its-slot-against-a-broad-value-probe
  (let [[cfg conn] (fresh-db)
        db @conn
        q '[:find [?e ...] :in $ ?name [?kind ...]
            :where [?e :item/name ?name] [?e :item/kind ?kind]]
        reference (fn [& args] (binding [dq/*disable-planner* true] (set (apply d/q q db args))))]
    (try
      (doseq [nm ["n7" "n1000" "n1999"]]
        (let [{:keys [result attrs]} (probe-driven-attrs #(d/q q db nm ["k0" "k1" "k2" "k3" "k4" "k5"]))]
          (is (= 15 (count result)) "precondition: the name selects 15 entities")
          (is (= (reference nm kinds) (set result)))
          (is (not-any? #{:item/kind} attrs)
              "two-thirds of the database is not a better driver than 15 rows"))
        (let [{:keys [result attrs]} (probe-driven-attrs #(d/q q db nm ["k1" "k4"]))]
          (is (= (reference nm ["k1" "k4"]) (set result)))
          (is (not-any? #{:item/kind} attrs) "nor is a third of it")))
      (finally (d/release conn) (d/delete-database cfg)))))

(deftest a-selective-value-probe-still-takes-the-scan-slot
  (testing "the rotation this code exists for: a few unique keys against a broad constant scan"
    (let [[cfg conn] (fresh-db)
          db @conn
          q '[:find [?e ...] :in $ ?kind [?key ...]
              :where [?e :item/kind ?kind] [?e :item/key ?key]]
          ks ["id0" "id6" "id12" "id7"]
          reference (binding [dq/*disable-planner* true] (set (d/q q db "k0" ks)))]
      (try
        (let [{:keys [result attrs]} (probe-driven-attrs #(d/q q db "k0" ks))]
          (is (= 3 (count result)))
          (is (= reference (set result)))
          (is (some #{:item/key} attrs) "four unique keys drive, not the 5000-row kind scan"))
        (finally (d/release conn) (d/delete-database cfg))))))
