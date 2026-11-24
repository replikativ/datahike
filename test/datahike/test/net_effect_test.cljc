(ns datahike.test.net-effect-test
  "A transaction records its NET effect.

   Operations run in order and each sees the result of the one before it, but
   what the transaction leaves behind — `:tx-data`, the history indexes, the
   hash — is the difference between the database before and after it: at most
   one entry per `[e a v]`.

   The oracle: applying a sequence of operations in one transaction must give
   the same database as applying only its net change in one transaction. The net
   change is computed here from the two current states, independently of the
   transactor."
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.index.interface :as di]))

(def ^:private instant #inst "2026-01-01T00:00:00.000-00:00")

(def ^:private schema
  [{:db/ident :one :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :many :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :idx :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :tags :db/valueType :db.type/long :db/cardinality :db.cardinality/many
    :db/index true}
   {:db/ident :quiet :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/noHistory true}])

(def ^:private user-attrs #{:one :many :idx :tags :quiet})

(defn- base-db
  "A history database with two entities (ids 101 and 102) and some earlier history."
  [history?]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? history? :schema-flexibility :write}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn schema)
    (d/transact conn [[:db/add 101 :one 24] [:db/add 101 :many :x] [:db/add 101 :idx 5]
                      [:db/add 101 :tags 1] [:db/add 101 :quiet 7] [:db/add 102 :one 30]])
    ;; an earlier overwrite and an earlier retraction, so history is not empty
    (d/transact conn [[:db/add 102 :one 31] [:db/add 102 :many :old]])
    (d/transact conn [[:db/add 102 :one 30] [:db/retract 102 :many :old]])
    (let [db @conn]
      (d/release conn)
      db)))

(defn- full [d] [(:e d) (:a d) (:v d) (:tx d) (:added d)])

(defn- user-datoms [datoms]
  (set (map full (filter #(user-attrs (:a %)) datoms))))

(defn- image
  "Everything a transaction leaves behind for the user attributes, with the
   transaction id and added flag of every entry."
  [db]
  (cond-> {:current (user-datoms (di/-seq (:eavt db)))
           :aevt    (user-datoms (di/-seq (:aevt db)))
           :avet    (user-datoms (di/-seq (:avet db)))
           :hash    (:hash db)}
    (:temporal-eavt db)
    (assoc :temporal-eavt (user-datoms (di/-seq (:temporal-eavt db)))
           :temporal-aevt (user-datoms (di/-seq (:temporal-aevt db)))
           :temporal-avet (user-datoms (di/-seq (:temporal-avet db))))))

(defn- eav-set [db]
  (set (map (juxt :e :a :v) (filter #(user-attrs (:a %)) (di/-seq (:eavt db))))))

(defn- run [db ops]
  (d/with db {:tx-data ops :tx-meta {:db/txInstant instant}}))

(defn- net-ops
  "The net change between two current states, as retractions then assertions."
  [db-before db-after]
  (let [b (eav-set db-before) a (eav-set db-after)]
    (concat (for [[e at v] (sort-by str (set/difference b a))] [:db/retract e at v])
            (for [[e at v] (sort-by str (set/difference a b))] [:db/add e at v]))))

(defn- check-against-oracle [db ops]
  (let [actual (run db ops)
        oracle (run db (net-ops db (:db-after actual)))]
    (is (= (image (:db-after oracle)) (image (:db-after actual)))
        (str "the database after " (pr-str ops)))
    (is (= (user-datoms (:tx-data oracle)) (user-datoms (:tx-data actual)))
        (str ":tx-data after " (pr-str ops)))
    (is (= (count (user-datoms (:tx-data actual)))
           (count (filter #(user-attrs (:a %)) (:tx-data actual))))
        (str ":tx-data holds no entry twice after " (pr-str ops)))
    actual))

(def ^:private fixed-cases
  {"add, retract a new value (card-many)"       [[:db/add 101 :many :n] [:db/retract 101 :many :n]]
   "retract, add a present value (card-many)"   [[:db/retract 101 :many :x] [:db/add 101 :many :x]]
   "add, retract, add (card-many)"              [[:db/add 101 :many :n] [:db/retract 101 :many :n] [:db/add 101 :many :n]]
   "retract, add, retract (card-many)"          [[:db/retract 101 :many :x] [:db/add 101 :many :x] [:db/retract 101 :many :x]]
   "add, retract a new value (card-one)"        [[:db/add 102 :idx 9] [:db/retract 102 :idx 9]]
   "retract, add the present value (card-one)"  [[:db/retract 101 :one 24] [:db/add 101 :one 24]]
   "card-one set twice"                         [[:db/add 101 :one 25] [:db/add 101 :one 21]]
   "card-one away and back"                     [[:db/add 101 :one 25] [:db/add 101 :one 24]]
   "card-one revisits a passing value"          [[:db/add 102 :one 40] [:db/add 102 :one 35] [:db/add 102 :one 40]]
   "card-one set then retracted"                [[:db/add 101 :one 25] [:db/retract 101 :one 25]]
   "indexed card-one set twice"                 [[:db/add 101 :idx 6] [:db/add 101 :idx 7]]
   "indexed card-one away and back"             [[:db/add 101 :idx 6] [:db/add 101 :idx 5]]
   "indexed card-many retract, add"             [[:db/retract 101 :tags 1] [:db/add 101 :tags 1]]
   "indexed card-many add, retract"             [[:db/add 101 :tags 2] [:db/retract 101 :tags 2]]
   "noHistory set twice"                        [[:db/add 101 :quiet 8] [:db/add 101 :quiet 9]]
   "noHistory away and back"                    [[:db/add 101 :quiet 8] [:db/add 101 :quiet 7]]
   "retractEntity then one attribute again"     [[:db/retractEntity 101] [:db/add 101 :one 24]]
   "untouched keys alongside a cancelled one"   [[:db/add 102 :many :a] [:db/add 101 :many :n] [:db/retract 101 :many :n] [:db/add 102 :one 33]]})

(deftest a-transaction-records-its-net-effect
  (doseq [history? [true false]]
    (testing (str "keep-history? " history?)
      (let [db (base-db history?)]
        (doseq [[label ops] (sort-by key fixed-cases)]
          (testing label
            (check-against-oracle db ops)))))))

(deftest a-value-retracted-and-asserted-again-keeps-its-transaction
  (let [db (base-db true)
        before (first (d/datoms db :eavt 101 :many :x))
        after (first (d/datoms (:db-after (run db [[:db/retract 101 :many :x] [:db/add 101 :many :x]]))
                               :eavt 101 :many :x))]
    (is (= (:tx before) (:tx after)))))

(deftest as-of-a-transaction-is-what-it-left
  (testing "also once a later transaction has changed the same values"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? true :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          cur (fn [db a] (set (map :v (d/datoms db :eavt 101 a))))]
      (d/transact conn schema)
      (d/transact conn [[:db/add 101 :many :x] [:db/add 101 :one 30]])
      (let [r (d/transact conn [[:db/retract 101 :many :x] [:db/add 101 :many :x]
                                [:db/add 101 :one 40] [:db/add 101 :one 35] [:db/add 101 :one 40]])
            t (:max-tx (:db-after r))]
        (d/transact conn [[:db/retract 101 :many :x] [:db/add 101 :one 50]])
        (is (= #{:x} (cur (d/as-of @conn t) :many)))
        (is (= #{40} (cur (d/as-of @conn t) :one)))
        (is (= #{[:x]} (d/q '[:find ?v :where [101 :many ?v]] (d/as-of @conn t))))
        (is (= #{[40]} (d/q '[:find ?v :where [101 :one ?v]] (d/as-of @conn t)))))
      (d/release conn))))

;; ---------------------------------------------------------------------------
;; random sequences

(defn- lcg
  "A small deterministic generator, the same on both runtimes."
  [seed]
  (let [state (atom (long seed))]
    (fn [n]
      (let [s (mod (+ (* @state 1103515245) 12345) 2147483648)]
        (reset! state s)
        (mod (quot s 65536) n)))))

(defn- random-ops [rnd n]
  (vec (repeatedly
        n
        (fn []
          (let [e (+ 101 (rnd 2))
                [a vs] (nth [[:one [24 25 21 30]] [:many [:x :n :m]] [:idx [5 6 7]]
                             [:tags [1 2 3]] [:quiet [7 8]]]
                            (rnd 5))
                v (nth vs (rnd (count vs)))]
            (if (zero? (rnd 3))
              [:db/retract e a v]
              [:db/add e a v]))))))

(deftest random-sequences-equal-their-net-change
  (doseq [history? [true false]]
    (let [db (base-db history?)]
      (doseq [seed (range 300)]
        (let [rnd (lcg (+ 17 seed))
              ops (random-ops rnd (+ 2 (rnd 7)))]
          (check-against-oracle db ops))))))
