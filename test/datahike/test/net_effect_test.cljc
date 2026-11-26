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
            [datahike.datom :as dd]
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
    :db/noHistory true}
   {:db/ident :s :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :flag :db/valueType :db.type/boolean :db/cardinality :db.cardinality/many}
   {:db/ident :blob :db/valueType :db.type/bytes :db/cardinality :db.cardinality/many}
   {:db/ident :u :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :self :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :a :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :b :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :a+b :db/valueType :db.type/tuple :db/tupleAttrs [:a :b]
    :db/cardinality :db.cardinality/one}])

(def ^:private user-attrs #{:one :many :idx :tags :quiet :blob :u :self :a :b :a+b :s :flag})

(defn- bytes-of [& xs] #?(:clj (byte-array xs) :cljs (js/Uint8Array. (clj->js xs))))

(defn- comparable [v]
  (if #?(:clj (bytes? v) :cljs (instance? js/Uint8Array v)) [:bytes (vec v)] v))

(defn- base-db
  "A history database with two entities (ids 101 and 102) and some earlier history."
  [history?]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? history? :schema-flexibility :write}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn schema)
    (d/transact conn [[:db/add 101 :one 24] [:db/add 101 :many :x] [:db/add 101 :idx 5]
                      [:db/add 101 :tags 1] [:db/add 101 :quiet 7] [:db/add 102 :one 30]
                      [:db/add 101 :blob (bytes-of 1 2 3)] [:db/add 101 :u 1]
                      [:db/add 101 :self 101] [:db/add 102 :self 101]
                      {:db/id 102 :a 1 :b 2}
                      [:db/add 101 :s "Aa"] [:db/add 101 :flag false]])
    ;; an earlier overwrite and an earlier retraction, so history is not empty
    (d/transact conn [[:db/add 102 :one 31] [:db/add 102 :many :old]])
    (d/transact conn [[:db/add 102 :one 30] [:db/retract 102 :many :old]])
    (let [db @conn]
      (d/release conn)
      db)))

(defn- full [d] [(:e d) (:a d) (comparable (:v d)) (:tx d) (:added d)])

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
  ;; the composite tuple is derived, never written directly
  (set (map (juxt :e :a :v) (filter #(and (user-attrs (:a %)) (not= :a+b (:a %)))
                                    (di/-seq (:eavt db))))))

(defn- run [db ops]
  (d/with db {:tx-data ops :tx-meta {:db/txInstant instant}}))

(defn- net-ops
  "The net change between two current states, as retractions then assertions."
  [db-before db-after]
  (let [key (fn [[e a v]] [e a (comparable v)])
        b (into {} (map (juxt key identity)) (eav-set db-before))
        a (into {} (map (juxt key identity)) (eav-set db-after))]
    (concat (for [k (sort-by str (keys b)) :when (not (contains? a k))] (into [:db/retract] (b k)))
            (for [k (sort-by str (keys a)) :when (not (contains? b k))] (into [:db/add] (a k))))))

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
   "bytes retract, add equal contents"          [[:db/retract 101 :blob (bytes-of 1 2 3)] [:db/add 101 :blob (bytes-of 1 2 3)]]
   "bytes add, retract"                         [[:db/add 101 :blob (bytes-of 9)] [:db/retract 101 :blob (bytes-of 9)]]
   "unique value moved and moved back"          [[:db/add 101 :u 2] [:db/add 102 :u 1] [:db/add 102 :u 3] [:db/add 101 :u 1]]
   "tuple source set twice"                     [[:db/add 102 :a 5] [:db/add 102 :a 6]]
   "tuple source away and back"                 [[:db/add 102 :b 7] [:db/add 102 :b 2]]
   "tuple built from nothing"                   [{:db/id 101 :a 3 :b 4}]
   "retractEntity with a reference to itself"   [[:db/retractEntity 101]]
   "a tempid resolved late, by upsert"          [[:db/add "t" :one 5] [:db/add "t" :one 6] [:db/add "t" :u 1]]
   ;; "Aa" and "BB" have the same hash
   "card-one over a value of equal hash"        [[:db/add 101 :s "BB"] [:db/add 101 :s "C"]]
   "card-one to a value of equal hash"          [[:db/add 101 :s "BB"]]
   "false retract, add"                         [[:db/retract 101 :flag false] [:db/add 101 :flag false]]
   "false add, retract next to true"            [[:db/add 102 :flag true] [:db/add 102 :flag false] [:db/retract 102 :flag false]]
   "true add, retract next to false"            [[:db/add 101 :flag true] [:db/retract 101 :flag true]]
   "false kept while true is added"             [[:db/retract 101 :flag false] [:db/add 101 :flag false] [:db/retract 101 :flag false] [:db/add 101 :flag true]]
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

(deftest operations-still-see-each-other
  (testing "a transaction function runs on the state the earlier operations left"
    (let [db (base-db true)
          seen (atom nil)
          r (run db [[:db/add 101 :one 25]
                     [:db.fn/call (fn [db]
                                    (reset! seen (:v (first (d/datoms db :eavt 101 :one))))
                                    [[:db/add 101 :one 21]])]])]
      (is (= 25 @seen))
      (is (= #{[101 :one 24 false] [101 :one 21 true]}
             (set (map (juxt :e :a :v :added) (filter #(= :one (:a %)) (:tx-data r)))))))))

(deftest retracting-what-is-not-there-reports-nothing
  (let [db (base-db true)
        r (run db [(dd/datom 101 :many :absent (inc (:max-tx db)) false)])]
    (is (empty? (filter #(user-attrs (:a %)) (:tx-data r))))))

(deftest a-write-dated-to-another-transaction-is-stored-as-given
  (testing "such input replays recorded data; the transaction is not netted"
    (let [db (base-db true)
          earlier (dec (:max-tx db))
          r (run db [[:db/add 101 :many :n earlier] [:db/retract 101 :many :n]])]
      (is (= [true false] (map :added (filter #(= :n (:v %)) (:tx-data r))))))))

(deftest a-transaction-that-purges-is-left-as-written
  (testing "a purge rewrites history itself; such a transaction is not netted"
    (let [db (base-db true)
          about (fn [db v] (filter #(= v (:v %)) (di/-seq (:temporal-eavt db))))]
      (testing "asserted, then purged: nothing of the value is left"
        (let [r (run db [[:db/add 101 :many :n] [:db/purge 101 :many :n]])]
          (is (empty? (about (:db-after r) :n)))
          (is (empty? (d/datoms (:db-after r) :eavt 101 :many :n)))
          (is (= (:hash (:db-after (run db []))) (:hash (:db-after r))))))
      (testing "retracted, then purged"
        (let [r (run db [[:db/retract 101 :many :x] [:db/purge 101 :many :x]])]
          (is (= [false] (map :added (filter #(= :x (:v %)) (:tx-data r)))))
          (is (empty? (about (:db-after r) :x)))))
      (testing "purged, then asserted again"
        (let [r (run db [[:db/purge 101 :many :x] [:db/add 101 :many :x]])]
          (is (= [true] (map :added (filter #(= :x (:v %)) (:tx-data r)))))
          (is (seq (d/datoms (:db-after r) :eavt 101 :many :x)))))
      (testing "other values in the same transaction are written step by step"
        (let [r (run db [[:db/add 101 :many :n] [:db/retract 101 :many :n]
                         [:db/purge 101 :many :x]])]
          (is (= [true false] (map :added (filter #(= :n (:v %)) (:tx-data r))))))))))

(deftest a-foreign-dated-operation-that-changes-nothing-does-not-count
  (let [db (base-db true)
        earlier (dec (:max-tx db))
        r (run db [[:db/add 101 :many :x earlier]
                   [:db/add 101 :many :n] [:db/retract 101 :many :n]])]
    (is (empty? (filter #(= :n (:v %)) (:tx-data r))))))

(deftest a-schema-change-taken-back-leaves-the-attribute-as-written
  (testing "the steps were recorded under a schema the end of the transaction no
            longer shows, so the attribute's keys are not netted"
    (let [db (base-db true)
          r (run db [[:db/add :quiet :db/noHistory false]
                     [:db/add 101 :quiet 8] [:db/add 101 :quiet 7]
                     [:db/add :quiet :db/noHistory true]])]
      (is (= #{7} (set (map :v (d/datoms (:db-after r) :eavt 101 :quiet))))))))

(deftest undeclared-attributes-are-netted-too
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true :schema-flexibility :read}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [[:db/add 101 :free 1]])
    (let [r (d/with @conn {:tx-data [[:db/add 101 :other 5] [:db/retract 101 :other 5]
                                     [:db/add 101 :free 2] [:db/add 101 :free 3]]})]
      (is (= [[:free 1 false] [:free 3 true]]
             (map (juxt :a :v :added) (filter #(#{:free :other} (:a %)) (:tx-data r)))))
      (is (empty? (filter #(= :other (:a %)) (di/-seq (:temporal-eavt (:db-after r)))))))
    (d/release conn)))

(deftest values-the-index-cannot-order-do-not-break-a-transaction
  (testing "two unequal maps of equal hash never meet in an index; tracking the
            transaction must not compare them into an exception"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? false :schema-flexibility :read}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (d/transact conn [[:db/add 101 :m {:v "Aa"}]])
      (let [r (d/with @conn {:tx-data [[:db/retract 101 :m {:v "Aa"}] [:db/add 101 :m {:v "BB"}]
                                       [:db/add 101 :n 1] [:db/retract 101 :n 1]]})]
        (is (= [{:v "BB"}] (map :v (d/datoms (:db-after r) :eavt 101 :m)))))
      (d/release conn))))

(deftest a-key-the-index-cannot-compare-is-left-as-written
  (testing "the removed map and the surviving one have equal hashes and cannot
            be ordered; the transaction is accepted, as it was before"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? true :schema-flexibility :read}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (d/transact conn [{:db/ident :m :db/cardinality :db.cardinality/many}])
      (let [r (d/with @conn {:tx-data [[:db/add 101 :m {:v "Aa"}]
                                       [:db/retract 101 :m {:v "Aa"}]
                                       [:db/add 101 :m {:v "BB"}]]})]
        (is (= [{:v "BB"}] (map :v (d/datoms (:db-after r) :eavt 101 :m)))))
      (d/release conn))))
