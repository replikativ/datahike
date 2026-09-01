(ns datahike.test.schema-state-validation-test
  "Pins the deferred end-of-transaction schema validation
   (transaction/validate-schema-changes!).

   History: check-schema-update (find-invalid-schema-updates + completeness)
   only guards the entity-map transaction path. Raw datom vectors
   [:db/add <attr> <schema-prop> <v>] reached update-schema, which validated
   only key-bearing-misuse — so retroactive :db/unique over duplicates,
   :db/valueType changes over existing data, and similar transitions were
   silently accepted, producing broken upserts, mixed-type indexes whose
   comparison failures are silently swallowed, and cardinality states where
   q/pull/entity disagree (entity crashes). Validation now runs on the
   RESULTING schema entries at the end of the transaction loop, uniformly
   over both paths and any datom order, with data-backed checks."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.index.interface :as di]))

(defn- fresh-conn
  ([] (fresh-conn {}))
  ([extra-cfg]
   (let [cfg (merge {:store {:backend :memory :id (random-uuid)}
                     :schema-flexibility :write
                     ;; most tests here exercise the backfill contract
                     :allow-index-backfill? true}
                    extra-cfg)]
     (d/create-database cfg)
     (d/connect cfg))))

(defn- schema-error? [f]
  (try (f) false
       (catch Exception e
         ;; the writer wraps the original ex-info; walk the cause chain and
         ;; fall back to the message (wrappers stringify the original)
         (boolean
          (or (loop [ex e]
                (cond (nil? ex) false
                      (= :transact/schema (:error (ex-data ex))) true
                      :else (recur (ex-cause ex))))
              (re-find #":transact/schema" (str (.getMessage e))))))))

(defn- avet-values [db attr]
  (->> (di/-seq (:avet db))
       (keep (fn [^datahike.datom.Datom datom]
               (when (= attr (.-a datom)) (.-v datom))))
       sort
       vec))

(deftest db-index-can-be-disabled-without-stranding-avet
  (let [conn (fresh-conn)
        _ (d/transact conn [{:db/ident :age
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}])
        _ (d/transact conn [{:db/id 1 :age 10} {:db/id 2 :age 20}])
        _ (d/transact conn [[:db/add :age :db/index true]])
        indexed-db (d/db conn)]
    (is (= [10 20] (avet-values indexed-db :age)))

    (d/transact conn [[:db/retract :age :db/index true]])
    (let [disabled-db (d/db conn)]
      (is (false? (boolean (get-in disabled-db [:schema :age :db/index]))))
      (is (empty? (avet-values disabled-db :age))
          "the disabling root contains no stale current AVET entries")
      (is (= [10 20] (avet-values indexed-db :age))
          "the immutable database value from before the transition is unchanged"))

    (d/transact conn [[:db/retract 1 :age 10]
                      [:db/add 1 :age 30]
                      [:db/add 3 :age 40]])
    (is (empty? (avet-values (d/db conn) :age))
        "later writes do not repopulate a disabled index")

    (d/transact conn [[:db/add :age :db/index true]])
    (is (= [20 30 40] (avet-values (d/db conn) :age))
        "re-enabling uses the existing backfill and sees every current value")))

(deftest disabling-explicit-index-preserves-intrinsic-avet-indices
  (let [conn (fresh-conn)
        _ (d/transact
           conn
           [{:db/ident :email
             :db/valueType :db.type/string
             :db/cardinality :db.cardinality/one
             :db/unique :db.unique/identity
             :db/index true}
            {:db/ident :friend
             :db/valueType :db.type/ref
             :db/cardinality :db.cardinality/one
             :db/index true}])
        _ (d/transact conn [{:db/id 1 :email "one@example.com" :friend 2}
                            {:db/id 2 :email "two@example.com"}])]
    (d/transact conn [[:db/retract :email :db/index true]
                      [:db/retract :friend :db/index true]])
    (let [db (d/db conn)]
      (is (= ["one@example.com" "two@example.com"]
             (avet-values db :email))
          "unique attributes remain indexed after their redundant flag is removed")
      (is (= [2] (avet-values db :friend))
          "reference attributes retain the AVET required by ref semantics"))))

(deftest raw-vector-unique-over-duplicates-rejected
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :email :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:email "x@y.z"} {:email "x@y.z"}])
    (testing "raw vector bypass is closed"
      (is (schema-error? #(d/transact conn [[:db/add :email :db/unique :db.unique/identity]]))
          "unique over existing duplicates must be rejected (was: accepted, broke upsert/lookup-refs)"))))

(deftest unique-add-requires-unused-attribute
  (testing "unique on an unused attribute is fine, and enforced from then on"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :email :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (is (d/transact conn [[:db/add :email :db/unique :db.unique/identity]]))
      (d/transact conn [{:email "a@y.z"}])
      ;; raw add — an entity map would UPSERT rather than violate uniqueness
      (is (thrown? Exception (d/transact conn [[:db/add 999 :email "a@y.z"]])))))
  (testing "unique on a USED attribute (no duplicates) is accepted and the
            index-backfill migration makes it enforceable against the
            PRE-EXISTING values: validate-datom consults AVET, which the
            backfill populated"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :email :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:email "a@y.z"} {:email "b@y.z"}])
      (is (d/transact conn [[:db/add :email :db/unique :db.unique/identity]]))
      ;; old value is now visible to uniqueness enforcement (raw add — an
      ;; entity map would UPSERT rather than violate)
      (is (thrown? Exception (d/transact conn [[:db/add 999 :email "a@y.z"]])))
      ;; and lookup refs resolve through the backfilled AVET
      (is (some? (d/entity (d/db conn) [:email "b@y.z"]))))))

(deftest raw-vector-valuetype-change-with-data-rejected
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :age :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:age "42"}])
    (is (schema-error? #(d/transact conn [[:db/add :age :db/valueType :db.type/long]]))
        "valueType change over existing data must be rejected (was: mixed-type index)")))

(deftest valuetype-change-on-unused-attribute-allowed
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :age :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (is (d/transact conn [[:db/add :age :db/valueType :db.type/long]])
        "no current or history datoms — retyping is safe")
    (is (d/transact conn [{:age 42}]))))

(deftest cardinality-narrowing-with-multi-values-rejected-on-both-paths
  (testing "raw vector"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :tags :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/many}])
      (d/transact conn [{:db/id 100 :tags [:a :b]}])
      (is (schema-error? #(d/transact conn [[:db/add :tags :db/cardinality :db.cardinality/one]]))
          "was: accepted on BOTH paths; q/pull/entity then disagreed and entity crashed")))
  (testing "entity map"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :tags :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/many}])
      (d/transact conn [{:db/id 100 :tags [:a :b]}])
      (is (schema-error? #(d/transact conn [{:db/id :tags :db/cardinality :db.cardinality/one}])))))
  (testing "narrowing is allowed once no entity holds multiple values"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :tags :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/many}])
      (d/transact conn [{:db/id 100 :tags [:a]}])
      (is (d/transact conn [[:db/add :tags :db/cardinality :db.cardinality/one]])))))

(deftest retracting-required-schema-key-rejected
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:name "x"}])
    (is (schema-error? #(d/transact conn [[:db/retract :name :db/valueType :db.type/string]]))
        "was: remove-schema left a live attribute with an incomplete entry")))

(deftest incremental-raw-vector-schema-in-one-tx-allowed
  (testing "order-independence: a complete entry assembled from raw vectors in one tx"
    (let [conn (fresh-conn)]
      (is (d/transact conn [[:db/add -1 :db/cardinality :db.cardinality/one]
                            [:db/add -1 :db/ident :score]
                            [:db/add -1 :db/valueType :db.type/long]]))
      (is (d/transact conn [{:score 1}])))))

(deftest benign-schema-changes-still-allowed
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:name "x"}])
    (testing "doc and noHistory changes on a live attribute"
      (is (d/transact conn [[:db/add :name :db/doc "a person's name"]]))
      (is (d/transact conn [[:db/add :name :db/noHistory true]])))
    (testing "enum idents and doc-only entities are not attribute entries"
      (is (d/transact conn [{:db/ident :color/red}]))
      (is (d/transact conn [{:db/ident :notes :db/doc "doc only"}])))))

(deftest tuple-attrs-must-reference-sound-attributes
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :a :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                      {:db/ident :many :db/valueType :db.type/long :db/cardinality :db.cardinality/many}])
    (testing "an UNDECLARED referenced attribute is supported (nil slot)"
      (is (d/transact conn [{:db/ident :t1 :db/valueType :db.type/tuple
                             :db/cardinality :db.cardinality/one
                             :db/tupleAttrs [:a :declared-later]}])))
    (testing "cardinality-many referenced attribute"
      (is (schema-error? #(d/transact conn [{:db/ident :t2 :db/valueType :db.type/tuple
                                             :db/cardinality :db.cardinality/one
                                             :db/tupleAttrs [:a :many]}]))))
    (testing "sound tuple definition still works"
      (is (d/transact conn [{:db/ident :b :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                            {:db/ident :t3 :db/valueType :db.type/tuple
                             :db/cardinality :db.cardinality/one
                             :db/tupleAttrs [:a :b]}])))))

(deftest backfill-is-opt-in
  (testing "without :allow-index-backfill? the historical rejection holds:
            enabling :db/index or :db/unique on a USED attribute is refused;
            an unused attribute is accepted as before"
    (let [conn (fresh-conn {:allow-index-backfill? false})]
      (d/transact conn [{:db/ident :email :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :spare :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/id 1 :email "a@y.z"}])
      (is (schema-error? #(d/transact conn [[:db/add :email :db/index true]])))
      (is (schema-error? #(d/transact conn [[:db/add :email :db/unique :db.unique/identity]])))
      ;; unused attribute: no migration needed, accepted without the flag
      (is (d/transact conn [[:db/add :spare :db/unique :db.unique/identity]])))))
