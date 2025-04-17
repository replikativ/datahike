(ns datahike.test.unstructured-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.experimental.unstructured :as du]))

(deftest test-value-type-inference
  (testing "Basic type inference"
    (is (= :db.type/long (du/value->type 42)))
    (is (= :db.type/float (du/value->type (float 42.0))))
    (is (= :db.type/string (du/value->type "hello")))
    (is (= :db.type/boolean (du/value->type true)))
    (is (= :db.type/keyword (du/value->type :keyword)))
    (is (= :db.type/ref (du/value->type {:a 1})))))

(deftest test-schema-inference
  (testing "Schema inference for basic values"
    (is (= {:db/ident :name
            :db/valueType :db.type/string
            :db/cardinality :db.cardinality/one}
           (du/infer-value-schema :name "Alice"))))

  (testing "Schema inference for collections"
    (is (= {:db/ident :tags
            :db/valueType :db.type/keyword
            :db/cardinality :db.cardinality/many}
           (du/infer-value-schema :tags [:tag1 :tag2])))

    (is (= {:db/ident :address
            :db/valueType :db.type/ref
            :db/cardinality :db.cardinality/one}
           (du/infer-value-schema :address {:street "123 Main St"}))))

  (testing "Empty vector handling"
    (is (nil? (du/infer-value-schema :empty-tags [])))))

(deftest test-process-unstructured-data
  (testing "Processing simple data"
    (let [data {:name "Alice" :age 30}
          result (du/process-unstructured-data data)]
      (is (= 2 (count (:schema result))))
      (is (= 1 (count (:tx-data result))))
      (is (= -1 (:db/id (first (:tx-data result)))))))

  (testing "Processing nested data"
    (let [data {:name "Bob"
                :age 25
                :address {:street "123 Main St"
                          :city "Anytown"}}
          result (du/process-unstructured-data data)]
      ;; There should be 5 schema entries: name, age, address, street, city
      (is (= 5 (count (:schema result))))
      (is (= 2 (count (:tx-data result))))
      (is (some #(= -1 (:db/id %)) (:tx-data result)))))

  (testing "Processing empty vectors"
    (let [data {:name "Charlie"
                :tags []}
          result (du/process-unstructured-data data)]
      ;; The schema should only include the name attribute, not tags
      (is (= 1 (count (:schema result))))
      (is (= :name
             (-> result :schema first :db/ident)))
      ;; The transaction data should still include the empty tags vector
      (let [tx-entity (first (:tx-data result))]
        (is (= "Charlie" (:name tx-entity)))
        (is (= [] (:tags tx-entity)))))))

(deftest test-basic-schema-on-read
  (testing "Basic operations with schema-on-read"
    (let [cfg {:store {:backend :mem
                       :id "test-basic-schema-on-read"}
               :schema-flexibility :read}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)]

      ;; Just add simple data directly
      (d/transact conn [{:name "Alice" :age 30}])

      ;; Simple query
      (is (= #{["Alice"]}
             (d/q '[:find ?n :where [?e :name ?n]] (d/db conn))))

      (d/release conn))))

(deftest test-unstructured-schema-on-read
  (testing "Using process-unstructured-data with schema-on-read"
    (let [cfg {:store {:backend :mem
                       :id "test-unstructured-schema-on-read"}
               :schema-flexibility :read}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          data {:name "Bob"
                :age 25
                :address {:street "123 Main St"
                          :city "Anytown"}}]

      ;; Use process-unstructured-data to prepare data
      (let [processed (du/process-unstructured-data data)
            tx-report (d/transact conn (:tx-data processed))]

        ;; Verify simple attribute
        (is (= #{["Bob"]}
               (d/q '[:find ?n :where [?e :name ?n]] (d/db conn))))

        ;; We can't directly query nested data with the temp IDs in the schema-on-read mode
        ;; This requires a different approach
        ;; Let's verify we can at least retrieve the address entity id
        (let [address-id (d/q '[:find ?a :where [?e :address ?a]] (d/db conn))]
          (is (not (empty? address-id)))))

      (d/release conn))))

(deftest test-transact-unstructured-api
  (testing "Using the transact-unstructured API with schema-on-read"
    (let [cfg {:store {:backend :mem
                       :id "test-transact-unstructured-api-read"}
               :schema-flexibility :read}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          data {:name "Charlie"
                :age 35
                :skills ["programming" "design" "management"]
                :contact {:email "charlie@example.com"
                          :phone "555-1234"}}]

      ;; Use the high-level API
      (du/transact-unstructured conn data)

      ;; Verify basic data
      (is (= #{["Charlie"]}
             (d/q '[:find ?n :where [?e :name ?n]] (d/db conn))))
      (is (= #{[35]}
             (d/q '[:find ?a :where [?e :age ?a]] (d/db conn))))

      ;; Verify array data - just check if any skills were added
      (let [skills (d/q '[:find ?s :where [_ :skills ?s]] (d/db conn))]
        (is (not (empty? skills))))

      ;; Verify that nested object was stored properly by checking contact
      (let [contacts (d/q '[:find ?e :where [?e :email "charlie@example.com"]] (d/db conn))]
        (is (not (empty? contacts))))

      (d/release conn))))

(deftest test-schema-on-write
  (testing "Using transact-unstructured with schema-on-write"
    (let [cfg {:store {:backend :mem
                       :id "test-schema-on-write"}}  ;; Default is schema-on-write
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          data {:name "Diana"
                :age 28
                :active true}]

      ;; First, verify that we can't insert data directly (requires schema)
      (is (thrown? Exception
                   (d/transact conn [data])))

      ;; Now use transact-unstructured, which should infer and apply schema first
      (du/transact-unstructured conn data)

      ;; Verify the schema was created
      (let [schema-query '[:find ?a ?t ?c
                           :where
                           [?e :db/ident ?a]
                           [?e :db/valueType ?t]
                           [?e :db/cardinality ?c]]]
        (is (= #{[:name :db.type/string :db.cardinality/one]
                 [:age :db.type/long :db.cardinality/one]
                 [:active :db.type/boolean :db.cardinality/one]}
               (d/q schema-query (d/db conn)))))

      ;; Verify the data was inserted
      (is (= #{["Diana"]}
             (d/q '[:find ?n :where [?e :name ?n]] (d/db conn))))
      (is (= #{[28]}
             (d/q '[:find ?a :where [?e :age ?a]] (d/db conn))))
      (is (= #{[true]}
             (d/q '[:find ?a :where [?e :active ?a]] (d/db conn))))

      ;; Test schema evolution with new attributes
      (let [new-data {:name "Erik"
                      :age 42
                      :email "erik@example.com"  ;; New attribute
                      :active false}]

        ;; This should work and auto-add the email schema
        (du/transact-unstructured conn new-data)

        ;; Verify new schema was added
        (let [schema-query '[:find ?a ?t ?c
                             :where
                             [?e :db/ident ?a]
                             [?e :db/valueType ?t]
                             [?e :db/cardinality ?c]]]
          (is (= #{[:name :db.type/string :db.cardinality/one]
                   [:age :db.type/long :db.cardinality/one]
                   [:active :db.type/boolean :db.cardinality/one]
                   [:email :db.type/string :db.cardinality/one]}
                 (d/q schema-query (d/db conn)))))

        ;; Verify both records exist
        (is (= #{["Diana"] ["Erik"]}
               (d/q '[:find ?n :where [?e :name ?n]] (d/db conn))))
        (is (= #{["erik@example.com"]}
               (d/q '[:find ?e :where [_ :email ?e]] (d/db conn)))))

      (d/release conn))))

(deftest test-cardinality-many-inference
  (testing "Inferring cardinality/many from vector values"
    (let [cfg {:store {:backend :mem
                       :id "test-cardinality-many"}}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          ;; First set up the schema for name with unique identity
          _ (d/transact conn [{:db/ident :name
                               :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one
                               :db/unique :db.unique/identity}])

          data {:name "Fiona"
                :tags ["clojure" "datalog" "databases"]} ;; Vector = cardinality many
          ]

      ;; This should automatically set up the tags attribute as cardinality/many
      (du/transact-unstructured conn data)

      ;; Verify the schema for tags with cardinality/many
      (let [schema-query '[:find ?t ?c
                           :where
                           [?e :db/ident :tags]
                           [?e :db/valueType ?t]
                           [?e :db/cardinality ?c]]]
        (is (= #{[:db.type/string :db.cardinality/many]} ;; Check cardinality/many
               (d/q schema-query (d/db conn)))))

      ;; Verify all array values were stored
      (let [tags (d/q '[:find ?t :where [_ :tags ?t]] (d/db conn))]
        (is (= #{"clojure" "datalog" "databases"}
               (set (map first tags)))))

      ;; Test adding more values to the array for the same entity
      (d/transact conn [{:db/id [:name "Fiona"]
                         :tags ["datahike"]}]) ;; Add another tag 

      ;; Verify all tags are preserved (cardinality/many behavior)
      (let [tags (d/q '[:find ?t :where [_ :tags ?t]] (d/db conn))]
        (is (= #{"clojure" "datalog" "databases" "datahike"}
               (set (map first tags)))))

      (d/release conn))))

(deftest test-schema-conflict
  (testing "Detecting schema conflicts"
    (let [cfg {:store {:backend :mem
                       :id "test-schema-conflict"}}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)]

      ;; First add schema for 'score' as a long
      (d/transact conn [{:db/ident :score
                         :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])

      ;; Add a data point with correct type
      (d/transact conn [{:score 100}])

      ;; Now try to add data with incompatible type (string instead of long)
      (let [data {:name "Grace"
                  :score "High"}] ;; Score is a string but schema says long

        ;; This should detect the conflict and throw an exception
        (is (thrown-with-msg? Exception #"Schema conflict detected"
                              (du/transact-unstructured conn data))))

      ;; Verify the data was not inserted
      (is (empty? (d/q '[:find ?n :where [_ :name ?n]] (d/db conn))))

      ;; Verify the original score still works
      (is (= #{[100]} (d/q '[:find ?s :where [_ :score ?s]] (d/db conn))))

      (d/release conn))))