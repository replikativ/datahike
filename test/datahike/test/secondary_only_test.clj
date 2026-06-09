(ns datahike.test.secondary-only-test
  "`:db.value`-style external storage via `:db.secondary/only`: the value lives
   only in the covering secondary index; the primary EAVT/AEVT/AVET hold a hasch
   content hash, keeping them small for large/unbounded payloads."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.index.secondary :as sec]
            [datahike.index.entity-set :as es]
            datahike.index.secondary.scriptum))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- with-secondary [conn attrs]
  (d/transact conn [{:db/ident :idx/ft :db.secondary/type :scriptum
                     :db.secondary/attrs attrs
                     :db.secondary/config {:path (str "/tmp/dh-so-test-" (random-uuid))}}])
  (Thread/sleep 400))

(deftest secondary-only-stores-hash-in-primary
  (testing "a :db.secondary/only value is replaced by a content hash in the primary, full value goes to the secondary"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :note/title   :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                        {:db/ident :note/content :db/valueType :db.type/string :db/cardinality :db.cardinality/one
                         :db.secondary/only true}])
      (with-secondary conn [:note/content])
      (let [big (apply str (repeat 5000 "lorem ipsum datalog databases "))] ; ~150 kB
        (d/transact conn [{:db/id -1 :note/title "n1" :note/content big}])
        (Thread/sleep 500)
        (let [stored (d/q '[:find ?v . :where [?e :note/content ?v]] @conn)]
          ;; primary holds only the 36-char hash, NOT the 150 kB value
          (is (= 36 (count stored)) "primary stores a fixed-size hash")
          (is (not= big stored)     "primary does not store the full value")
          (is (uuid? (java.util.UUID/fromString stored)) "the hash is a hasch uuid string"))
        ;; the full value is in the secondary — fulltext-searchable
        (let [ft (get-in @conn [:secondary-indices :idx/ft])]
          (is (seq (es/entity-bitset-seq (sec/-search ft {:query "datalog" :field :value} nil)))
              "value is searchable via the secondary")
          (is (empty? (es/entity-bitset-seq (sec/-search ft {:query "zzznotpresent" :field :value} nil)))
              "search is real, not always-match"))
        ;; a normal attribute is unaffected
        (is (= "n1" (d/q '[:find ?v . :where [?e :note/title ?v]] @conn)))))))

(deftest secondary-only-dedups-and-retracts
  (testing "identical values dedup by hash; retraction re-hashes to find and remove the datom"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :doc/body :db/valueType :db.type/string :db/cardinality :db.cardinality/one
                         :db.secondary/only true}])
      (with-secondary conn [:doc/body])
      (d/transact conn [{:db/id -1 :doc/body "same text"}
                        {:db/id -2 :doc/body "same text"}])
      (Thread/sleep 300)
      (let [pairs (d/q '[:find ?e ?v :where [?e :doc/body ?v]] @conn)]
        (is (= 2 (count pairs)) "two entities each carry the hash")
        (is (= 1 (count (set (map second pairs)))) "identical content → identical hash (dedup-friendly)"))
      ;; retract by value: re-hashes internally to find [e a hash]
      (let [eid (d/q '[:find ?e . :where [?e :doc/body _]] @conn)]
        (d/transact conn [[:db/retract eid :doc/body "same text"]])
        (Thread/sleep 100)
        (is (nil? (d/q '[:find ?v . :in $ ?e :where [?e :doc/body ?v]] @conn eid))
            "retraction by value removed the datom")))))

(deftest secondary-family-has-stable-ids-in-attribute-refs
  (testing "the :db.secondary/* family is in the system schema, so attribute-refs DBs assign it fixed entity IDs and a secondary-only attr works in that mode"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write :attribute-refs? true}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)
            irm  (:ident-ref-map @conn)]
        ;; fixed ids from datahike.constants/system-schema (41..46)
        (is (= 41 (get irm :db.secondary/type)))
        (is (= 46 (get irm :db.secondary/only)))
        ;; end-to-end: secondary-only write + search in attribute-refs mode
        (d/transact conn [{:db/ident :ar/body :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db.secondary/only true}])
        (with-secondary conn [:ar/body])
        (d/transact conn [{:db/id -1 :ar/body "attribute refs and datalog"}])
        (Thread/sleep 400)
        (is (= 36 (count (d/q '[:find ?v . :where [?e :ar/body ?v]] @conn)))
            "primary holds the hash in attribute-refs mode too")
        (let [ft (get-in @conn [:secondary-indices :idx/ft])]
          (is (seq (es/entity-bitset-seq (sec/-search ft {:query "datalog" :field :value} nil)))))))))

(deftest secondary-only-requires-a-covering-secondary
  (testing "writing a :db.secondary/only value with no covering secondary raises (value would be lost)"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :x/c :db/valueType :db.type/string :db/cardinality :db.cardinality/one
                         :db.secondary/only true}])
      (is (thrown? Exception (d/transact conn [{:db/id -1 :x/c "hi"}]))))))
