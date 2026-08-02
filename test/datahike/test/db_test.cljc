(ns datahike.test.db-test
  (:require
   [clojure.data]
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.constants :as const]
   [datahike.test.core-test]
   [datahike.db :as db #?@(:cljs [:refer-macros [defrecord-updatable]]
                           :clj  [:refer [defrecord-updatable]])]))

#?(:cljs (def Throwable js/Error))

;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  #?@(:cljs [IHash                (-hash  [hb] 0xBEEF)]
      :clj  [clojure.lang.IHashEq (hasheq [hb] 0xBEEF)]))

(deftest test-defrecord-updatable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))

(deftest test-fn-hash-changes
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Konrad"}])
        r1 (d/db-with db [[:db.fn/retractEntity 1]])
        r2 (d/db-with db [[:db.fn/retractEntity 1]])]
    (is (= (hash r1) (hash r2)))))

(deftest test-equiv-db-hash
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Konrad"}])
        r1 (d/db-with db [[:db.fn/retractEntity 1]])]
    (is (= (hash (db/empty-db)) (hash r1)))))

(deftest empty-db-with-schema
  (testing "Test old write schema"
    (is (thrown-with-msg? Throwable
                          #"Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
                          (db/empty-db {:name {:db/cardinality :db.cardinality/many}} {:schema-flexibility :write})))
    (is (= (merge const/non-ref-implicit-schema
                  {:name {:db/cardinality :db.cardinality/one :db/valueType :db.type/string}})
           (:schema (db/empty-db {:name {:db/cardinality :db.cardinality/one
                                         :db/valueType :db.type/string}}
                                 {:schema-flexibility :write}))))

    (is (= (merge const/non-ref-implicit-schema
                  {:name {:db/ident :name :db/cardinality :db.cardinality/one :db/valueType :db.type/string}})
           (:schema (db/empty-db [{:db/ident :name
                                   :db/cardinality :db.cardinality/one
                                   :db/valueType :db.type/string}]
                                 {:schema-flexibility :write}))))))

;; ---------------------------------------------------------------------------
;; Object.equals / hashCode contract
;;
;; DB and the DB views (FilteredDB / HistoricalDB / AsOfDB / SinceDB)
;; override `seq` to yield Datoms rather than map entries. Any Object
;; method left to defrecord's generated implementation therefore walked
;; that seq casting each element to Map.Entry and raised a
;; ClassCastException from inside APersistentMap — so `.equals` threw
;; instead of answering false, and the views could not be hashed at all.
;; Every java.util collection holding a db (HashMap, HashSet,
;; Objects.equals, List.contains) was affected.

#?(:clj
   (deftest test-db-equals-contract
     (let [db1 (d/db-with (db/empty-db) [{:db/id 1 :name "Konrad"}])
           db2 (d/db-with (db/empty-db) [{:db/id 1 :name "Chrislain"}])]
       (testing "equals answers false rather than throwing"
         (is (false? (.equals db1 db2)))
         (is (true? (.equals db1 db1)))
         (is (false? (.equals db1 42)))
         (is (false? (.equals db1 nil)))
         (is (false? (java.util.Objects/equals db1 db2))))

       (testing "equals agrees with ="
         (is (= (.equals db1 db2) (= db1 db2)))
         (is (= (.equals db1 db1) (= db1 db1))))

       (testing "dbs are usable as java.util collection members"
         (let [s (java.util.HashSet.)]
           (.add s db1)
           (.add s db2)
           (is (= 2 (.size s)))
           (is (.contains s db1)))))))

#?(:clj
   (deftest test-db-view-equals-contract
     ;; history/as-of/since need a temporal db; filter does not, but one
     ;; db keeps the four views comparable.
     (let [db  (d/db-with (db/empty-db nil {:keep-history? true})
                          [{:db/id 1 :name "Konrad"}])
           f1  (d/filter db (fn [_ _] true))
           f2  (d/filter db (fn [_ _] true))
           views {"FilteredDB"   f1
                  "HistoricalDB" (d/history db)
                  "AsOfDB"       (d/as-of db (java.util.Date.))
                  "SinceDB"      (d/since db (java.util.Date. 0))}]
       (testing "views can be hashed and are reflexive"
         (doseq [[label v] views]
           (is (integer? (.hashCode ^Object v)) (str label " .hashCode"))
           (is (integer? (hash v))              (str label " hash"))
           (is (true? (.equals ^Object v v))    (str label " reflexive .equals"))
           (is (= v v)                          (str label " reflexive ="))
           (let [s (java.util.HashSet.)]
             (.add s v)
             (is (.contains s v) (str label " HashSet round trip")))))

       (testing "a pass-through filter equals its db, symmetrically"
         (is (= db f1))
         (is (= f1 db))
         (is (= (.equals f1 db) (.equals ^Object db f1))))

       (testing "equal values hash alike — required by the equals contract"
         (is (= (hash db) (hash f1)))
         (is (= f1 f2))
         (is (= (hash f1) (hash f2)))))))
