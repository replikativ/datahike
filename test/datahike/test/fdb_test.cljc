(ns datahike.test.fdb-test
  (:import (com.apple.foundationdb KeySelector
             FDB))
  (:require [datahike.core :as d]
            [datahike.db :as dh-db]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fdb.core :as fdb]
            [fdb.keys :as k]
            [datahike.datom :refer [datom]]))

;;---- FDB Keys -----

(defn assert-vec-conversion
  "Checks that given a vec we get the same vec back after going through fdb key conversion"
  [index-type from-vect to-vect]
  (let [buff       (k/->byteBuffer index-type from-vect)
        buff->vect (k/byteBuffer->vect index-type buff)]
    (is (= to-vect buff->vect))))

(deftest eavt
  (let [vect [20 :hello "some analysis" 3]]
    "->byteArr and back"
    (is (= (k/key->vect :eavt (k/->byteArr :eavt vect)) vect))

    "basic vector conversion and inserting a string value"
    (assert-vec-conversion :eavt [20 :hello "some analysis" 3] [20 :hello "some analysis" 3])

    "int value"
    (assert-vec-conversion :eavt [20 :hello (int 2356) 3] [20 :hello (int 2356) 3])

    "boolean value"
    (assert-vec-conversion :eavt [20 :hello true 3] [20 :hello true 3])
    (assert-vec-conversion :eavt [20 :hello false 3] [20 :hello false 3])

    "long value"
    (assert-vec-conversion :eavt [20 :hello (long 234) 3] [20 :hello (long 234) 3])

    "keyword value"
    (assert-vec-conversion :eavt [20 :hello :ns/a_keyword 3] [20 :hello :ns/a_keyword 3])

    "biggest 'e' value"
    (assert-vec-conversion :eavt [9223372036854775807 :hello (long 234) 3]
      [9223372036854775807 :hello (long 234) 3])))


;; --------- :aevt indices
(deftest aevt
  "simple insert and retrieval"
  (let [vect [20 :hello "some data" 3]]
    (is (= [:hello 20 "some data" 3] (k/key->vect :aevt (k/->byteArr :aevt vect)))))

  "basic vector conversion"
  (assert-vec-conversion :aevt [20 :hello  "some analysis" 3] [:hello 20 "some analysis" 3])

  "int value"
  (assert-vec-conversion :aevt [20 :hello  (int 2356) 3] [:hello 20 (int 2356) 3])

  "boolean value"
  (assert-vec-conversion :aevt [20 :hello true 3] [:hello 20 true 3])

  "biggest 'e' value"
  (assert-vec-conversion :aevt [9223372036854775807 :hello (long 234) 3]
    [:hello 9223372036854775807 (long 234) 3]))



;; --------- :avet indices
(deftest avet
  "simple insert and retrieval"
  (let [vect [20 :hello "some values" 3]]
    (is (= [:hello "some values" 20 3] (k/key->vect :avet (k/->byteArr :avet vect)))))

  "basic vector conversion"
  (assert-vec-conversion :avet [20 :hello "some analysis" 3] [:hello "some analysis" 20 3])

  "int value"
  (assert-vec-conversion :avet [20 :hello (int 2356) 3] [:hello (int 2356) 20 3])

  "boolean value"
  (assert-vec-conversion :avet [20 :hello false 3] [:hello false 20 3])

  "biggest 'e' value"
  (assert-vec-conversion :avet [9223372036854775807 :hello (long 234) 3] [:hello (long 234) 9223372036854775807 3]))



(deftest illegal-argument
  "wrong index descriptor"
  (is (thrown? AssertionError (k/->byteBuffer :vrt [:hello 9223372036854775807 (long 234) 3]))))




;;----- FDB Dathike integration -----
;;

(defn empty-db []
  (dh-db/empty-db nil :datahike.index/fdb))


(deftest empty-db-creation
  "empty db creation"
  (is (empty-db)))


;; Asserts strings order are preserved
(deftest string-order
  (testing ""
    (let [db (-> (empty-db)
               (d/db-with [[:db/add 1 :name "Petr"]]))]
      (is (not (empty? (fdb/get-range :eavt [0 :name nil 536870912] [1 :name "Q" 2147483647]))))
      (is (not (empty? (fdb/get-range :eavt [0 :name nil 536870912] [1 :name "Pf" 2147483647]))))
      (is (not (empty? (fdb/get-range :eavt [0 :name nil 536870912] [1 :name "Petr" 2147483647])))))))


(deftest nil-meaning
  (let [db (-> (empty-db)
             (d/db-with [ [:db/add 1 :name "Petr"]]))]
    (is (not (empty? (fdb/get-range :eavt [0 :name nil 536870912] [1 :name nil 2147483647]))))))


(deftest datom-fn
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (d/db-with (empty-db) [(datom 123 :likes "Hans" 1 true)
                                  (datom 124 :likes "GG" 1 true)])]
    (testing "datoms works"
      (is (= [[123 :likes "Hans"]
              [124 :likes "GG"]]
            (map dvec (d/datoms db :eavt)))))))


(deftest min-vals
  (let [[e _ _ t] (k/byteBuffer->vect :eavt
                    (k/->byteBuffer :eavt [:dh-fdb/min-val :dh-fdb/min-val
                                           :dh-fdb/min-val :dh-fdb/min-val]))]
    (is (= [e t] [0 0]))))

(deftest max-vals
  (let [[e _ _ t] (k/byteBuffer->vect :eavt
                    (k/->byteBuffer :eavt [:dh-fdb/max-val :a
                                           :dh-fdb/max-val :dh-fdb/max-val]))]
    (is (= [e t] [9187201402835828740 9187201402835828740]))))


(deftest simple-insertions
  (testing "using db-with"
    (let [db (-> (empty-db)
               (d/db-with [[:db/add 1 :name "Petr"]
                           [:db/add 1 :ko "Ivan"]]))]
      (is (= 1
            (count (fdb/get-range :aevt [1 :name "Petr"] [20 :name "Petr"]))))))

  (let [dvec #(vector (:e %) (:a %) (:v %))
        data [[:db/add 1 :name "Petr"]
              [:db/add 1 :age 44]
              [:db/add 2 :name "Ivan"]
              [:db/add 2 :age 25]
              [:db/add 3 :name "Sergey"]
              [:db/add 3 :age 11]]
        db (-> (empty-db)
             (d/db-with data))]

    (testing ":eavt"
      (is (= [[1 :age 44]
              [1 :name "Petr"]
              [2 :age 25]
              [2 :name "Ivan"]
              [3 :age 11]
              [3 :name "Sergey"]]
            (mapv dvec (d/datoms db :eavt))))
      (is (= (count data)
            (count (fdb/get-range :eavt
                     [:dh-fdb/min-val :dh-fdb/min-val :dh-fdb/min-val :dh-fdb/min-val]
                     [:dh-fdb/max-val :dh-fdb/max-val :dh-fdb/max-val :dh-fdb/max-val])))))

    (testing ":aevt"
      (is (= [[1 :age 44]
              [2 :age 25]
              [3 :age 11]
              [1 :name "Petr"]
              [2 :name "Ivan"]
              [3 :name "Sergey"]]
            (map dvec (d/datoms db :aevt))))
      (is (= (count data)
            (count (fdb/get-range :eavt
                     [:dh-fdb/min-val :dh-fdb/min-val :dh-fdb/min-val :dh-fdb/min-val]
                     [:dh-fdb/max-val :dh-fdb/max-val :dh-fdb/max-val :dh-fdb/max-val])))))))


(deftest db-with
  (testing  "simple get-range"
    (let [db (-> (empty-db)
               (d/db-with [[:db/add 1 :name "Ivan"]
                           [:db/add 1 :name "Petr"]]))]
      (is (= 1
            (count (fdb/get-range :aevt [1 :name "Petr"] [20 :name "Petr"]))))))

  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (empty-db)
             (d/db-with [[:db/add 1 :name "Petr"]
                         [:db/add 1 :age 44]
                         [:db/add 2 :name "Ivan"]
                         [:db/add 2 :age 25]
                         [:db/add 3 :name "Sergey"]
                         [:db/add 3 :age 11]
                         ]))]
    (testing "datoms in :eavt order"
      (is (= [[1 :age 44]
              [1 :name "Petr"]
              [2 :age 25]
              [2 :name "Ivan"]
              [3 :age 11]
              [3 :name "Sergey"]]
            (mapv dvec (d/datoms db :eavt)))))

    (testing "datoms in :aevt order"
      (is (= [[1 :age 44]
              [2 :age 25]
              [3 :age 11]
              [1 :name "Petr"]
              [2 :name "Ivan"]
              [3 :name "Sergey"]]
            (map dvec (d/datoms db :aevt)))))))


(deftest clear
  (let [db (-> (empty-db)
               (d/db-with [[:db/add 1 :name "Ivan"]]))]
      (is (= 1 (count (fdb/get-range :eavt [1 :name "Ivan"] [20 :name "Ivan"]))))
      (is (= 0 (count (fdb/clear :eavt [1 :name "Ivan"]))))))


(deftest test-transact!
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many }})]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka  "Devil"]])
    (d/transact! conn [[:db/add 1 :aka  "Tupen"]])

    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
          #{["Petr"]}))
    (is (= (d/q '[:find ?v
                    :where [1 :aka ?v]] @conn)
            #{["Devil"] ["Tupen"]}))))


(deftest db-with
  (testing "get :e-end"
    (let [db                          (empty-db)
          {:keys [eavt eavt-durable]} (d/db-with db [(datom 123 :likes "Hans" 1 true)
                                                     (datom 124 :likes "GG" 1 true)])]
      ;; get :e-end
      (is (== (nth (fdb/get (:eavt-scalable db) :eavt [123 :likes "Hans" 1 true])
                (k/position :eavt :e-end))
            123))
      (is (== (nth (fdb/get (:eavt-scalable db) :eavt [124 :likes "GG" 1 true])
                (k/position :eavt :e-end))
            124))))

  (testing "simple range :eavt"
    (let [db (empty-db)
          _  (d/db-with db [(datom 123 :likes "Hans" 1 true)
                            (datom 124 :likes "GG" 1 true)
                            (datom 125 :likes "GG" 1 true)
                            (datom 1 :likes "GG" 1 true)
                            (datom 2 :likes "GG" 1 true)
                            (datom 3 :likes "GG" 1 true)
                            (datom 3 :likes "HH" 1 true)])]
      ;; :eavt
      (is (= 3
            (count (fdb/get-range :eavt
                     [123 :likes "Hans" 1 true]
                     [125 :likes "GG" 1 true]))))
      (is (= 3
            (count (fdb/get-range :eavt [123] [125]))))
      (is (= 2
            (count (fdb/get-range :eavt [3] [3]))))
      (is (= 1
            (count (fdb/get-range :eavt
                     [3 :likes "HH" 1 true]
                     [3 :likes "HH" 1 true]))))))

  (testing "simple range :aevt"
      (let [db (empty-db)
            _  (d/db-with db [(datom 123 :a "Hans" 1 true)
                              (datom 124 :b "GG" 1 true)
                              (datom 125 :c "GG" 1 true)
                              (datom 1 :d "GG" 1 true)
                              (datom 2 :e "GG" 1 true)
                              (datom 3 :f "GG" 1 true)
                              (datom 4 :f "GG" 1 true)])]
        ;; :aevt
        (is (= 3
              (count (fdb/get-range :aevt
                       [:a 123 "Hans" 1 true]
                       [:c 125 "GG" 1 true]))))
        (is (= 3
              (count (fdb/get-range :aevt
                       [:a 123 "Hans" 1 true]
                       [:c 9999999 "GG" 1 true]))))
        (is (= 2
              (count (fdb/get-range :aevt
                       [:a 123 "Hans" 1 true]
                       [:c 0 "GG" 1 true]))))
        (is (= 3
              (count (fdb/get-range :aevt [:e] [:g]))))
        (is (= 0
              (count (fdb/get-range :aevt [:f] [:f]))))))


  (testing "simple range :avet"
      (let [db (empty-db)
            _  (d/db-with db [(datom 123 :a "Hans" 1 true)
                              (datom 124 :b "GG" 1 true)
                              (datom 125 :c "GG" 1 true)
                              (datom 1 :d "GG" 1 true)
                              (datom 2 :e "GG" 1 true)
                              (datom 3 :f "GG" 1 true)
                              (datom 4 :f "GG" 1 true)])]
        ;; :aevt
        (is (= 3
              (count (fdb/get-range :avet
                       [:a "Hans" 123  1 true]
                       [:c "GG" 125  1 true]))))
        (is (= 3
              (count (fdb/get-range :avet
                       [:a "Hans" 123 1 true]
                       [:c "GG" 9999999 1 true]))))
        (is (= 2
              (count (fdb/get-range :avet
                       [:a "Hans" 123 1 true]
                       [:c "GG" 0  1 true]))))
        (is (= 3
              (count (fdb/get-range :avet [:e] [:g]))))
        (is (= 0
              (count (fdb/get-range :avet [:f] [:f]))))))



  "large range"
  (let [db (empty-db)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 1 true)) db (range 100))]
    (is (= 51
          (count (fdb/get-range :eavt [1 :likes "Hans" 1 true]
                   [51 :likes "Hans" 1 true])))))

  "large range"
  (let [db (empty-db)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 1 true)) db (range 10))]
    (is (= 3
          (count (fdb/get-range :eavt [1 :likes "Hans" 1 true]
                   [3 :likes "Hans" 1 true]))))))



;; ------------------------ PERFOMANCE Testing -------------------------


;; FoundationDB write 1 million keys with 10 parallel clients and 100k keys per client

#_(let [fd (select-api-version 510)
        kv (map #(vector (str %1) %1) (range 100000))]
    (time (let [clients (repeatedly 10 #(future
                                          (with-open [db (open fd)]
                                            (tr! db
                                                 (doall (doseq [[k v] kv]
                                                          (set-val tr k v)))))))]
            (doall (map deref clients))
            "Finished")))

;; "Elapsed time: 27903.477365 msecs"


(comment
  (with-open [db (.open fd)]
    (.run
      db
      (clojure.core/reify
        java.util.function.Function
        (apply [this tr]
          (.set tr (fdb.keys/key index-type [1 1 "a" 1]) (fdb.core/key index-type [1 1 "a" 1])))))
    db))


;; To test how quick inserting 100k datoms is
(comment
  (let [v  (byte-array [])
        fd (FDB/selectAPIVersion 510)
        all_kv (map #(vector (fdb.keys/key index-type [%1 (str ":attribute/" %1) %1 %1])  v)
                (range 100000))]
    (time (with-open [db (.open fd)]
            ;; with fdb key size of 500 bytes
            (doall (doseq [kv_200 (partition 5000 all_kv)]
                     ;;(println "a")
                     (fdb/tr! db (doseq [[k v] kv_200]
                                   (.set tr k v)))))))))

;; * With 100k datoms to store
;; - and with fdb key size of 500 bytes and 5000 datoms per transaction: "Elapsed time: 14517.359219 msecs"
;; storing 10000 datoms per transaction exceeds FDB limits
;; - and with fdb key size of 100 bytes and 20000 datoms per transaction: "Elapsed time: 13781.928804 msecs"



;; Tests init-db for 100k
(comment
  (let [data (map #(vec [% % %]) (range 100000))]
    (time (-> (map #(apply dh-db/datom %) data)
              (dh-db/init-db)))))
;; => nil
