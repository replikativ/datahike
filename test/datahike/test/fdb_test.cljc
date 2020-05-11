(ns datahike.test.fdb-test
  (:import (com.apple.foundationdb KeySelector
             FDB))
  (:require [datahike.db :as dh-db :refer [with-datom]]
            [datahike.core :as d]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fdb.core :as fdb]
            [fdb.keys :as k]
            ;;[hitchhiker.tree.messaging :as hmsg]
            [datahike.datom :refer [datom]]))

;;---- FDB Keys -----


(defn assert-vec-conversion
  "Checks that given a 'vect we get the same vect back after going through fdb"
  [index-type from-vect to-vect]
  (let [buff       (k/->byteBuffer index-type from-vect)
        buff->vect (k/byteBuffer->vect index-type buff)
        ]
    (is (= to-vect buff->vect))))

(deftest eavt
  (let [vect [20 :hello "some analysis" 3]]
    "->byteArr and back"
    (is (= (k/key->vect :eavt (k/->byteArr :eavt vect)) vect))

    "basic vector conversion and inserting a string value"
    (assert-vec-conversion :eavt [20 :hello "some analysis" 3] [20 :hello "some analysis" 3])

    "int value"
    (assert-vec-conversion :eavt [20 :hello (int 2356) 3] [20 :hello (int 2356) 3])

    "long value"
    (assert-vec-conversion :eavt [20 :hello (long 234) 3] [20 :hello (long 234) 3])

    "biggest 'e' value"
    (assert-vec-conversion :eavt [9223372036854775807 :hello (long 234) 3]
      [9223372036854775807 :hello (long 234) 3]))

  ;; TODO: what happens when we try to insert something like :eavt [12 nil nil]
  ;; TODO: this raises the question of 'can we pass a vector with only subelements in get range when we do a search? e.g. get-range [125]? Shall we convert that automatically to [125 nil nil]?
  ;; Test as shown that you cannot insert a nil value in datahike: @(transact conn [{ :db/id 5, :name nil  }])

  )


;; --------- :aevt indices
(deftest aevt
  "simple insert and retrieval"
  (let [vect [20 :hello "some data" 3]]
    (is (= [:hello 20 "some data" 3] (k/key->vect :aevt (k/->byteArr :aevt vect)))))

  "basic vector conversion"
  (assert-vec-conversion :aevt [20 :hello  "some analysis" 3] [:hello 20 "some analysis" 3])

  "int value"
  (assert-vec-conversion :aevt [20 :hello  (int 2356) 3] [:hello 20 (int 2356) 3])

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

    "biggest 'e' value"
  (assert-vec-conversion :avet [9223372036854775807 :hello (long 234) 3] [:hello (long 234) 9223372036854775807 3]))



(deftest illegal-argument
  "wrong index descriptor"
  (is (thrown? AssertionError (k/->byteBuffer :vrt [:hello 9223372036854775807 (long 234) 3]))))





;;----- FDB Dathike integration -----
;;

(defn empty-db []
  (dh-db/empty-db nil :datahike.index/fdb))


(comment
  (def db (-> (empty-db)
            (with-datom (datom 123 :likes "Hans" 1 true))
            (with-datom (datom 124 :likes "GG" 1 true))))
  (d/datoms db :eavt)
  )


(deftest empty-db-creation
  "empty db creation"
  (is (empty-db)))


(deftest datoms-fn
  (let [db (-> (empty-db)
             (with-datom (datom 123 :likes "Hans" 1 true))
             (with-datom (datom 124 :likes "GG" 1 true)))]
    (testing "datoms works"
      (is (= [[123 :likes "Hans" 1]
              [124 :likes "GG" 1]]
            (d/datoms db :eavt))))
    ))


(deftest db-with
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> ;;(d/empty-db {:age {:db/index true}} :datahike.index/fdb)
             (empty-db)
             (d/db-with [ [:db/add 1 :name "Petr"]
                         [:db/add 1 :age 44]
                         [:db/add 2 :name "Ivan"]
                         [:db/add 2 :age 25]
                         [:db/add 3 :name "Sergey"]
                         [:db/add 3 :age 11] ]))]
    (testing "datoms in :eavt order"
      (is (= [[1 :age 44]
              [1 :name "Petr"]
              [2 :age 25]
              [2 :name "Ivan"]
              [3 :age 11]
              [3 :name "Sergey"]]
            (map dvec (d/datoms db :eavt)))))

    #_(testing "datoms in :aevt order"
      (is (= [[1 :age 44]
              [1 :name "Petr"]
              [2 :age 25]
              [2 :name "Ivan"]
              [3 :age 11]
              [3 :name "Sergey"]]
            (map dvec (d/datoms db :avet)))))))


(deftest using-with-datom
  "get"
  (let [db                          (empty-db)
        {:keys [eavt eavt-durable]} (-> (with-datom db (datom 123 :likes "Hans" 1 true))
                                      (with-datom (datom 124 :likes "GG" 1 true)))]

    ;; get :e-end
    (is (== (nth (fdb/get (:eavt-scalable db) :eavt
                   [123 :likes "Hans" 1 true])
              (k/position :eavt :e-end))
          123))
    (is (== (nth (fdb/get (:eavt-scalable db) :eavt
                   [124 :likes "GG" 1 true]) (k/position :eavt :e-end))
          124)))

  (testing "simple range :eavt"
    (let [db (empty-db)
          _  (-> (with-datom db (datom 123 :likes "Hans" 1 true))
               (with-datom (datom 124 :likes "GG" 1 true))
               (with-datom (datom 125 :likes "GG" 1 true))
               (with-datom (datom 1 :likes "GG" 1 true))
               (with-datom (datom 2 :likes "GG" 1 true))
               (with-datom (datom 3 :likes "GG" 1 true))
               (with-datom (datom 3 :likes "HH" 1 true)))]
      ;; :eavt
      (is (= 3
            (count (fdb/get-range :eavt
                     [123 :likes "Hans" 1 true]
                     [125 :likes "GG" 1 true]))))
      (is (= 2 ;; Not 3 because [125] does not exist in the db.
            (count (fdb/get-range :eavt [123] [125]))))
      (is (= 0
            (count (fdb/get-range :eavt [3] [3]))))
      (is (= 1
            (count (fdb/get-range :eavt
                     [3 :likes "HH" 1 true]
                     [3 :likes "HH" 1 true]))))
      ))

  ;; 06-05.2020: Will not work for now as here we send datoms in aevt format
  ;; whereas the code expects from now on to always be eavt. See function ->bytebuffer
  #_(testing "simple range :aevt"
      (let [db (empty-db)
            _  (-> (with-datom db (datom 123 :a "Hans" 1 true))
                 (with-datom (datom 124 :b "GG" 1 true))
                 (with-datom (datom 125 :c "GG" 1 true))
                 (with-datom (datom 1 :d "GG" 1 true))
                 (with-datom (datom 2 :e "GG" 1 true))
                 (with-datom (datom 3 :f "GG" 1 true))
                 (with-datom (datom 4 :f "GG" 1 true)))]
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


  #_(testing "simple range :avet"
      (let [db (empty-db)
            _  (-> (with-datom db (datom 123 :a "Hans" 1 true))
                 (with-datom (datom 124 :b "GG" 1 true))
                 (with-datom (datom 125 :c "GG" 1 true))
                 (with-datom (datom 1 :d "GG" 1 true))
                 (with-datom (datom 2 :e "GG" 1 true))
                 (with-datom (datom 3 :f "GG" 1 true))
                 (with-datom (datom 4 :f "GG" 1 true)))]
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
        #_(is (= 3
                (count (fdb/get-range :avet [:e] [:g]))))
        #_(is (= 0
                (count (fdb/get-range :avet [:f] [:f]))))))



  "large range"
  (let [db (empty-db)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 1 true)) db (range 100))]
    (is (= 51
          (count (fdb/get-range :eavt [1 :likes "Hans" 1 true]
                   [51 :likes "Hans" 1 true])))))

  "large range"
  ;; TODO: if we don't clear the fdb db then there are weird things:
  ;; check as the following might not work for instance.
  ;; Could it be that the same key is allowed to be reinserted multiple
  ;; times and appears multiple times?
  (let [db (empty-db)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 1 true)) db (range 10))]
    (is (= 3
          (count (fdb/get-range :eavt [1 :likes "Hans" 1 true]
                   [3 :likes "Hans" 1 true])))))

  ;; TODO: iterate-from no longer works so commenting out for now
  #_"iterate-from"
  #_(let [db         (empty-db)
          datom-1    (datom 123 :likes "Hans" 1 true)
          datom-2    (datom 124 :likes "GG" 1 true)
          datoms     (-> (with-datom db datom-1)
                         (with-datom datom-2))
          index-type :eavt
          iterate    #(take % (fdb/iterate-from index-type (k/key index-type datom-1)))
          iterate-5  (iterate 5)]

      ;; NOTE: Because the fdb keys are Java arrays, we need to convert them
      ;; into seq if we want to compate them with =
      ;; (is (some #(= (seq (fdb/key datom-1)) (seq %))
      ;;           (fdb/iterate-from index-type (fdb/key datom-1))))
      (is (= (seq (first (iterate 1)))
             (seq (k/key index-type datom-1))))
      (is (= (seq (first (iterate 2)))
             (seq (k/key index-type datom-1))))
      (is (= (seq (nth (iterate 2) 1))
             (seq (k/key index-type datom-2))))
      (is (= (seq (first iterate-5))
             (seq (nth iterate-5 0))))
      (is (not (= (seq (first iterate-5))
                  (seq (nth iterate-5 1)))))
      ))


#_(deftest slice-simple
  "slice-simple"
  (let [db                          (empty-db)
        datom-1                     (datom 123 :likes "Hans" 1 true)
        datom-2                     (datom 124 :likes "Hans" 1 true)
        datom-3                     (datom 125 :likes "Hans" 1 true)
        {:keys [eavt eavt-durable]} (-> (with-datom db datom-1)
                                        (with-datom datom-2))

        ;; before (k/key->vect :eavt (fdb/get-key (KeySelector/firstGreaterOrEqual (k/key :eavt [124 nil nil nil]))))
        ;; after  (k/key->vect :eavt (fdb/get-key (KeySelector/firstGreaterThan (k/key :eavt [124 nil nil nil]))))
        ;; r      (fdb/get-range :eavt [124 nil nil nil] [124 nil nil nil])

        create-eavt (fn [e a v tx] (datom e a v tx true))]
    (is (= datom-1
           (first (slice eavt eavt-durable (datom 123 nil nil 1 nil) [123]
                         (datom 124 nil nil 1 nil)  [124] create-eavt))))
    ;;(hmsg/lookup-fwd-iter eavt-durable [124 nil nil nil])
    (is (= datom-2
           (first (slice eavt eavt-durable (datom 124 nil nil 1 nil) [124]
                         create-eavt))))
    (is (= [datom-1 datom-2]
           (vec (slice eavt eavt-durable nil [123]
                       nil  [125] create-eavt))))
    ))


#_(deftest slice-large-range
  "slice-large"
  (let [db                          (empty-db)
        {:keys [eavt eavt-durable]} (reduce #(with-datom %1
                                               (datom %2 :likes "Hans" 1 true))
                                            db
                                            (range 100))
        create-eavt                 (fn [e a v tx] (datom e a v tx true))
        sliced                      (slice eavt eavt-durable (datom 50 nil nil 1 nil)
                                           [50 :likes] (datom 60 nil nil 1 nil) [60]
                                           create-eavt)]
    (is (= 11 ;; TODO: check but fdb.getrange and our slice does not seem to have the same behaviour. Why is it 2 here:     (is (= 2 ;; Not 3 because [125] does not exist in the db.
           ;; (count (fdb/get-range :eavt [123]
           ;;                       [125])))))
           ;;
           ;; and 11 in our current test.
           (count sliced)))))


(deftest fdb-using-init-db
  (testing "init-db on the simplest example"
    (let [db (empty-db)]
      (dh-db/init-db [(datom 1000 :a/b 3)
                      ;; TODO: weird if we use "5" instead of 5, it does not work
                      (datom 4 :shared/policy "hello")])))
  ;; TODO: test that what's inserted in fdb through init-db above is really inside fdb
  )



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
