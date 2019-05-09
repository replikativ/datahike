(ns datahike.test.fdb
  (:import (com.apple.foundationdb KeySelector
                                   FDB))
  (:require [datahike.db :as dh-db :refer [with-datom slice]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fdb.core :as fdb]
            [fdb.keys :as k]
            ;;[hitchhiker.tree.messaging :as hmsg]
            [datahike.db :refer [datom]]))

;;---- FDB Keys -----


(defn assert-vec-conversion
  [index-type vect]
  (let [buff       (k/->byteBuffer index-type vect)
        buff->vect (k/byteBuffer->vect index-type buff)
        ]
    (is (= vect buff->vect))))

(deftest eavt
  "->byteArr and back"
  (let [vect [20 :hello "some analysis" 3]]
    (is (= (k/key->vect :eavt (k/->byteArr :eavt vect)) vect)))

  "basic vector conversion"
  (assert-vec-conversion :eavt [20 :hello "some analysis" 3])

  "int value"
  (assert-vec-conversion :eavt [20 :hello (int 2356) 3])

  "long value"
  (assert-vec-conversion :eavt [20 :hello (long 234) 3])

  "biggest 'e' value"
  (assert-vec-conversion :eavt [9223372036854775807 :hello (long 234) 3]))

;; --------- :aevt indices
(deftest aevt
  "simple insert and retrieval"
  (let [vect [:hello 20 "some data" 3]]
    (is (= vect (k/key->vect :aevt (k/->byteArr :aevt vect)))))

  "basic vector conversion"
  (assert-vec-conversion :aevt [:hello 20 "some analysis" 3])

  "int value"
  (assert-vec-conversion :aevt [:hello 20 (int 2356) 3])

  "biggest 'e' value"
  (assert-vec-conversion :aevt [:hello 9223372036854775807 (long 234) 3])
  )


;; --------- :avet indices
(deftest aevt
  "simple insert and retrieval"
  (let [vect [:hello "some values" 20  3]]
    (is (= vect (k/key->vect :avet (k/->byteArr :avet vect)))))

  "basic vector conversion"
  (assert-vec-conversion :avet [:hello "some analysis" 20 3])

  "int value"
  (assert-vec-conversion :avet [:hello (int 2356) 20 3])

  "biggest 'e' value"
  (assert-vec-conversion :avet [:hello (long 234) 9223372036854775807 3])
  )


(deftest illegal-argument
  (is (thrown? IllegalArgumentException (k/->byteBuffer :vrt [:hello 9223372036854775807 (long 234) 3]))))



;;----- FDB integration -----

(deftest fdb-using-with-datom
  "get"
  (let [db                          (dh-db/empty-db)
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
    (let [db (dh-db/empty-db)
          _  (fdb/clear-all)
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
                                   [3 :likes "HH" 1 true]))))))

  (testing "simple range :aevt"
    (let [db (dh-db/empty-db)
          _  (fdb/clear-all)
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
      (let [db (dh-db/empty-db)
            _  (fdb/clear-all)
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
  (let [db (dh-db/empty-db)
        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 1 true)) db (range 100))]
    (is (= 51
           (count (fdb/get-range :eavt [1 :likes "Hans" 1 true]
                                 [51 :likes "Hans" 1 true])))))

  "large range"
  ;; TODO: if we don't clear the fdb db then there are weird things:
  ;; check as the following might not work for instance.
  ;; Could it be that the same key is allowed to be reinserted multiple
  ;; times and appears multiple times?
  (let [db (dh-db/empty-db)
        ;;        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 1 true)) db (range 10))]
    (is (= 3
           (count (fdb/get-range :eavt [1 :likes "Hans" 1 true]
                                 [3 :likes "Hans" 1 true])))))

  "iterate-from"
  ;; TODO: iterate-from no longer works so commenting out for now
  #_(let [db         (dh-db/empty-db)
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


(deftest slice-simple
  "slice-simple"
  (let [db                          (dh-db/empty-db)
        _                           (fdb/clear-all)
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


(deftest slice-large-range
  "slice-large"
  (let [db                          (dh-db/empty-db)
        _                           (fdb/clear-all)
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
    (let [db (dh-db/empty-db)]
      (dh-db/init-db [(dh-db/datom 1000 :a/b 3)
                      ;; TODO: weird if we use "5" instead of 5, it does not work
                      (dh-db/datom 4 :shared/policy "hello")])))
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
