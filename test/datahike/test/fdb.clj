(ns datahike.test.fdb
  (:import (com.apple.foundationdb KeySelector
                                   FDB))
  (:require [datahike.db :as dh-db :refer [with-datom slice]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fdb.core :as fdb]
            [fdb.keys :as k]
            [datahike.db :refer [datom]]))

;;---- FDB Keys -----


;; helper
(defn assert-vec-conversion
  [vect]
  (let [buff       (k/->byteBuffer vect)
        buff->vect (k/byteBuffer->vect buff)
        ;; _          (prn buff->vect)
        ;; _          (prn vect)
        ]
    (is (= buff->vect vect))))

(deftest fdb-keys
  "->byteArr and back"
  (let [vect [20 :hello "some analysis" 3]]
    (is (= (k/key->vect (k/->byteArr vect)) vect)))

  "basic vector conversion"
  (assert-vec-conversion [20 :hello "some analysis" 3])

  "int value"
  (assert-vec-conversion [20 :hello (int 2356) 3])

  "long value"
  (assert-vec-conversion [20 :hello (long 234) 3])
  )


;;----- FDB integration -----

(deftest fdb-using-with-datom
  "get"
  (let [db                          (dh-db/empty-db)
        {:keys [eavt eavt-durable]} (-> (with-datom db (datom 123 :likes "Hans" 0 true))
                                        (with-datom (datom 124 :likes "GG" 0 true)))]

    (is (== (nth (fdb/get (:eavt-scalable db)
                          [123 :likes "Hans" 0 true]) 7)
            123))
    (is (== (nth (fdb/get (:eavt-scalable db)
                          [124 :likes "GG" 0 true]) 7)
            124)))

  "simple range"
  (let [db (dh-db/empty-db)
        _  (fdb/clear-all)
        _  (-> (with-datom db (datom 123 :likes "Hans" 0 true))
               (with-datom (datom 124 :likes "GG" 0 true))
               (with-datom (datom 125 :likes "GG" 0 true))
               (with-datom (datom 1 :likes "GG" 0 true))
               (with-datom (datom 2 :likes "GG" 0 true))
               (with-datom (datom 3 :likes "GG" 0 true)))]
    (is (= 3
           (count (fdb/get-range [123 :likes "Hans" 0 true]
                                 [125 :likes "GG" 0 true]))))
    (is (= 2 ;; Not 3 because [125] does not exist in the db.
           (count (fdb/get-range [123]
                                 [125])))))

  "large range"
  (let [db (dh-db/empty-db)
        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 0 true)) db (range 100))]
    (is (= 51
           (count (fdb/get-range [1 :likes "Hans" 0 true]
                                 [51 :likes "Hans" 0 true])))))

  "large range"
  ;; TODO: if we don't clear the fdb db then there are weird things:
  ;; check as the following might not work for instance.
  ;; Could it be that the same key is allowed to be reinserted multiple
  ;; times and appears multiple times?
  (let [db (dh-db/empty-db)
        ;;        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (datom %2 :likes "Hans" 0 true)) db (range 10))]
    (is (= 3
           (count (fdb/get-range [1 :likes "Hans" 0 true]
                                 [3 :likes "Hans" 0 true])))))

  "iterate-from"
  (let [db        (dh-db/empty-db)
        datom-1   (datom 123 :likes "Hans" 0 true)
        datom-2   (datom 124 :likes "GG" 0 true)
        datoms    (-> (with-datom db datom-1)
                      (with-datom datom-2))
        iterate   #(take % (fdb/iterate-from (fdb/key datom-1)))
        iterate-5 (iterate 5)]

    ;; NOTE: Because the fdb keys are Java arrays, we need to convert them
    ;; into seq if we want to compate them with =
    ;; (is (some #(= (seq (fdb/key datom-1)) (seq %))
    ;;           (fdb/iterate-from (fdb/key datom-1))))
    (is (= (seq (first (iterate 1)))
           (seq (fdb/key datom-1))))
    (is (= (seq (first (iterate 2)))
           (seq (fdb/key datom-1))))
    (is (= (seq (nth (iterate 2) 1))
           (seq (fdb/key datom-2))))
    (is (= (seq (first iterate-5))
           (seq (nth iterate-5 0))))
    (is (not (= (seq (first iterate-5))
                (seq (nth iterate-5 1)))))
    ))


(deftest slice-simple
  "slice-simple"
  (let [db                          (dh-db/empty-db)
        _                           (fdb/clear-all)
        datom-1                     (datom 123 :likes "Hans" 0 true)
        datom-2                     (datom 124 :likes "Hans" 0 true)
        datom-3                     (datom 125 :likes "Hans" 0 true)
        {:keys [eavt eavt-durable]} (-> (with-datom db datom-1)
                                        (with-datom datom-2))
        create-eavt                 (fn [e a v tx] (datom e a v tx true))]
    (is (= datom-1
           (first (slice eavt eavt-durable (datom 123 nil nil nil nil) [123]
                         (datom 124 nil nil nil nil)  [124] create-eavt))))
    (is (= datom-2
           (first (slice eavt eavt-durable (datom 124 nil nil nil nil) [124]
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
                                               (datom %2 :likes "Hans" 0 true))
                                            db
                                            (range 100))
        create-eavt                 (fn [e a v tx] (datom e a v tx true))
        sliced                      (slice eavt eavt-durable (datom 50 nil nil nil nil)
                                           [50 :likes] (datom 60 nil nil nil nil) [60]
                                           create-eavt)]
    (is (= 11 ;; TODO: check but fdb.getrange and our slice does not seem to have the same behaviour. Why is it 2 here:     (is (= 2 ;; Not 3 because [125] does not exist in the db.
           ;; (count (fdb/get-range [123]
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
          (.set tr (fdb.core/key [1 1 "a" 1]) (fdb.core/key [1 1 "a" 1])))))
    db))


;; To test how quick inserting 100k datoms is
(comment
  (let [v  (byte-array [])
        fd (FDB/selectAPIVersion 510)
        all_kv (map #(vector (fdb.core/key [%1 (str ":attribute/" %1) %1 %1])  v)
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
