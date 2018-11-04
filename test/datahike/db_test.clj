(ns datahike.db-test
  (:import [datahike.db Datom]
           (com.apple.foundationdb KeySelector))
  (:require [datahike.db :as dh-db :refer [with-datom slice]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fdb.core :as fdb]))


(deftest fdb
  "get"
  (let [db                          (dh-db/empty-db)
        {:keys [eavt eavt-durable]} (-> (with-datom db (Datom. 123 :likes "Hans" 0 true))
                                        (with-datom (Datom. 124 :likes "GG" 0 true)))]

    (is (== (nth (fdb/get (:eavt-scalable db)
                          [123 :likes "Hans" 0 true]) 7)
            123))
    (is (== (nth (fdb/get (:eavt-scalable db)
                          [124 :likes "GG" 0 true]) 7)
            124)))

  "simple range"
  (let [db      (dh-db/empty-db)
        _  (fdb/clear-all)
        _ (-> (with-datom db (Datom. 123 :likes "Hans" 0 true))
              (with-datom (Datom. 124 :likes "GG" 0 true))
              (with-datom (Datom. 125 :likes "GG" 0 true))
              (with-datom (Datom. 1 :likes "GG" 0 true))
              (with-datom (Datom. 2 :likes "GG" 0 true))
              (with-datom (Datom. 3 :likes "GG" 0 true)))]
    (is (= 2
           (count (fdb/get-range [123 :likes "Hans" 0 true]
                                 [125 :likes "GG" 0 true])))))

  "large range"
  (let [db (dh-db/empty-db)
        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (Datom. %2 :likes "Hans" 0 true)) db (range 100))]
    (is (= 50
           (count (fdb/get-range [1 :likes "Hans" 0 true]
                                 [51 :likes "Hans" 0 true])))))

  "large range"
  ;; TODO: if we don't clear the fdb db then tnere are weird things:
  ;; check as the following might not work for instance.
  ;; Could it be that the same key is allowed to be reinserted multiple
  ;; times and appears multiple times?
  (let [db (dh-db/empty-db)
        ;;        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (Datom. %2 :likes "Hans" 0 true)) db (range 10))]
    (is (= 2
           (count (fdb/get-range [1 :likes "Hans" 0 true]
                                 [3 :likes "Hans" 0 true])))))

  "iterate-from"
  (let [db    (dh-db/empty-db)
        datom-1 (Datom. 123 :likes "Hans" 0 true)
        datom-2 (Datom. 124 :likes "GG" 0 true)
        datoms  (-> (with-datom db datom-1)
                    (with-datom datom-2))
        iterate #(take % (fdb/iterate-from (fdb/key datom-1)))
        iterate-5 (iterate 5)]
    ;; NOTE: Because the fdb keys are Java arrays, we need to convert them
    ;; into seq if we want to compate them with =
    (is (some #(= (seq (fdb/key datom-1)) (seq %))
              (fdb/iterate-from (fdb/key datom-1))))
    (is (= (seq (first (iterate 1)))
           (seq (fdb/key datom-1))))
    (is (= (seq (first (iterate 2)))
           (seq (fdb/key datom-1))))
    (is (= (seq (first iterate-5))
           (seq (nth iterate-5 0))))
    (is (not (= (seq (first iterate-5))
                (seq (nth iterate-5 1)))))
    (is (not (= (seq (nth iterate-5 3))
                (seq (nth iterate-5 4)))))
    #_(is (= (seq (fdb/key datom))
           (seq (first iterate-5))))
    #_(is (= (seq (nth iterate-5 2))
           (seq (first iterate-5))))
    )


  "slice"
  #_(let [db                          (dh-db/empty-db)
        datom                       (Datom. 124 :likes "GG" 0 true)
        {:keys [eavt eavt-durable]} (-> (with-datom db (Datom. 123 :likes "Hans" 0 true))
                                        (with-datom datom))
        create-eavt                 (fn [e a v tx] (Datom. e a v tx true))]
    (is (= datom
           (first (slice eavt eavt-durable (Datom. 123 nil nil nil nil) [124] create-eavt)))))
)
