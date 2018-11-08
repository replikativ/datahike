(ns datahike.db-test
  (:import (com.apple.foundationdb KeySelector))
  (:require [datahike.db :as dh-db :refer [with-datom slice]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fdb.core :as fdb]
            [datahike.db :refer [datom]]
           ))


(deftest fdb
  "get"
  (let [db                          (dh-db/empty-db)
        {:keys [eavt eavt-durable]} (-> (with-datom db (datom 123 "likes" "Hans" 0 true))
                                        (with-datom (datom 124 "likes" "GG" 0 true)))]

    (is (== (nth (fdb/get (:eavt-scalable db)
                          [123 "likes" "Hans" 0 true]) 7)
            123))
    (is (== (nth (fdb/get (:eavt-scalable db)
                          [124 "likes" "GG" 0 true]) 7)
            124)))

  "simple range"
  (let [db      (dh-db/empty-db)
        _  (fdb/clear-all)
        _ (-> (with-datom db (datom 123 "likes" "Hans" 0 true))
              (with-datom (datom 124 "likes" "GG" 0 true))
              (with-datom (datom 125 "likes" "GG" 0 true))
              (with-datom (datom 1 "likes" "GG" 0 true))
              (with-datom (datom 2 "likes" "GG" 0 true))
              (with-datom (datom 3 "likes" "GG" 0 true)))]
    (is (= 2
           (count (fdb/get-range [123 "likes" "Hans" 0 true]
                                 [125 "likes" "GG" 0 true]))))
    (is (= 2
           (count (fdb/get-range [123]
                                 [125])))))

  "large range"
  (let [db (dh-db/empty-db)
        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (datom %2 "likes" "Hans" 0 true)) db (range 100))]
    (is (= 50
           (count (fdb/get-range [1 "likes" "Hans" 0 true]
                                 [51 "likes" "Hans" 0 true])))))

  "large range"
  ;; TODO: if we don't clear the fdb db then tnere are weird things:
  ;; check as the following might not work for instance.
  ;; Could it be that the same key is allowed to be reinserted multiple
  ;; times and appears multiple times?
  (let [db (dh-db/empty-db)
        ;;        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (datom %2 "likes" "Hans" 0 true)) db (range 10))]
    (is (= 2
           (count (fdb/get-range [1 "likes" "Hans" 0 true]
                                 [3 "likes" "Hans" 0 true])))))

  "iterate-from"
  (let [db      (dh-db/empty-db)
        datom-1 (datom 123 "likes" "Hans" 0 true)
        datom-2 (datom 124 "likes" "GG" 0 true)
        datoms  (-> (with-datom db datom-1)
                    (with-datom datom-2))
        iterate #(take % (fdb/iterate-from (fdb/key datom-1)))
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


(deftest slice-test
  "slice"
  (let [db                          (dh-db/empty-db)
        _                           (fdb/clear-all)
        datom-1                     (datom 123 "likes" "Hans" 0 true)
        datom-2                     (datom 124 "likes" "GG" 0 true)
        datom-3                     (datom 125 "likes" "GG" 0 true)
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
           (vec (slice eavt eavt-durable (datom 123 nil nil nil nil) [123]
                       (datom 125 nil nil nil nil)  [125] create-eavt))))
    ))
