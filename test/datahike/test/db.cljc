(ns datahike.test.db
  (:require
   [clojure.data]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.db :as db #?@(:cljs [:refer-macros [defrecord-updatable]]
                           :clj  [:refer [defrecord-updatable]])]))

;;
;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  #?@(:cljs [IHash                (-hash  [hb] 0xBEEF)]
      :clj  [clojure.lang.IHashEq (hasheq [hb] 0xBEEF)]))

(deftest test-defrecord-updatable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))

(defn- now []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(deftest test-uuid
  (let [now-ms (loop []
                 (let [ts (now)]
                   (if (> (mod ts 1000) 900) ;; sleeping over end of a second
                     (recur)
                     ts)))
        now    (int (/ now-ms 1000))]
    (is (= (* 1000 now) (d/squuid-time-millis (d/squuid))))
    (is (not= (d/squuid) (d/squuid)))
    (is (= (subs (str (d/squuid)) 0 8)
           (subs (str (d/squuid)) 0 8)))))

(deftest test-diff
  (is (= [[(d/datom 1 :b 2) (d/datom 1 :c 4) (d/datom 2 :a 1)]
          [(d/datom 1 :b 3) (d/datom 1 :d 5)]
          [(d/datom 1 :a 1)]]
         (clojure.data/diff
          (d/db-with (d/empty-db) [{:a 1 :b 2 :c 4} {:a 1}])
          (d/db-with (d/empty-db) [{:a 1 :b 3 :d 5}])))))

(deftest test-fn-hash-changes
  (let [db (d/db-with (d/empty-db)
                      [{:db/id 1 :name "Konrad"}])
        r1 (d/db-with db [[:db.fn/retractEntity 1]])
        r2 (d/db-with db [[:db.fn/retractEntity 1]])]
    (is (= (hash r1) (hash r2)))))

(deftest test-equiv-db-hash
  (let [db (d/db-with (d/empty-db)
                      [{:db/id 1 :name "Konrad"}])
        r1 (d/db-with db [[:db.fn/retractEntity 1]])]
    (is (= (hash (d/empty-db)) (hash r1)))))

(deftest empty-db-with-schema
  (testing "Test old write schema"
    (is (thrown-msg?
         "Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
         (d/empty-db {:name {:db/cardinality :db.cardinality/many}} {:schema-flexibility :write})))
    (is (= {:name {:db/cardinality :db.cardinality/one :db/valueType :db.type/string}
            :db/ident {:db/unique :db.unique/identity}}
           (:schema (d/empty-db {:name {:db/cardinality :db.cardinality/one
                                        :db/valueType :db.type/string}}
                                {:schema-flexibility :write}))))

    (is (= {:name {:db/ident :name :db/cardinality :db.cardinality/one :db/valueType :db.type/string}
            :db/ident {:db/unique :db.unique/identity}}
           (:schema (d/empty-db [{:db/ident :name
                                  :db/cardinality :db.cardinality/one
                                  :db/valueType :db.type/string}]
                                {:schema-flexibility :write}))))))
