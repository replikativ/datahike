(ns datahike.test.db-test
  (:require
   [clojure.data]
   [clojure.core.async :as async]
   [hitchhiker.tree.utils.cljs.async :as ha]
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

(comment
  ;; REPL code


  ;;test-diff

  (def datom-thing [[(d/datom 1 :b 2) (d/datom 1 :c 4) (d/datom 2 :a 1)]
                    [(d/datom 1 :b 3) (d/datom 1 :d 5)]
                    [(d/datom 1 :a 1)]])


  (cljs.pprint/pprint datom-thing)

  (ha/go-try (println (ha/<? (d/db-with (ha/<? (d/empty-db)) [{:a 1 :b 2 :c 4} {:a 1}]))))

  (async/go
    (println
     (clojure.data/diff
      (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 2 :c 4} {:a 1}]))
      (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 3 :d 5}])))))

  (async/go
    (def datom-diff
      (clojure.data/diff
       (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 2 :c 4} {:a 1}]))
       (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 3 :d 5}])))))


  (async/go
    (println
     (= [[(d/datom 1 :b 2) (d/datom 1 :c 4) (d/datom 2 :a 1)]
         [(d/datom 1 :b 3) (d/datom 1 :d 5)]
         [(d/datom 1 :a 1)]]
        (clojure.data/diff
         (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 2 :c 4} {:a 1}]))
         (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 3 :d 5}]))))))

  ;;test-fn-hash-changes
  
  
  (async/go
    (let [db (async/<! (d/db-with (async/<! (d/empty-db))
                                  [{:db/id 1 :name "Konrad"}]))
          _ (println db)
          r1 (async/<! (d/db-with db [[:db.fn/retractEntity 1]]))
          _ (println r1)
          _ (println "the hash" (hash r1))
          r2 (async/<! (d/db-with db [[:db.fn/retractEntity 1]]))]

      (is (= (hash r1) (hash r2)))))


  ;; format blocker
  )

#_#?(:cljs
   (deftest test-diff
     (t/async done
              (async/go
                (is (= [[(d/datom 1 :b 2) (d/datom 1 :c 4) (d/datom 2 :a 1)]
                        [(d/datom 1 :b 3) (d/datom 1 :d 5)]
                        [(d/datom 1 :a 1)]]
                       
                       (clojure.data/diff
                        (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 2 :c 4} {:a 1}]))
                        (async/<! (d/db-with (async/<! (d/empty-db)) [{:a 1 :b 3 :d 5}])))))
                (done)))))

#_#?(:cljs (deftest test-fn-hash-changes
           (let [db (async/<! (d/db-with (async/<! (d/empty-db))
                                         [{:db/id 1 :name "Konrad"}]))
                 r1 (async/<! (d/db-with db [[:db.fn/retractEntity 1]]))
                 r2 (async/<! (d/db-with db [[:db.fn/retractEntity 1]]))]
             (is (= (hash r1) (hash r2))))))

#_(deftest test-equiv-db-hash
  (let [db (d/db-with (d/empty-db)
                      [{:db/id 1 :name "Konrad"}])
        r1 (d/db-with db [[:db.fn/retractEntity 1]])]
    (is (= (hash (d/empty-db)) (hash r1)))))

#_(deftest empty-db-with-schema
  (testing "Test old write schema"
    (is (thrown-msg?
         "Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
         (d/empty-db {:name {:db/cardinality :db.cardinality/many}} {:schema-flexibility :write})))
    (is (= (merge db/implicit-schema
                  {:name {:db/cardinality :db.cardinality/one :db/valueType :db.type/string}})
           (:schema (d/empty-db {:name {:db/cardinality :db.cardinality/one
                                        :db/valueType :db.type/string}}
                                {:schema-flexibility :write}))))

    (is (= (merge db/implicit-schema
                  {:name {:db/ident :name :db/cardinality :db.cardinality/one :db/valueType :db.type/string}})
           (:schema (d/empty-db [{:db/ident :name
                                  :db/cardinality :db.cardinality/one
                                  :db/valueType :db.type/string}]
                                {:schema-flexibility :write}))))))
