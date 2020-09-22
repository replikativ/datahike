(ns datahike.test.api
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.test.core]
   [datahike.api :as d]))

(deftest test-database-hash
  (testing "Hashing without history"
    (let [cfg {:store {:backend :mem
                       :id "hashing"}
               :keep-history? false
               :schema-flexibility :read}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          hash-0 0]
      (testing "first hash equals zero"
        (is (= hash-0 (hash @conn))))
      (testing "hash remains 0 after reconnecting"
        (is (= hash-0 (-> (d/connect cfg) deref hash))))
      (testing "add entity to database"
        (let [_ (d/transact conn [{:db/id 1 :name "Max Mustermann"}])
              hash-1 (hash @conn)]
          (is (= hash-1 (-> (d/connect cfg) deref hash)))
          (testing "remove entity again"
            (let [_ (d/transact conn [[:db/retractEntity 1]])
                  hash-2 (hash @conn)]
              (is (not= hash-2 hash-1))
              (is (= hash-0 hash-2))))))))
  (testing "Hashing with history"
    (let [cfg {:store {:backend :mem
                       :id "hashing-with-history"}
               :keep-history? true
               :schema-flexibility :read}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          hash-0 (hash @conn)]
      (testing "first hash equals zero"
        (is (= hash-0 (hash @conn))))
      (testing "hash remains 0 after reconnecting"
        (is (= hash-0 (-> (d/connect cfg) deref hash))))
      (testing "add entity to database"
        (let [_ (d/transact conn [{:db/id 1 :name "Max Mustermann"}])
              hash-1 (hash @conn)]
          (is (= hash-1 (-> (d/connect cfg) deref hash)))
          (testing "retract entity again"
            (let [_ (d/transact conn [[:db/retractEntity 1]])
                  hash-2 (hash @conn)]
              (is (not= hash-1 hash-2))
              (is (not= hash-0 hash-2)))))))))