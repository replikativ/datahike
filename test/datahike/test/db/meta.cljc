(ns datahike.test.db.meta
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.test.utils :refer [setup-db]])
  #?(:clj (:import [java.util Date UUID])))

(defn with-default-cfg [cfg]
  (merge
   {:store {:backend :mem
            :id "test-db-meta"}
    :attribute-refs? false
    :keep-history? false
    :schema-flexibility :read}
   cfg))

(defn create-mem-db-cfg [id]
  (with-default-cfg {:store {:backend :mem
                             :id id}}))

(defn create-file-db-cfg [relative-path]
  (with-default-cfg {:store {:backend :file
                             :path (str #?(:clj (System/getProperty "java.io.tmpdir")
                                           :cljs "/tmp")
                                        relative-path)}}))

(deftest test-db-meta-data
  (testing "On empty DB"
    (let [cfg (create-mem-db-cfg "db-meta-data-test")
          conn (setup-db cfg)
          {:keys [id version created-at]} (:meta @conn)]
      (is (= #?(:clj UUID
                :cljs js/Date)
             (type id)))
      (is (= "DEVELOPMENT" version))
      (is (= #?(:clj Date
                :cljs js/Date)
             (type created-at)))))
  (testing "On existing empty DB"
    (let [cfg (create-file-db-cfg "/db-meta-data-empty-test")
          conn (setup-db cfg)
          new-conn (d/connect cfg)]
      (is (= (:meta @conn)
             (:meta @new-conn)))))
  (testing "On non-empty DB"
    (let [cfg (create-file-db-cfg "/db-meta-data-non-empty-test")
          conn (setup-db cfg)
          _ (d/transact conn [{:name "Alice" :age 25} {:name "Bob" :age 30} {:name "Charlie" :age 35}])
          new-conn (d/connect cfg)]
      (is (= (:meta @conn)
             (:meta @new-conn))))))
