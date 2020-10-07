(ns datahike.test.attribute-refs
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.api :as d]
   [datahike.db :as db :refer [ref-datoms]])
  #?(:clj (:import [clojure.lang AMapEntry]
                   [java.util Date]
                   [datahike.datom Datom])))

(def schema [{:db/ident       :name
              :db/cardinality :db.cardinality/one
              :db/index       true
              :db/unique      :db.unique/identity
              :db/valueType   :db.type/string}
             {:db/ident       :sibling
              :db/cardinality :db.cardinality/many
              :db/valueType   :db.type/ref}
             {:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])

(def no-ref-cfg
  {:store              {:backend :mem
                        :id      "attr-no-refs-test"}
   :keep-history?      false
   :attribute-refs?    false
   :schema-flexibility :read})

(def ref-cfg
  {:store              {:backend :mem
                        :id      "attr-no-refs-test"}
   :keep-history?      false
   :attribute-refs?    true
   :schema-flexibility :write})

(defn start-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(deftest test-empty-db-without-attr-refs
  (let [conn (start-db no-ref-cfg)]
    (testing "empty EAVT datoms"
      (is (= nil
             (d/datoms @conn :eavt nil))))
    (testing "empty AEVT datoms"
      (is (= nil
             (d/datoms @conn :aevt nil))))
    (testing "empty AVET datoms"
      (is (= nil
             (d/datoms @conn :avet nil))))))

(deftest test-empty-db-with-attr-refs
  (let [conn (start-db ref-cfg)]
    (testing "empty EAVT datoms"
      (is (= (set ref-datoms)
             (set (d/datoms @conn :eavt)))))
    (testing "empty AEVT datoms"
      (is (= (set ref-datoms)
             (set (d/datoms @conn :aevt)))))
    (testing "empty AVET datoms"
      (is (= (set (filter (fn [^Datom datom] (contains? #{1 9} (.-a datom)))
                          ref-datoms))
             (set (d/datoms @conn :avet)))))))

(deftest test-invalid-config-with-attr-refs
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        read-config (assoc ref-cfg :schema-flexibility :read)]
    (is (thrown-msg "Attribute references cannot be used with schema-flexibility ':read'.")
        (db/empty-db schema read-config))
    (is (thrown-msg "Attribute references cannot be used with schema-flexibility ':read'.")
        (db/init-db [] schema read-config))))

