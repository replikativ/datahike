(ns datahike.test.attribute-refs
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.constants :as c]
   [datahike.db :refer [ref-datoms]])
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

(deftest test-empty-db-without-attr-refs
  (let [cfg {:store {:backend :mem
                     :id "attr-no-refs-test"}
             :keep-history? false
             :attribute-refs? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
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
  (let [cfg  {:store              {:backend :mem
                                   :id      "attr-refs-test"}
              :keep-history?      false
              :attribute-refs?    true
              :schema-flexibility :read}
        _    (d/delete-database cfg)
        _    (d/create-database cfg)
        conn (d/connect cfg)]
    (testing "empty EAVT datoms"
      (is (= (count ref-datoms)
             (count (d/datoms @conn :eavt)))))
    (testing "empty AEVT datoms"
      (is (= (count ref-datoms)
             (count (d/datoms @conn :aevt)))))
    (testing "empty AVET datoms"
      (is (= (count (filter (fn [^Datom datom] (contains? #{1 9}  (.-a datom)))
                            ref-datoms))
             (count (d/datoms @conn :avet)))))))