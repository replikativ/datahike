(ns datahike.test.attribute-refs
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as d]
   [datahike.db :refer [ref-datoms]]
   [datahike.constants :as c]
   [datahike.test.core :as tdc]))

(deftest test-empty-db-without-attr-refs
  (let [cfg {:store {:backend :mem
                     :id "attr-refs-test"}
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
 (let [cfg {:store {:backend :mem
                    :id "attr-refs-test"}
            :keep-history? false
            :attribute-refs? true
            :schema-flexibility :read}
       _ (d/delete-database cfg)
       _ (d/create-database cfg)
       conn (d/connect cfg)]
  (testing "empty EAVT datoms"
   (is (= (count ref-datoms)
          (count (d/datoms @conn :eavt)))))
  (testing "empty AEVT datoms"
   (is (= (count ref-datoms)
          (count (d/datoms @conn :aevt)))))
  (testing "empty AVET datoms"
   (is (= (count ref-datoms) (d/datoms @conn :avet))))))

#_(let [cfg  {:store              {:backend :mem
                                 :id      "attr-refs-test"}
            :keep-history?      false
            :attribute-refs?    true
            :schema-flexibility :read}
      _    (d/delete-database cfg)
      _    (d/create-database cfg)
      conn (d/connect cfg)]
 (d/datoms @conn :avet)
 )