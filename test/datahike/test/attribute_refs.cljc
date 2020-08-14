(ns datahike.test.attribute-refs
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as d]
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
    (testing "EAVT system datoms"
      (is (= (into #{} (map #(conj % true) c/system-schema))
             (->> (d/datoms @conn :eavt nil)
                  (mapv (comp vec seq))
                  (into #{})))))
    (testing "AEVT system datoms"
      (is (= (into #{} (map #(conj % true) c/system-schema))
             (->> (d/datoms @conn :aevt nil)
                  (mapv (comp vec seq))
                  (into #{})))))
    (testing "AVET system datoms"
      (let [xf (comp
                (map #(conj % true))
                (filter (fn [[_ a _ _]] (= 1 a))))]
        (is (= (into #{} xf c/system-schema)
               (->> (d/datoms @conn :avet nil)
                    (mapv (comp vec seq))
                    (into #{}))))))))
