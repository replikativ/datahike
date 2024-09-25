(ns datahike.test.attribute-refs.datoms-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db :refer [ref-datoms]]
   [datahike.test.utils :refer [with-connect provide-unique-id
                                recreate-database]]))

(def cfg
  {:store {:backend :mem}
   :keep-history? true
   :attribute-refs? true
   :schema-flexibility :write})

(deftest test-datoms-with-components
  (with-connect [conn (-> cfg
                          provide-unique-id
                          recreate-database)]
    (d/transact conn [{:db/ident :name
                       :db/cardinality :db.cardinality/one
                       :db/index true
                       :db/valueType :db.type/string}
                      {:db/ident :age
                       :db/cardinality :db.cardinality/one
                       :db/valueType :db.type/long}])
    (d/transact conn [{:name "Alice"
                       :age 10}])
    (let [all-datoms (d/datoms @conn {:index :avet :components []})
          all-tx-inst-datoms (filter (fn [datom]
                                       (and (= (:e datom)
                                               (:tx datom))
                                            (instance? java.util.Date
                                                       (:v datom))))
                                     all-datoms)
          name-datoms (filter (fn [datom] (= "Alice" (:v datom)))
                              all-datoms)]
      (is (= 1 (count name-datoms)))
      (doseq [datom name-datoms]
        (is (= [datom]
               (d/datoms
                @conn
                {:index :avet
                 :components [(:a datom)]})))
        (is (= [datom]
               (d/datoms
                @conn
                {:index :avet
                 :components [(:a datom)
                              (:v datom)]})))
        (is (= [datom]
               (d/datoms
                @conn
                {:index :avet
                 :components [(:a datom)
                              (:v datom)
                              (:e datom)]})))
        (is (= [datom]
               (d/datoms
                @conn
                {:index :avet
                 :components [(:a datom)
                              (:v datom)
                              (:e datom)
                              (:tx datom)]}))))

      (is (= 3 (count all-tx-inst-datoms)))
      (is (= (set all-tx-inst-datoms)
             (set (d/datoms @conn {:index :avet
                                   :components [:db/txInstant]}))))
      (is (= (set all-tx-inst-datoms)
             (set (d/datoms @conn {:index :aevt
                                   :components [:db/txInstant]}))))
      (doseq [datom all-tx-inst-datoms]
        (is (= [datom]
               (d/datoms
                @conn
                {:index :avet
                 :components [:db/txInstant
                              (:v datom)
                              (:e datom)]})))
        (is (= [datom]
               (d/datoms
                @conn
                {:index :avet
                 :components [:db/txInstant
                              (:v datom)
                              (:e datom)
                              (:tx datom)]})))
        (is (= [datom]
               (d/datoms
                @conn
                {:index :aevt
                 :components [:db/txInstant
                              (:e datom)]})))
        (is (= [datom]
               (d/datoms
                @conn
                {:index :aevt
                 :components [:db/txInstant
                              (:e datom)
                              (:v datom)]})))
        (is (= [datom]
               (d/datoms
                @conn
                {:index :aevt
                 :components [:db/txInstant
                              (:e datom)
                              (:v datom)
                              (:tx datom)]})))))))
