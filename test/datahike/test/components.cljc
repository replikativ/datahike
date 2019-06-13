(ns datahike.test.components
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as api]
   [datahike.core :as d]
   [datahike.db :as db]
   [datahike.test.core :as tdc]))

(t/use-fixtures :once tdc/no-namespace-maps)

#?(:cljs
   (def Throwable js/Error))

(def uri "datahike:mem://dev")

(deftest test-components
  #_(is (thrown-msg? "Bad attribute specification for :profile: {:db/isComponent true} should also have {:db/valueType :db.type/ref}"
                     (d/empty-db {:profile {:db/isComponent true}})))
  #_(is (thrown-msg? "Bad attribute specification for {:profile {:db/isComponent \"aaa\"}}, expected one of #{true false}"
                     (d/empty-db {:profile {:db/isComponent "aaa" :db/valueType :db.type/ref}})))

  (let [_ (api/create-database uri [{:db/id #db/id[db.part/db]
                                     :db/ident :profile
                                     :db/valueType :db.type/ref
                                     :db/isComponent true}
                                    {:db/id #db/id[db.part/db]
                                     :db/ident :name
                                     :db/valueType :db.type/string}
                                    {:db/id #db/id[db.part/db]
                                     :db/ident :email
                                     :db/valueType :db.type/string}])
        db (d/db-with
            (api/db (api/connect uri))
            [{:db/id 6 :name "Ivan" :profile 7}
             {:db/id 7 :email "@3"}
             {:db/id 8 :email "@4"}])
        visible #(edn/read-string (pr-str %))
        touched #(visible (d/touch %))]

    (testing "touch"
      (is (= {:db/id 6
              :name "Ivan"
              :profile {:db/id 7
                        :email "@3"}}
             (touched (d/entity db 6))))
      (is (= {:db/id 6
              :name "Ivan"
              :profile {:db/id 7
                        :email "@3"
                        :profile {:db/id 8
                                  :email "@4"}}}
             (touched (d/entity (d/db-with db [[:db/add 7 :profile 8]]) 6)))))
    (testing "retractEntity"
      (let [db (d/db-with db [[:db.fn/retractEntity 6]])]
        (is (= #{}
               (d/q '[:find ?a ?v :where [6 ?a ?v]] db)))
        (is (= #{}
               (d/q '[:find ?a ?v :where [7 ?a ?v]] db)))))

    (testing "retractAttribute"
      (let [db (d/db-with db [[:db.fn/retractAttribute 6 :profile]])]
        (is (= #{}
               (d/q '[:find ?a ?v :where [7 ?a ?v]] db)))))

    (testing "reverse navigation"
      (is (= {:db/id 6}
             (visible (:_profile (d/entity db 7))))))))

(deftest test-components-multival
  (let [_ (api/create-database uri [{:db/id #db/id[db.part/db]
                                     :db/ident :profile
                                     :db/cardinality :db.cardinality/many
                                     :db/valueType :db.type/ref
                                     :db/isComponent true}
                                    {:db/id #db/id[db.part/db]
                                     :db/ident :name
                                     :db/valueType :db.type/string}
                                    {:db/id #db/id[db.part/db]
                                     :db/ident :email
                                     :db/valueType :db.type/string}])
        db (d/db-with
            (api/db (api/connect uri))
            [{:db/id 6 :name "Ivan" :profile [7 8]}
             {:db/id 7 :email "@3"}
             {:db/id 8 :email "@4"}])
        visible #(edn/read-string (pr-str %))
        touched #(visible (d/touch %))]

    (testing "touch"
      (is (= {:db/id 6
              :name "Ivan"
              :profile #{{:db/id 7 :email "@3"}
                         {:db/id 8 :email "@4"}}}
             (touched (d/entity db 6)))))

    (testing "retractEntity"
      (let [db (d/db-with db [[:db.fn/retractEntity 6]])]
        (is (= #{}
               (d/q '[:find ?a ?v :in $ [?e ...] :where [?e ?a ?v]] db [6 7 8])))))

    (testing "retractAttribute"
      (let [db (d/db-with db [[:db.fn/retractAttribute 6 :profile]])]
        (is (= #{}
               (d/q '[:find ?a ?v :in $ [?e ...] :where [?e ?a ?v]] db [7 8])))))

    (testing "reverse navigation"
      (is (= {:db/id 6}
             (visible (:_profile (d/entity db 7))))))))
