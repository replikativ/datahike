(ns datahike.test.upsert
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-upsert
  (let [edb (d/empty-db {:name  {:db/unique :db.unique/identity} ; TODO: check empty-db with schema in tests!
                         :email {:db/unique :db.unique/identity}})

        e0 (:max-eid edb)
        e1 (+ 1 (:max-eid edb))
        e2 (+ 2 (:max-eid edb))
        e3 (+ 3 (:max-eid edb))
        db (d/db-with edb
                      [{:db/id e1 :name "Ivan" :email "@1"} {:db/id e2 :name "Petr" :email "@2"}])

        touched (fn [tx e] (into {} (d/touch (d/entity (:db-after tx) e))))
        tempids (fn [tx] (dissoc (:tempids tx) :db/current-tx))]
    (testing "upsert, no tempid"
      (let [tx (d/with db [{:name "Ivan" :age 35}])]
        (is (= (touched tx e1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 attrs, no tempid"
      (let [tx (d/with db [{:name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx e1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {-1 (+ d/e0 1)}))))

    (testing "upsert with string tempid"
      (let [tx (d/with db [{:db/id (str e1) :name "Ivan" :age 35}
                           [:db/add (str e2) :name "Oleg"]
                           [:db/add (str e2) :email "@2"]])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (touched tx (+ d/e0 2))
               {:name "Oleg" :email "@2"}))
        (is (= (tempids tx)
               {(str e1) e1
                (str e2) e2}))))

    (testing "upsert by 2 attrs with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {-1 (+ d/e0 1)}))))

    (testing "upsert to two entities, resolve to same tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -1 :name "Ivan" :age 36}])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@1" :age 36}))
        (is (= (tempids tx)
               {-1 (+ d/e0 1)}))))

    (testing "upsert to two entities, two tempids"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -2 :name "Ivan" :age 36}])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@1" :age 36}))
        (is (= (tempids tx)
               {-1 (+ e0 1), -2 (+ e0 1)}))))

    (testing "upsert with existing id"
      (let [tx (d/with db [{:db/id (+ d/e0 1) :name "Ivan" :age 35}])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 attrs with existing id"
      (let [tx (d/with db [{:db/id (+ d/e0 1) :name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert conficts with existing id"
      (is (thrown-msg? (str "Conflicting upsert: [:name \"Ivan\"] resolves to " e1 ", but entity already has :db/id " e2)
                       (d/with db [{:db/id e2 :name "Ivan" :age 36}]))))

    (testing "upsert conficts with non-existing id"
      (is (thrown-msg? (str "Conflicting upsert: [:name \"Ivan\"] resolves to " e1 ", but entity already has :db/id " e3)
                       (d/with db [{:db/id e3 :name "Ivan" :age 36}]))))

    (testing "upsert by non-existing value resolves as update"
      (let [tx (d/with db [{:name "Ivan" :email "@3" :age 35}])]
        (is (= (touched tx (+ d/e0 1))
               {:name "Ivan" :email "@3" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 conflicting fields"
      (is (thrown-msg? (str "Conflicting upserts: [:name \"Ivan\"] resolves to " e1 ", but [:email \"@2\"] resolves to " e2)
                       (d/with db [{:name "Ivan" :email "@2" :age 35}]))))

    (testing "upsert over intermediate db"
      (let [tx (d/with db [{:name "Igor" :age 35}
                           {:name "Igor" :age 36}])]
        (is (= (touched tx e3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert over intermediate db, tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -1 :name "Igor" :age 36}])]
        (is (= (touched tx e3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {-1 e3}))))

    (testing "upsert over intermediate db, different tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -2 :name "Igor" :age 36}])]
        (is (= (touched tx e3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {-1 e3, -2 e3}))))

    #_(testing "upsert and :current-tx conflict"            ;; TODO: check if different than randomized
        (is (thrown-with-msg? Throwable (re-pattern (str "Conflicting upsert: [:name \"Ivan\"] resolves to " e1 ", but entity already has :db/id " ctx))
                              (d/with db [{:db/id :db/current-tx :name "Ivan" :age 35}]))))))

(deftest test-redefining-ids
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [{:db/id -1 :name "Ivan"}]))]
    (let [tx (d/with db [{:db/id -1 :age 35}
                         {:db/id -1 :name "Ivan" :age 36}])]
      (is (= #{[(+ d/e0 1) :age 36] [(+ d/e0 1) :name "Ivan"]}
             (tdc/all-datoms (:db-after tx))))
      (is (= {-1 (+ d/e0 1), :db/current-tx (+ d/tx0 2)}
             (:tempids tx)))))

  (let [db (-> (d/empty-db {:name  {:db/unique :db.unique/identity}})
               (d/db-with [{:db/id -1 :name "Ivan"}
                           {:db/id -2 :name "Oleg"}]))]
    (is (thrown-with-msg? Throwable (re-pattern (str "Conflicting upsert: -1 resolves both to " (+ d/e0 1) " and " (+ d/e0 2)))
                          (d/with db [{:db/id -1 :name "Ivan" :age 35}
                                      {:db/id -1 :name "Oleg" :age 36}])))))

;; https://github.com/tonsky/datahike/issues/285
(deftest test-retries-order
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [[:db/add -1 :age 42]
                           [:db/add -2 :likes "Pizza"]
                           [:db/add -1 :name "Bob"]
                           [:db/add -2 :name "Bob"]]))]
    (is (= {:db/id (+ d/e0 1), :name "Bob", :likes "Pizza", :age 42}
           (tdc/entity-map db (+ d/e0 1)))))

  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [[:db/add -1 :age 42]
                           [:db/add -2 :likes "Pizza"]
                           [:db/add -2 :name "Bob"]
                           [:db/add -1 :name "Bob"]]))]
    (is (= {:db/id (+ d/e0 2), :name "Bob", :likes "Pizza", :age 42}
           (tdc/entity-map db (+ d/e0 2))))))

(deftest test-vector-upsert
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [{:db/id -1, :name "Ivan"}]))]
    (are [tx res] (= res (tdc/all-datoms (d/db-with db tx)))
      [[:db/add -1 :name "Ivan"]
       [:db/add -1 :age 12]]
      #{[(+ d/e0 1) :age 12] [(+ d/e0 1) :name "Ivan"]}

      [[:db/add -1 :age 12]
       [:db/add -1 :name "Ivan"]]
      #{[(+ d/e0 1) :age 12] [(+ d/e0 1) :name "Ivan"]}))

  (let [db (-> (d/empty-db {:name  {:db/unique :db.unique/identity}})
               (d/db-with [[:db/add -1 :name "Ivan"]
                           [:db/add -2 :name "Oleg"]]))]
    (is (thrown-with-msg? Throwable (re-pattern (str "Conflicting upsert: -1 resolves both to " (+ d/e0 1) " and " (+ d/e0 2)))
                          (d/with db [[:db/add -1 :name "Ivan"]
                                      [:db/add -1 :age 35]
                                      [:db/add -1 :name "Oleg"]
                                      [:db/add -1 :age 36]])))))
