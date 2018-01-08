(ns datahike.test.middleware
  (:require
    [datahike.core :as d]
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datahike.db :as db]
    [datahike.middleware :as mw]
    [datahike.test.core :as tdc])
  #?(:clj
      (:import [clojure.lang ExceptionInfo])))

(defn reset-meta
  ""
  [new-meta]
  (fn [transact]
    (fn [report txs]
      (let [report (transact report txs)]
        (update-in
          report
          [:db-after]
          with-meta
          new-meta)))))

;; Theoretically something could happen where the meta is not preserved during the transaction.
(def eat-meta (reset-meta nil))

(deftest test-keep-meta
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (with-meta {:has-meta true}))]

    (is (not
          (->
            db
            (d/db-with [[:db/add 1 :name "Ivan"]] {:datahike.db/tx-middleware eat-meta})
            meta
            :has-meta)))

    ;; keep-meta-middleware should come first when you compose
    (is (not
          (->
            db
            (d/db-with [[:db/add 1 :name "Ivan"]]
                       {:datahike.db/tx-middleware (comp eat-meta mw/keep-meta-middleware)})
            meta
            :has-meta)))

    (is (->
          db
          (d/db-with [[:db/add 1 :name "Ivan"]]
                     {:datahike.db/tx-middleware (comp mw/keep-meta-middleware eat-meta)})
          meta
          :has-meta))

    (is (->
          db
          (d/db-with [[:db/add 1 :name "Ivan"]]
                     {:datahike.db/tx-middleware (comp mw/keep-meta-middleware (reset-meta {:different-meta true}))})

          meta
          (#(and (:has-meta %) (:different-meta %) (not (:unknown-meta %))))))))

(deftest test-schema
  (let [schema-mw {:datahike.db/tx-middleware mw/schema-middleware}
        db (-> (d/empty-db mw/bare-bones-schema)
               (d/db-with mw/enum-idents schema-mw)
               (d/db-with mw/schema-idents schema-mw)
               (d/db-with [{:db/id 100
                             :db/ident :name
                             :db/valueType :db.type/string}
                            {:db/id 102
                             :db/ident :aka
                             :db/cardinality :db.cardinality/many}
                            {:db/id 103
                             :db/ident :friend
                             :db/valueType [:db/ident :db.type/ref]}]
                          schema-mw)
               )]
    (is (= (get-in db [:schema :friend :db/valueType]) :db.type/ref))
    (is (= (get-in db [:schema :aka :db/cardinality]) :db.cardinality/many))))
