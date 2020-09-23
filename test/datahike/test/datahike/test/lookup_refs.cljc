(ns datahike.test.lookup-refs
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datahike.core :as d]
    [datahike.test.core :as tdc])
    #?(:clj
      (:import [clojure.lang ExceptionInfo])))



(deftest test-lookup-refs
  (let [db (d/db-with (d/empty-db {:name  { :db/unique :db.unique/identity }
                                   :email { :db/unique :db.unique/value }})
                      [{:db/id tdc/e1 :name "Ivan" :email "@1" :age 35}
                       {:db/id tdc/e2 :name "Petr" :email "@2" :age 22}])]
    
    (are [eid res] (= (tdc/entity-map db eid) res)
      [:name "Ivan"]   {:db/id tdc/e1 :name "Ivan" :email "@1" :age 35}
      [:email "@1"]    {:db/id tdc/e1 :name "Ivan" :email "@1" :age 35}
      [:name "Sergey"] nil
      [:name nil]      nil)
    
    (are [eid msg] (thrown-msg? msg (d/entity db eid))
      [:name]     "Lookup ref should contain 2 elements: [:name]"
      [:name 1 2] "Lookup ref should contain 2 elements: [:name 1 2]"
      [:age 10]   "Lookup ref attribute should be marked as :db/unique: [:age 10]")))

(deftest test-lookup-refs-transact
  (let [db (d/db-with (d/empty-db {:name    { :db/unique :db.unique/identity }
                                   :friend  { :db/valueType :db.type/ref }})
                      [{:db/id tdc/e1 :name "Ivan"}
                       {:db/id tdc/e2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))
      ;; Additions
      [[:db/add [:name "Ivan"] :age 35]]
      {:db/id tdc/e1 :name "Ivan" :age 35}
      
      [{:db/id [:name "Ivan"] :age 35}]
      {:db/id tdc/e1 :name "Ivan" :age 35}
         
      [[:db/add tdc/e1 :friend [:name "Petr"]]]
      {:db/id tdc/e1 :name "Ivan" :friend {:db/id tdc/e2}}

      [[:db/add tdc/e1 :friend [:name "Petr"]]]
      {:db/id tdc/e1 :name "Ivan" :friend {:db/id tdc/e2}}
         
      [{:db/id tdc/e1 :friend [:name "Petr"]}]
      {:db/id tdc/e1 :name "Ivan" :friend {:db/id tdc/e2}}
      
      [{:db/id tdc/e2 :_friend [:name "Ivan"]}]
      {:db/id tdc/e1 :name "Ivan" :friend {:db/id tdc/e2}}
      
      ;; lookup refs are resolved at intermediate DB value
      [[:db/add tdc/e3 :name "Oleg"]
       [:db/add tdc/e1 :friend [:name "Oleg"]]]
      {:db/id tdc/e1 :name "Ivan" :friend {:db/id tdc/e3}}
      
      ;; CAS
      [[:db.fn/cas [:name "Ivan"] :name "Ivan" "Oleg"]]
      {:db/id tdc/e1 :name "Oleg"}
      
      [[:db/add tdc/e1 :friend tdc/e1]
       [:db.fn/cas tdc/e1 :friend [:name "Ivan"] tdc/e2]]
      {:db/id tdc/e1 :name "Ivan" :friend {:db/id tdc/e2}}
         
      [[:db/add tdc/e1 :friend tdc/e1]
       [:db.fn/cas tdc/e1 :friend tdc/e1 [:name "Petr"]]]
      {:db/id tdc/e1 :name "Ivan" :friend {:db/id tdc/e2}}
         
      ;; Retractions
      [[:db/add tdc/e1 :age 35]
       [:db/retract [:name "Ivan"] :age 35]]
      {:db/id tdc/e1 :name "Ivan"}
      
      [[:db/add tdc/e1 :friend tdc/e2]
       [:db/retract tdc/e1 :friend [:name "Petr"]]]
      {:db/id tdc/e1 :name "Ivan"}
         
      [[:db/add tdc/e1 :age 35]
       [:db.fn/retractAttribute [:name "Ivan"] :age]]
      {:db/id tdc/e1 :name "Ivan"}
         
      [[:db.fn/retractEntity [:name "Ivan"]]]
      {:db/id tdc/e1})
    
    (are [tx msg] (thrown-msg? msg (d/db-with db tx))
      [{:db/id [:name "Oleg"], :age 10}]
      "Nothing found for entity id [:name \"Oleg\"]"
         
      [[:db/add [:name "Oleg"] :age 10]]
      "Nothing found for entity id [:name \"Oleg\"]")
    ))

(deftest test-lookup-refs-transact-multi
  (let [db (d/db-with (d/empty-db {:name    { :db/unique :db.unique/identity }
                                   :friends { :db/valueType :db.type/ref
                                              :db/cardinality :db.cardinality/many }})
                      [{:db/id tdc/e1 :name "Ivan"}
                       {:db/id tdc/e2 :name "Petr"}
                       {:db/id tdc/e3 :name "Oleg"}
                       {:db/id tdc/e4 :name "Sergey"}])]
    (are [tx res] (= (tdc/entity-map (d/db-with db tx) 1) res)
      ;; Additions
      [[:db/add tdc/e1 :friends [:name "Petr"]]]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2}}}

      [[:db/add tdc/e1 :friends [:name "Petr"]]
       [:db/add tdc/e1 :friends [:name "Oleg"]]]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2} {:db/id tdc/e3}}}
         
      [{:db/id tdc/e1 :friends [:name "Petr"]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2}}}

      [{:db/id tdc/e1 :friends [[:name "Petr"]]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2}}}
         
      [{:db/id tdc/e1 :friends [[:name "Petr"] [:name "Oleg"]]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2} {:db/id tdc/e3}}}

      [{:db/id tdc/e1 :friends [tdc/e2 [:name "Oleg"]]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2} {:db/id tdc/e3}}}

      [{:db/id tdc/e1 :friends [[:name "Petr"] tdc/e3]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2} {:db/id tdc/e3}}}
         
      ;; reverse refs
      [{:db/id tdc/e2 :_friends [:name "Ivan"]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2}}}

      [{:db/id tdc/e2 :_friends [[:name "Ivan"]]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2}}}

      [{:db/id tdc/e2 :_friends [[:name "Ivan"] [:name "Oleg"]]}]
      {:db/id tdc/e1 :name "Ivan" :friends #{{:db/id tdc/e2}}}
    )))

(deftest lookup-refs-index-access
  (let [db (d/db-with (d/empty-db {:name    { :db/unique :db.unique/identity }
                                   :friends { :db/valueType :db.type/ref
                                              :db/cardinality :db.cardinality/many}})
                      [{:db/id tdc/e1 :name "Ivan" :friends [tdc/e2 tdc/e3]}
                       {:db/id tdc/e2 :name "Petr" :friends tdc/e3}
                       {:db/id tdc/e3 :name "Oleg"}])]
     (are [index attrs datoms] (= (map (juxt :e :a :v) (apply d/datoms db index attrs)) datoms)
       :eavt [[:name "Ivan"]]
       [[tdc/e1 :friends tdc/e2] [tdc/e1 :friends tdc/e3] [tdc/e1 :name "Ivan"]]
       
       :eavt [[:name "Ivan"] :friends]
       [[tdc/e1 :friends tdc/e2] [tdc/e1 :friends tdc/e3]]
          
       :eavt [[:name "Ivan"] :friends [:name "Petr"]]
       [[tdc/e1 :friends tdc/e2]]
       
       :aevt [:friends [:name "Ivan"]]
       [[tdc/e1 :friends tdc/e2] [tdc/e1 :friends tdc/e3]]
          
       :aevt [:friends [:name "Ivan"] [:name "Petr"]]
       [[tdc/e1 :friends tdc/e2]]
       
       :avet [:friends [:name "Oleg"]]
       [[tdc/e1 :friends tdc/e3] [tdc/e2 :friends tdc/e3]]
       
       :avet [:friends [:name "Oleg"] [:name "Ivan"]]
       [[tdc/e1 :friends tdc/e3]])
    
     (are [index attrs resolved-attrs] (= (vec (apply d/seek-datoms db index attrs))
                                          (vec (apply d/seek-datoms db index resolved-attrs)))
       :eavt [[:name "Ivan"]] [tdc/e1]
       :eavt [[:name "Ivan"] :name] [tdc/e1 :name]
       :eavt [[:name "Ivan"] :friends [:name "Oleg"]] [tdc/e1 :friends tdc/e3]
       
       :aevt [:friends [:name "Petr"]] [:friends tdc/e2]
       :aevt [:friends [:name "Ivan"] [:name "Oleg"]] [:friends tdc/e1 tdc/e3]
       
       :avet [:friends [:name "Oleg"]] [:friends tdc/e3]
       :avet [:friends [:name "Oleg"] [:name "Petr"]] [:friends tdc/e3 tdc/e2]
      )
    
    (are [attr start end datoms] (= (map (juxt :e :a :v) (d/index-range db attr start end)) datoms)
       :friends [:name "Oleg"] [:name "Oleg"]
       [[tdc/e1 :friends tdc/e3] [tdc/e2 :friends tdc/e3]]
       
       :friends [:name "Petr"] [:name "Petr"]
       [[tdc/e1 :friends tdc/e2]]
       
       :friends [:name "Petr"] [:name "Oleg"]
       [[tdc/e1 :friends tdc/e2] [tdc/e1 :friends tdc/e3] [tdc/e2 :friends tdc/e3]])
))

(deftest test-lookup-refs-query
  (let [schema {:name   { :db/unique :db.unique/identity }
                :friend { :db/valueType :db.type/ref }}
        db (d/db-with (d/empty-db schema)
                    [{:db/id tdc/e1 :id tdc/e1 :name "Ivan" :age 11 :friend tdc/e2}
                     {:db/id tdc/e2 :id tdc/e2 :name "Petr" :age 22 :friend tdc/e3}
                     {:db/id tdc/e3 :id tdc/e3 :name "Oleg" :age 33 }])]
    (is (= (set (d/q '[:find ?e ?v
                       :in $ ?e
                       :where [?e :age ?v]]
                     db [:name "Ivan"]))
           #{[[:name "Ivan"] 11]}))
    
    (is (= (set (d/q '[:find [?v ...]
                       :in $ [?e ...]
                       :where [?e :age ?v]]
                     db [[:name "Ivan"] [:name "Petr"]]))
           #{11 22}))
    
    (is (= (set (d/q '[:find [?e ...]
                       :in $ ?v
                       :where [?e :friend ?v]]
                     db [:name "Petr"]))
           #{tdc/e1}))
    
    (is (= (set (d/q '[:find [?e ...]
                       :in $ [?v ...]
                       :where [?e :friend ?v]]
                     db [[:name "Petr"] [:name "Oleg"]]))
           #{tdc/e1 tdc/e2}))
    
    (is (= (d/q '[:find ?e ?v
                  :in $ ?e ?v
                  :where [?e :friend ?v]]
                db [:name "Ivan"] [:name "Petr"])
           #{[[:name "Ivan"] [:name "Petr"]]}))
    
    (is (= (d/q '[:find ?e ?v
                  :in $ [?e ...] [?v ...]
                  :where [?e :friend ?v]]
                db [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]]
                   [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]])
           #{[[:name "Ivan"] [:name "Petr"]]
             [[:name "Petr"] [:name "Oleg"]]}))

    ;; https://github.com/tonsky/datahike/issues/214
    (is (= (d/q '[:find ?e
                  :in $ [?e ...]
                  :where [?e :friend tdc/e3]]
                db [tdc/e1 tdc/e2 tdc/e3 "A"])
           #{[2]}))
    
    (let [db2 (d/db-with (d/empty-db schema)
                [{:db/id tdc/e3 :name "Ivan" :id tdc/e4}
                 {:db/id tdc/e1 :name "Petr" :id tdc/e1}
                 {:db/id tdc/e2 :name "Oleg" :id tdc/e2}])]
      (is (= (d/q '[:find ?e ?e1 ?e2
                    :in $1 $2 [?e ...]
                    :where [$1 ?e :id ?e1]
                           [$2 ?e :id ?e2]]
                  db db2 [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]])
             #{[[:name "Ivan"] tdc/e1 tdc/e3]
               [[:name "Petr"] tdc/e2 tdc/e1]
               [[:name "Oleg"] tdc/e3 tdc/e2]})))
    
    (testing "inline refs"
      (is (= (d/q '[:find ?v
                    :where [[:name "Ivan"] :friend ?v]]
                  db)
             #{[2]}))
      
      (is (= (d/q '[:find ?e
                    :where [?e :friend [:name "Petr"]]]
                  db)
             #{[1]}))
      
      (is (thrown-msg? "Nothing found for entity id [:name \"Valery\"]"
            (d/q '[:find ?e
                   :where [[:name "Valery"] :friend ?e]]
                  db)))

      )
))