(ns datahike.test.explode
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datahike.core :as d]
    [datahike.test.core :as tdc]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-explode
  (doseq [coll [["Devil" "Tupen"]
                #{"Devil" "Tupen"}
                '("Devil" "Tupen")
                (to-array ["Devil" "Tupen"])]]
    (testing coll
      (let [conn (d/create-conn { :aka { :db/cardinality :db.cardinality/many }
                                 :also { :db/cardinality :db.cardinality/many} })]
        (d/transact! conn [{:db/id -1
                            :name  "Ivan"
                            :age   16
                            :aka   coll
                            :also  "ok"}])
        (is (= (d/q '[:find  ?n ?a
                      :where [e1 :name ?n]
                      [e1 :age ?a]] @conn)
               #{["Ivan" 16]}))
        (is (= (d/q '[:find  ?v
                      :where [e1 :also ?v]] @conn)
               #{["ok"]}))
        (is (= (d/q '[:find  ?v
                      :where [tdc/e1 :aka ?v]] @conn)
               #{["Devil"] ["Tupen"]}))))))

(deftest test-explode-ref
  (let [db0 (d/empty-db { :children { :db/valueType :db.type/ref
                                      :db/cardinality :db.cardinality/many } })]
    (let [db (d/db-with db0 [{:db/id -1, :name "Ivan", :children [-2 -3]}
                             {:db/id -2, :name "Petr"} 
                             {:db/id -3, :name "Evgeny"}])]
      (is (= (d/q '[:find ?n
                    :where [_ :children ?e]
                           [?e :name ?n]] db)
             #{["Petr"] ["Evgeny"]})))
    
    (let [db (d/db-with db0 [{:db/id -1, :name "Ivan"}
                             {:db/id -2, :name "Petr", :_children -1} 
                             {:db/id -3, :name "Evgeny", :_children -1}])]
      (is (= (d/q '[:find ?n
                    :where [_ :children ?e]
                           [?e :name ?n]] db)
             #{["Petr"] ["Evgeny"]})))
    
    (is (thrown-msg? "Bad attribute :_parent: reverse attribute name requires {:db/valueType :db.type/ref} in schema"
      (d/db-with db0 [{:name "Sergey" :_parent tdc/e1}])))))

(deftest test-explode-nested-maps
  (let [schema { :profile { :db/valueType :db.type/ref }}
        db     (d/empty-db schema)]
    (are [tx res] (= (d/q '[:find ?e ?a ?v
                            :where [?e ?a ?v]]
                          (d/db-with db tx)) res)
      [ {:db/id tdc/e5 :name "Ivan" :profile {:db/id tdc/e7 :email "@2"}} ]
      #{ [tdc/e5 :name "Ivan"] [tdc/e5 :profile tdc/e7] [tdc/e7 :email "@2"] }
         
      [ {:name "Ivan" :profile {:email "@2"}} ]
      #{ [tdc/e1 :name "Ivan"] [tdc/e1 :profile tdc/e2] [tdc/e2 :email "@2"] }
         
      [ {:profile {:email "@2"}} ] ;; issue #59
      #{ [tdc/e1 :profile tdc/e2] [tdc/e2 :email "@2"] }
         
      [ {:email "@2" :_profile {:name "Ivan"}} ]
      #{ [tdc/e1 :email "@2"] [tdc/e2 :name "Ivan"] [tdc/e2 :profile tdc/e1] }
    ))
  
  (testing "multi-valued"
    (let [schema { :profile { :db/valueType :db.type/ref
                              :db/cardinality :db.cardinality/many }}
          db     (d/empty-db schema)]
      (are [tx res] (= (d/q '[:find ?e ?a ?v
                              :where [?e ?a ?v]]
                            (d/db-with db tx)) res)
        [ {:db/id tdc/e5 :name "Ivan" :profile {:db/id tdc/e7 :email "@2"}} ]
        #{ [tdc/e5 :name "Ivan"] [tdc/e5 :profile tdc/e7] [tdc/e7 :email "@2"] }
           
        [ {:db/id tdc/e5 :name "Ivan" :profile [{:db/id tdc/e7 :email "@2"} {:db/id tdc/e8 :email "@3"}]} ]
        #{ [tdc/e5 :name "Ivan"] [tdc/e5 :profile tdc/e7] [tdc/e7 :email "@2"] [tdc/e5 :profile tdc/e8] [tdc/e8 :email "@3"] }

        [ {:name "Ivan" :profile {:email "@2"}} ]
        #{ [tdc/e1 :name "Ivan"] [tdc/e1 :profile tdc/e2] [tdc/e2 :email "@2"] }

        [ {:name "Ivan" :profile [{:email "@2"} {:email "@3"}]} ]
        #{ [tdc/e1 :name "Ivan"] [tdc/e1 :profile tdc/e2] [tdc/e2 :email "@2"] [tdc/e1 :profile tdc/e3] [tdc/e3 :email "@3"] }
           
        [ {:email "@2" :_profile {:name "Ivan"}} ]
        #{ [tdc/e1 :email "@2"] [tdc/e2 :name "Ivan"] [tdc/e2 :profile tdc/e1] }

        [ {:email "@2" :_profile [{:name "Ivan"} {:name "Petr"} ]} ]
        #{ [tdc/e1 :email "@2"] [tdc/e2 :name "Ivan"] [tdc/e2 :profile tdc/e1] [tdc/e3 :name "Petr"] [tdc/e3 :profile tdc/e1] }
      ))))

(deftest test-circular-refs
  (let [schema {:comp {:db/valueType   :db.type/ref
                       :db/cardinality :db.cardinality/many
                       :db/isComponent true}}
        db     (d/db-with (d/empty-db schema)
                 [{:db/id tdc/e1, :comp [{:name "C"}]}])]
    (is (= (mapv (juxt :e :a :v) (d/datoms db :eavt))
           [ [ tdc/e1 :comp tdc/e2  ]
             [ tdc/e2 :name "C"] ])))
  
  (let [schema {:comp {:db/valueType   :db.type/ref
                       :db/cardinality :db.cardinality/many}}
        db     (d/db-with (d/empty-db schema)
                 [{:db/id tdc/e1, :comp [{:name "C"}]}])]
    (is (= (mapv (juxt :e :a :v) (d/datoms db :eavt))
           [ [ tdc/e1 :comp tdc/e2  ]
             [ tdc/e2 :name "C"] ])))
  
  (let [schema {:comp {:db/valueType   :db.type/ref
                       :db/isComponent true}}
        db     (d/db-with (d/empty-db schema)
                 [{:db/id tdc/e1, :comp {:name "C"}}])]
    (is (= (mapv (juxt :e :a :v) (d/datoms db :eavt))
           [ [ tdc/e1 :comp tdc/e2  ]
             [ tdc/e2 :name "C"] ])))
  
  (let [schema {:comp {:db/valueType   :db.type/ref}}
        db     (d/db-with (d/empty-db schema)
                 [{:db/id tdc/e1, :comp {:name "C"}}])]
    (is (= (mapv (juxt :e :a :v) (d/datoms db :eavt))
           [ [ tdc/e1 :comp tdc/e2  ]
             [ tdc/e2 :name "C"] ]))))
 
