(ns benchmark.test.measure
  (:require [clojure.test :as t :refer [is deftest]]
            [benchmark.measure :as b]))

(def config {:config-name :all
             :output-format "edn"
             :data-types [:int :str]
             :iterations 1, 
             :data-found-opts :all
             :tx-entity-counts [0 10]
             :tag #{test} 
             :query :all 
             :db-entity-counts [0 10]})

(deftest connection-test 
  (let [measurements (b/get-measurements (assoc config :function :connection))]
    (is (= (set (keys (:time (first measurements)))) 
           #{:mean :median :std :count :observations}))
    (is (= (get-in (first measurements) [:time :count])
           1))
    (is (= (map :context measurements)
           '({:dh-config {:schema-flexibility :write
                          :keep-history? false
                          :index :datahike.index/persistent-set
                          :name "mem-set", :backend :mem}
              :function :connection
              :db-entities 0
              :db-datoms 0}
             {:dh-config {:schema-flexibility :write
                          :keep-history? false
                          :index :datahike.index/persistent-set
                          :name "mem-set", :backend :mem}
              :function :connection
              :db-entities 10
              :db-datoms 40}
             {:dh-config {:schema-flexibility :write
                          :keep-history? false, :index
                          :datahike.index/hitchhiker-tree
                          :name "mem-hht", :backend :mem}
              :function :connection
              :db-entities 0
              :db-datoms 0}
             {:dh-config {:schema-flexibility :write
                          :keep-history? false
                          :index :datahike.index/hitchhiker-tree
                          :name "mem-hht", :backend :mem}
              :function :connection, :db-entities 10, :db-datoms 40}
             {:dh-config {:schema-flexibility :write
                          :keep-history? false
                          :index :datahike.index/hitchhiker-tree
                          :name "file", :backend :file}
              :function :connection
              :db-entities 0
              :db-datoms 0}
             {:dh-config {:schema-flexibility :write
                          :keep-history? false
                          :index :datahike.index/hitchhiker-tree
                          :name "file", :backend :file}
              :function :connection
              :db-entities 10
              :db-datoms 40})))))

(deftest transaction-test
  (is (= 12 (count (b/get-measurements (assoc config :function :transaction))))))

(deftest query-test
  (is (= 198 (count (b/get-measurements (assoc config :function :query))))))

