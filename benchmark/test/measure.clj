(ns benchmark.test.measure
  (:require [clojure.test :as t :refer [is deftest]]
            [benchmark.measure :as b]))

(def config {:output-format "edn"
             :data-types [:int :str]
             :iterations 1, 
             :data-found-opts :all
             :tx-entity-counts [0 10]
             :tag #{test} 
             :query :all 
             :db-entity-counts [0 10]})

(deftest transaction-test
  (is (= 12 (count (b/get-measurements (assoc config :function :transaction))))))

;; (+ (* 3 (* 2 8)) (* 3 (+ (* 2 8) (* 4 6) (* 4 2) 6))) = (+ 48 162) = 210
(deftest query-test
  (is (= 210 (count (b/get-measurements (assoc config :function :query))))))

(deftest connection-test
  (let [measurements (b/get-measurements (assoc config :function :connection))]
    (is (= #{:mean :median :std :count :observations}
           (set (keys (:time (first measurements))))))
    (is (= 1
           (get-in (first measurements) [:time :count])))
    (is (= '({:dh-config {:schema-flexibility :write
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
              :db-datoms 40}
             {:db-datoms   0
              :db-entities 0
              :dh-config   {:backend            :file
                            :index              :datahike.index/hitchhiker-tree
                            :keep-history?      true
                            :name               "file-with-history"
                            :schema-flexibility :write}
              :function    :connection}
             {:db-datoms   40
              :db-entities 10
              :dh-config   {:backend            :file
                            :index              :datahike.index/hitchhiker-tree
                            :keep-history?      true
                            :name               "file-with-history"
                            :schema-flexibility :write}
              :function    :connection}
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
              :db-datoms 40})
           (map :context measurements)))))
