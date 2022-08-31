(ns benchmark.measure-test
  (:require [clojure.test :refer [is deftest testing]]
            [benchmark.measure :as b]
            [benchmark.config :as c]))

(def options {:output-format "edn"
              :db-samples 1
              :data-types [:int :str]
              :iterations 1,
              :data-found-opts :all
              :tx-entity-counts [1 10]
              :index :all
              :backend :all
              :history :all
              :schema :all
              :tag #{test}
              :function :all
              :query :all
              :cache [0]
              :db-entity-counts [1 10]})

;; -> (count configs) = (count cache) (count index) (count backend) (count history) (count schema) = 16
;; -> (count common-loop) = (* (count configs) db-samples iterations (count db-entity-counts)) = 32

(deftest transaction-test
  (is (= 64
         ; (* (count common-loop) (count tx-entities-count))
         ; (* 32                  2 )
         (count (b/get-measurements (assoc options :function :transaction))))))

(deftest query-test
  (testing "single query"
    (is (= 128
           ; (* (count common-loop) (count :data-types) (count :data-found-opts?))
           ; (* 32                  2                   2 )
           (count (b/get-measurements (assoc options :function :query
                                                     :query :simple-query))))))
  (testing "all queries"
    (is (= 1728
           ; (* (count common-loop) (+ (* (count var-queries)     (count :data-types) (count :data-found-opts?))))
           ;                           (* (count non-var-queries) (count :data-types))
           ;                           (count aggregate-queries)
           ;                           (* (count cache-check-queries) (count :data-types) (count :data-found-opts?))
           ; (* 32                  (+ (* 6                       2                    2)
           ;                           (* 8                       2)
           ;                           6
           ;                           (* 2                            2                   2))
           ; (* (32                 54))
           (count (b/get-measurements (assoc options :function :query
                                                     :cache [10])))))))

(deftest connection-test
  (let [measurements (b/get-measurements (assoc options :function :connection))]
    (is (= #{:mean :median :std :min :max :count :observations}
           (set (keys (:time (first measurements))))))
    (is (= 1
           (get-in (first measurements) [:time :count])))
    (is (= 32
           ; (count common-loop)
           (count measurements)))))

(deftest preset-configs
  (is (= '({:db-datoms 4
            :db-entities 1
            :dh-config {:backend :mem
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/persistent-set
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 40
            :db-entities 10
            :dh-config {:backend :mem
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/persistent-set
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection})
         (map :context (b/get-measurements (assoc options :config-name "mem-set"
                                                          :function :connection)))))
  (is (= '({:db-datoms 4
            :db-entities 1
            :dh-config {:backend :mem
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/persistent-set
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 40
            :db-entities 10
            :dh-config {:backend :mem
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/persistent-set
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 4
            :db-entities 1
            :dh-config {:backend :mem
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/hitchhiker-tree
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 40
            :db-entities 10
            :dh-config {:backend :mem
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/hitchhiker-tree
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 4
            :db-entities 1
            :dh-config {:backend :file
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/persistent-set
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 40
            :db-entities 10
            :dh-config {:backend :file
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/persistent-set
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 4
            :db-entities 1
            :dh-config {:backend :file
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/hitchhiker-tree
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection}
           {:db-datoms 40
            :db-entities 10
            :dh-config {:backend :file
                        :search-cache-size 0
                        :store-cache-size 1
                        :index :datahike.index/hitchhiker-tree
                        :keep-history? false
                        :schema-flexibility :write}
            :function :connection})
         (map :context (b/get-measurements (assoc options :function :connection)
                                           (vals c/named-db-configs))))))
