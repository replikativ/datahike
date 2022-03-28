(ns benchmark.test.measure
  (:require [clojure.test :as t :refer [is deftest]]
            [benchmark.measure :as b]))

(def config {:output-format "edn"
             :data-types [:int :str]
             :iterations 1
             :data-found-opts :all
             :tx-entity-counts [0 10]
             :tag #{test} 
             :query :all 
             :db-entity-counts [0 10]})

(deftest transaction-test
  (is (= 24 (count (b/get-measurements (assoc config :function :transaction))))))

;; (+ (* 6 (* 2 8)) (* 6 (+ (* 2 8) (* 4 6) (* 4 2) 6))) = (+ 96 324) = 420
(deftest query-test
  (is (= 420 (count (b/get-measurements (assoc config :function :query))))))

(defn- dh-config
  ([cfg-name] (dh-config cfg-name true))
  ([cfg-name hist] (dh-config cfg-name hist :mem))
  ([cfg-name hist backend] (dh-config cfg-name hist backend :datahike.index/hitchhiker-tree))
  ([cfg-name hist backend index] {:schema-flexibility :write
                                  :keep-history? hist
                                  :index index
                                  :name cfg-name
                                  :backend backend}))

(defn- test-samples [dh-config]
  #{{:dh-config dh-config
     :function :connection
     :db-entities 0
     :db-datoms 0}
    {:dh-config dh-config
     :function :connection
     :db-entities 10
     :db-datoms 40}})

(deftest connection-test
  (let [measurements (b/get-measurements (assoc config :function :connection))]
    (is (= #{:mean :median :std :count :observations}
           (set (keys (:time (first measurements))))))
    (is (= 1
           (get-in (first measurements) [:time :count])))
    (is (= (clojure.set/union
            (test-samples (dh-config "file" false :file))
            (test-samples (dh-config "mem-hht" false))
            (test-samples (dh-config "mem-set" false :mem :datahike.index/persistent-set))
            (test-samples (dh-config "file-with-history" true :file))
            (test-samples (dh-config "mem-hht-with-history"))
            (test-samples (dh-config "mem-set-with-history" true :mem :datahike.index/persistent-set)))
           (set (map :context measurements))))))
