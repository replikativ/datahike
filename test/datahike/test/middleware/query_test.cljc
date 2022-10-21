(ns datahike.test.middleware.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]
            [datahike.middleware.query :refer [timed]]
            [taoensso.timbre :as timbre]))

(deftest timed-should-log-time-for-query-to-run
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? false
             :schema-flexibility :read
             :middleware {:query timed}}
        conn (utils/setup-db cfg)
        log-state (atom [])]
    (timbre/with-level :debug
    (timbre/merge-config! {:appenders {:my-appender {:enabled? true
                                                     :fn (fn [data] (swap! log-state conj data))}}})
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (d/q '[:find ?e :where [?e :name "Anna"]] @conn)
    (let [[text data] (-> @log-state second :vargs)]
      (is (= "Query time:"
             text))
      (is (number? (:t data)))
      (is (= {:inputs [nil]
              :q      {:query '[:find
                               ?e
                               :where
                               [?e
                                :name
                                "Anna"]]}}
             (-> data
                 (dissoc :t)
                 (update-in [:q] dissoc :args))))))))

(deftest invalid-middleware-should-be-caught-on-connection
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? false
             :schema-flexibility :read
             :middleware {:query "no-fun"}}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid Datahike configuration." (utils/setup-db cfg)))))
