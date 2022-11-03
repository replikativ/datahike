(ns datahike.middleware.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]
            [datahike.middleware.query]
            [taoensso.timbre :as timbre])
  (:import [java.util Date]
           [clojure.lang ExceptionInfo]))

(deftest timed-query-should-log-time-for-query-to-run
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? false
             :schema-flexibility :read
             :middleware {:query ['datahike.middleware.query/timed-query]}}
        conn (utils/setup-db cfg)
        log-state (atom [])]
    (timbre/merge-config! {:appenders {:my-appender {:enabled? true
                                                     :fn (fn [data] (swap! log-state conj data))}}})
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (d/q '[:find ?e :where [?e :name "Anna"]] @conn)
    (let [[text data] (-> @log-state second :vargs)]
      (is (= "Query time:"
             text))
      (is (number? (:t data)))
      (is (= {:q      {:query '[:find
                                ?e
                                :where
                                [?e
                                 :name
                                 "Anna"]]}}
             (-> data
                 (dissoc :t)
                 (dissoc :inputs)
                 (update-in [:q] dissoc :args)))))))

(deftest invalid-middleware-should-be-caught-on-connection
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? false
             :schema-flexibility :read
             :middleware {:query "this is neither a function nor a vector!"}}]
    (is (thrown-with-msg? ExceptionInfo #"Invalid Datahike configuration." (utils/setup-db cfg)))))

(deftest middleware-should-work-with-as-of-db
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? true
             :schema-flexibility :read
             :middleware {:query ['datahike.middleware.query/timed-query]}}
        conn (utils/setup-db cfg)

        log-state (atom [])]
    (timbre/merge-config! {:appenders {:my-appender {:enabled? true
                                                     :fn (fn [data] (swap! log-state conj data))}}})
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (let [before (Date.)
          _ (d/transact conn {:tx-data [{:name "Charlize"}]})
          _ (d/q '[:find ?e :where [?e :name "Anna"]] (d/as-of @conn before))
          [text data] (-> @log-state last :vargs)]
      (is (= "Query time:"
             text))
      (is (number? (:t data)))
      (is (is (= "AsOfDB"
                 (re-find #"AsOfDB" (get-in data [:q :args])))))
      (is (= {:q {:query '[:find
                           ?e
                           :where
                           [?e
                            :name
                            "Anna"]]}}
             (-> data
                 (dissoc :t)
                 (dissoc :inputs)
                 (update :q dissoc :args)))))))

(deftest middleware-should-work-with-since
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? true
             :schema-flexibility :read
             :middleware {:query ['datahike.middleware.query/timed-query]}}
        conn (utils/setup-db cfg)

        log-state (atom [])]
    (timbre/merge-config! {:appenders {:my-appender {:enabled? true
                                                     :fn (fn [data] (swap! log-state conj data))}}})
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (let [before (Date.)
          _ (d/transact conn {:tx-data [{:name "Charlize"}]})
          _ (d/q '[:find ?e :where [?e :name "Anna"]] (d/since @conn before))
          [text data] (-> @log-state last :vargs)]
      (is (= "Query time:"
             text))
      (is (number? (:t data)))
      (is (= "SinceDB"
             (re-find #"SinceDB" (get-in data [:q :args]))))
      (is (= {:q {:query '[:find
                           ?e
                           :where
                           [?e
                            :name
                            "Anna"]]}}
             (-> data
                 (dissoc :t)
                 (dissoc :inputs)
                 (update :q dissoc :args)))))))

(deftest middleware-should-work-with-history
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? true
             :schema-flexibility :read
             :middleware {:query ['datahike.middleware.query/timed-query]}}
        conn (utils/setup-db cfg)

        log-state (atom [])]
    (timbre/merge-config! {:appenders {:my-appender {:enabled? true
                                                     :fn (fn [data] (swap! log-state conj data))}}})
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (let [_ (d/transact conn {:tx-data [{:name "Charlize"}]})
          _ (d/q '[:find ?e :where [?e :name "Anna"]] (d/history @conn))
          [text data] (-> @log-state last :vargs)]
      (is (= "Query time:"
             text))
      (is (number? (:t data)))
      (is (= "HistoricalDB"
             (re-find #"HistoricalDB" (get-in data [:q :args]))))
      (is (= {:q {:query '[:find
                           ?e
                           :where
                           [?e
                            :name
                            "Anna"]]}}
             (-> data
                 (dissoc :t)
                 (dissoc :inputs)
                 (update :q dissoc :args)))))))