(ns datahike.test.middleware.query-test
  (:require
   [clojure.test :refer [deftest is]]
   [datahike.api :as d]
   [datahike.middleware.query]
   [datahike.test.utils :as utils])
  (:import
   [clojure.lang ExceptionInfo]
   [java.util Date]))

(deftest timed-query-should-log-time-for-query-to-run
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? false
             :schema-flexibility :read
             :middleware {:query ['datahike.middleware.query/timed-query]}}
        conn (utils/setup-db cfg)]
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (let [out-str (with-out-str (d/q '[:find ?e :where [?e :name "Anna"]] @conn))]
      (is (re-find #"Query time" out-str))
      (is (re-find #":query" out-str))
      (is (re-find #":args" out-str))
      (is (re-find #"DB" out-str))
      (is (re-find #"[:find ?e :where [?e :name \"Anna\"]]" out-str))
      (is (re-find #":t" out-str)))
    (d/release conn)))

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
        conn (utils/setup-db cfg)]
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (let [before (Date.)
          _ (d/transact conn {:tx-data [{:name "Charlize"}]})
          out-str (with-out-str (d/q '[:find ?e :where [?e :name "Anna"]] (d/as-of @conn before)))]
      (is (re-find #"Query time" out-str))
      (is (re-find #":query" out-str))
      (is (re-find #":args" out-str))
      (is (re-find #"AsOfDB" out-str))
      (is (re-find #"[:find ?e :where [?e :name \"Anna\"]]" out-str))
      (is (re-find #":t" out-str)))
    (d/release conn)))

(deftest middleware-should-work-with-since
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? true
             :schema-flexibility :read
             :middleware {:query ['datahike.middleware.query/timed-query]}}
        conn (utils/setup-db cfg)]
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (let [before (Date.)
          _ (d/transact conn {:tx-data [{:name "Charlize"}]})
          out-str (with-out-str (d/q '[:find ?e :where [?e :name "Anna"]] (d/since @conn before)))]
      (is (re-find #"Query time" out-str))
      (is (re-find #":query" out-str))
      (is (re-find #":args" out-str))
      (is (re-find #"SinceDB" out-str))
      (is (re-find #"[:find ?e :where [?e :name \"Anna\"]]" out-str))
      (is (re-find #":t" out-str)))
    (d/release conn)))

(deftest middleware-should-work-with-history
  (let [cfg {:store {:backend :mem
                     :id "query-middleware"}
             :keep-history? true
             :schema-flexibility :read
             :middleware {:query ['datahike.middleware.query/timed-query]}}
        conn (utils/setup-db cfg)]
    (d/transact conn {:tx-data [{:name "Anna"}
                                {:name "Boris"}]})
    (let [_ (d/transact conn {:tx-data [{:name "Charlize"}]})
          out-str (with-out-str (d/q '[:find ?e :where [?e :name "Anna"]] (d/history @conn)))]
      (is (re-find #"Query time" out-str))
      (is (re-find #":query" out-str))
      (is (re-find #":args" out-str))
      (is (re-find #"HistoricalDB" out-str))
      (is (re-find #"[:find ?e :where [?e :name \"Anna\"]]" out-str))
      (is (re-find #":t" out-str)))
    (d/release conn)))
