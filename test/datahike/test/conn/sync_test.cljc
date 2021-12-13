(ns datahike.test.conn.sync-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.test.utils :as utils])
  (:import [clojure.lang ExceptionInfo]))

(deftest sync?-setting
  (let [cfg {:store              {:backend :mem
                                  :id      "sync?-setting-test"}
             :schema-flexibility :read
             :keep-history?      false
             :attribute-refs?    false}
        conn-cfg {:tx/sync? false}]
    (testing "default sync? setting"
      (let [conn (utils/setup-db cfg)]
        (is (= true
               (-> conn meta :tx/sync?)))))
    (testing "manual sync? setting via configuration"
      (let [conn (utils/setup-db cfg conn-cfg)]
        (is (= false
               (-> conn meta :tx/sync?)))))
    (testing "manual sync? setting via start-sync"
      (let [conn (utils/setup-db cfg)]
        (d/start-sync conn)
        (is (= true
               (-> conn meta :tx/sync?)))))
    (testing "manual sync? setting via stop-sync"
      (let [conn (utils/setup-db (assoc-in cfg [:connection :sync?] true))]
        (d/stop-sync conn)
        (is (= false
               (-> conn meta :tx/sync?)))))
    (testing "switch between settings via start-sync and stop-sync"
      (let [conn (utils/setup-db cfg)]
        (testing "starting sync"
          (d/start-sync conn)
          (is (= true
                 (-> conn meta :tx/sync?))))
        (testing "stopping sync"
          (d/stop-sync conn)
          (is (= false
                 (-> conn meta :tx/sync?))))
        (testing "starting sync after stop"
          (d/start-sync conn)
          (is (= true
                 (-> conn meta :tx/sync?))))))
    (testing "wrong configuration data"
      (is (thrown-with-msg? ExceptionInfo
                            #"Bad connection options 666 - failed: boolean\? in: \[:tx\/sync\?\] at: \[:tx\/sync\?\] spec: :tx\/sync\?"
                            (utils/setup-db cfg {:tx/sync? 666}))))))

(deftest sync-transactions
  (let [cfg          {:store              {:backend :mem
                                           :id      "sync?-setting-test"}
                      :schema-flexibility :read
                      :keep-history?      false
                      :attribute-refs?    false}
        query        '[:find ?n
                       :where
                       [?e :name ?n]]
        query-result #{["Otto"] ["Brunhilde"]}
        init-data    (fn [conn]
                       (d/transact conn [{:name "Otto"}
                                         {:name "Brunhilde"}]))]
    (testing "transact behavior with explicit active sync? setting"
      (let [conn (utils/setup-db (assoc-in cfg [:connection :sync?] true))]
        (init-data conn)
        (is (= query-result
               (d/q query @conn)))))
    (testing "transact behavior with in-active sync? and no sync! before re-connect"
      (let [conn (utils/setup-db cfg {:tx/sync? false})]
        (init-data conn)
        (is (= query-result
               (d/q query @conn)))
        (is (= #{}
               (d/q query @(d/connect cfg))))))
    (testing "transact behavior with in-active sync? and sync! before re-connect"
      (let [conn (utils/setup-db cfg)]
        (init-data conn)
        (is (= query-result
               (d/q query @conn)))
        (d/sync! conn)
        (is (= query-result
               (d/q query @(d/connect cfg))))))
    (testing "transact behavior with in-active sync? , tart-sync before re-connect"
      (let [conn (utils/setup-db cfg)]
        (init-data conn)
        (is (= query-result
               (d/q query @conn)))
        (d/start-sync conn)
        (d/transact conn [{:name "Wilhelm"}])
        (is (= (conj query-result ["Wilhelm"])
               (d/q query @(d/connect cfg))))))
    (testing "transact behavior with active sync?, stop-sync before re-connect"
      (let [conn (utils/setup-db (assoc-in cfg [:connection :sync?] true))]
        (init-data conn)
        (is (= query-result
               (d/q query @conn)))
        (d/stop-sync conn)
        (d/transact conn [{:name "Wilhelm"}])
        (is (= query-result
               (d/q query @(d/connect cfg))))))
    (testing "transact behavior with explicit active sync?, stop-sync, syn! before re-connect"
      (let [conn (utils/setup-db (assoc-in cfg [:connection :sync?] true))]
        (init-data conn)
        (is (= query-result
               (d/q query @conn)))
        (d/stop-sync conn)
        (d/transact conn [{:name "Wilhelm"}])
        (d/sync! conn)
        (is (= (conj query-result ["Wilhelm"])
               (d/q query @(d/connect cfg))))))))
