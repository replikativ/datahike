(ns datahike.test.versioning-test
  (:require #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer [is deftest testing]])
            [datahike.experimental.versioning :refer
             [branch-history branch! delete-branch! merge! force-branch!]]
            [datahike.api :as d]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]))

(deftest datahike-versioning-test
  (testing "Testing versioning functionality."
    (let [cfg {:store              {:backend :file
                                    :path    "/tmp/dh-versioning-test"}
               :keep-history?      true
               :schema-flexibility :write
               :index              :datahike.index/persistent-set}
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          schema [{:db/ident       :age
                   :db/cardinality :db.cardinality/one
                   :db/valueType   :db.type/long}]
          _ (d/transact conn schema)
          store (:store @conn)]
      (branch! conn :db :foo)
      (is (= (k/get store :branches nil {:sync? true})
             #{:db :foo}))
      (let [foo-conn (d/connect (assoc cfg :branch :foo))]
        (d/transact foo-conn [{:age 42}])
        ;; extracted data from foo and decide to merge it into :db
        (merge! conn #{:foo} [{:age 42}]))
      (is (= 4 (count (<?? S (branch-history conn)))))
      (is (= 2 (count (k/get-in store [:db :meta :datahike/parents] nil {:sync? true}))))
      (force-branch! @conn :foo2 #{:foo})
      (is (= 4 (count (<?? S (branch-history (d/connect (assoc cfg :branch :foo2)))))))
      (delete-branch! conn :foo)
      (is (= (k/get store :branches nil {:sync? true})
             #{:db :foo2})))))
