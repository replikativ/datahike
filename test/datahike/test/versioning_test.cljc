(ns datahike.test.versioning-test
  (:require #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer [is deftest testing]])
            [datahike.experimental.versioning :refer
             [branch-history branch! delete-branch! merge! force-branch! branch-as-db parent-commit-ids
              commit-id commit-as-db]]
            [datahike.db.utils :refer [db?]]
            [datahike.api :as d]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]))

(deftest datahike-versioning-test
  (testing "Testing versioning functionality."
    (let [cfg {:store              {:backend :file
                                    :path    "/tmp/dh-versioning-test"
                                    :id #uuid "1e510000-0000-0000-0000-00000000001e"}
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
      (testing "Test branching."
        (branch! conn :db :foo)
        (is (= (k/get store :branches nil {:sync? true})
               #{:db :foo})))
      (testing "Test merging."
        (let [foo-conn (d/connect (assoc cfg :branch :foo))]
          (d/transact foo-conn [{:age 42}])
          ;; extracted data from foo and decide to merge it into :db
          (merge! conn #{:foo} [{:age 42}])
          (d/release foo-conn))
        (is (= 4 (count (<?? S (branch-history conn)))))
        (is (= 2 (count (parent-commit-ids @conn)))))
      (testing "Force branch."
        (force-branch! @conn :foo2 #{:foo})
        (is (db? (branch-as-db store :foo2))))
      (testing "Load different references on current db."
        (is (= (commit-as-db store (commit-id @conn))
               (branch-as-db store :db)
               @conn)))
      (testing "Check branch history."
        (let [conn-foo2 (d/connect (assoc cfg :branch :foo2))]
          (is (= 4 (count (<?? S (branch-history conn-foo2)))))
          (d/release conn-foo2)))
      (testing "Delete branch."
        (delete-branch! conn :foo)
        (is (= (k/get store :branches nil {:sync? true})
               #{:db :foo2})))
      (d/release conn))))
