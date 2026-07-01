(ns datahike.integration-test.tiered-store-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [konserve.store :as ks]))

(def store-id #uuid "0e7000b9-0000-0000-0000-000000000001")

(def frontend-cfg {:backend :memory :id store-id})

(def tiered-cfg
  {:store {:backend :tiered
           :id store-id
           :frontend-config frontend-cfg
           :backend-config {:backend :file
                            :path "/tmp/datahike-tiered-store-test"
                            :id store-id}}
   :keep-history? false
   :schema-flexibility :read
   :index :datahike.index/persistent-set})

(deftest ^:integration frontend-deletion-recovery-test
  (d/delete-database tiered-cfg)

  (d/create-database tiered-cfg)
  (let [conn (d/connect tiered-cfg)]
    (d/transact conn [{:db/ident :name
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:name "Alice"}])
    (d/release conn))

  (ks/delete-store frontend-cfg)

  (let [conn (d/connect tiered-cfg)]
    (is (= #{["Alice"]} (d/q '[:find ?n :where [_ :name ?n]] @conn)))
    (d/release conn))

  (d/delete-database tiered-cfg))
