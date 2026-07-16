(ns datahike.kabel.walker-test
  "The datahike konserve-sync walker follows `:db.type/store-ref` values, so a blob
   referenced by a datom replicates with the datoms that name it — and lands in the
   NODE portion of the walk (ahead of the mutable pointer cells), so a subscriber
   never applies a head that points at an object it has not yet received."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.blob :as blob]
            [datahike.kabel.walker :as w]
            [konserve.core :as k]
            [clojure.core.async :as a]
            [superv.async :refer [<?? S]]))

(defn- cfg []
  {:store {:backend :file
           :path (str (System/getProperty "java.io.tmpdir") "/dh-walker-" (java.util.UUID/randomUUID))
           :id (java.util.UUID/randomUUID)}
   :schema-flexibility :write
   :keep-history? false})

(def ^:private schema
  [{:db/ident :issue/title :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :issue/attachment :db/valueType :db.type/store-ref
    :db/cardinality :db.cardinality/one}])

(deftest walk-follows-store-refs
  (let [c (cfg)]
    (d/create-database c)
    (let [conn  (d/connect c)
          _     (d/transact conn schema)
          store (:store @conn)
          bytes (.getBytes "sync payload" "UTF-8")
          blob  (blob/blob-id bytes)]
      (a/<!! (k/bassoc store blob bytes {:sync? false}))
      (d/transact conn [{:issue/title "sync me" :issue/attachment blob}])
      (let [walked (<?? S (w/datahike-walk-fn store {}))
            v      (vec walked)]
        (testing "a store-ref'd blob is in the walked set — index-only walks miss it"
          (is (contains? (set walked) blob)
              "the blob a datom names must be shipped, or the reference dangles on the subscriber"))
        (testing "and it precedes the mutable pointer cells, so it arrives before the head"
          (is (< (.indexOf v blob) (.indexOf v :branches))
              "content-addressed blobs belong with the nodes, ahead of :branches/:db")
          (is (contains? (set walked) :db) "the branch head is walked")))
      (testing "retract the datom and the blob drops out of the walk"
        (let [eid (d/q '[:find ?e . :where [?e :issue/title "sync me"]] @conn)]
          (d/transact conn [[:db/retractEntity eid]]))
        (is (not (contains? (set (<?? S (w/datahike-walk-fn store {}))) blob))
            "nothing names it now — no reason to replicate it"))
      (d/release conn))
    (d/delete-database c)))
