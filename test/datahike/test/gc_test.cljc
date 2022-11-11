(ns datahike.test.gc-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [superv.async :refer [<?? S]]
   [datahike.api :as d]
   [datahike.experimental.gc :refer [gc!]]
   [datahike.index.persistent-set :refer [mark]]
   [konserve.core :as k]
   [datahike.test.core-test]))

(defn- count-store [db]
  (count (k/keys (:store db) {:sync? true})))

(deftest datahike-gc-test
  (testing "Garbage collector functionality."
    (let [cfg {:store {:backend :file
                       :path    "/tmp/dh-gc-test"}
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
          ;; everything will fit into the root nodes of each index here
          num-roots 6
          fresh-count (+ num-roots 2) ;; :branches + :db + roots
          ]
      (is (= 1 (count (mark (:eavt @conn)))))
      (is (= fresh-count (count-store @conn)))
      (d/transact conn schema)
      (is (= 1 (count (mark (:eavt @conn)))))
      (is (= (+ 1 fresh-count num-roots) (count-store @conn)))
      ;; delete old roots
      (is (= (+ 1 num-roots) (count (<?? S (gc! @conn (java.util.Date.))))))
      (is (= fresh-count (count-store @conn)))

      ;; try to run on dirty index
      (is (thrown-msg? "Index needs to be properly flushed before marking."
                       (mark (:eavt
                              (:db-after
                               (d/with @conn [{:db/id 100
                                               :age   5}]))))))

      ;; check that we can still read the data
      (d/transact conn (vec (for [i (range 1000)]
                              {:age i})))
      (<?? S (gc! @conn (java.util.Date.)))
      (is (= 1000 (d/q '[:find (count ?e) .
                         :where
                         [?e :age _]]
                       @(d/connect cfg)))))))
