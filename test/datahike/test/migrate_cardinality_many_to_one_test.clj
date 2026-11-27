(ns datahike.test.migrate-cardinality-many-to-one-test
  "A many→one cardinality change round-trips through export/import (sorted
   export). It used to fail verification (19 expected, 18 present) because the
   SOURCE history held a retraction of a card-many-era value without its
   assertion — see `datahike.test.cardinality-history-test`."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.constants :as c]
            [datahike.db.interface :as dbi]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]))

(defn- cfg [attribute-refs?]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write :attribute-refs? attribute-refs?})

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- history [db]
  (let [h (d/history db)
        id (fn [x] (if (number? x) (dbi/-ident-for h x) x))]
    (set (for [dm (d/datoms h :eavt) :when (> (:tx dm) c/tx0)]
           [(id (:a dm))
            (if (and (number? (:v dm)) (= :db/cardinality (id (:a dm)))) (id (:v dm)) (:v dm))
            (:added dm)]))))

(deftest many-to-one-round-trips
  (doseq [attribute-refs? [false true]]
    (testing (str ":attribute-refs? " attribute-refs?)
      (let [src (utils/setup-db (cfg attribute-refs?))
            _ (d/transact src [{:db/ident :name :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                               {:db/ident :tag :db/valueType :db.type/keyword
                                :db/cardinality :db.cardinality/many}])
            _ (d/transact src [{:name "a" :tag [:x :y]}])
            _ (d/transact src [[:db/retract [:name "a"] :tag :y]])
            _ (d/transact src [{:db/id :tag :db/cardinality :db.cardinality/one}])
            _ (d/transact src [{:name "a" :tag :z}])
            dir (str (System/getProperty "java.io.tmpdir") "/dh-m2o-" (java.util.UUID/randomUUID))
            _ (m/export-db @src dir {:history? true})
            tgt (utils/setup-db (cfg attribute-refs?))
            rep (try (m/import-db tgt dir {})
                     (catch clojure.lang.ExceptionInfo e {:error (ex-data e)}))]
        (try
          (is (nil? (:error rep)))
          (is (:verified? rep))
          (is (contains? (history @src) [:tag :x true]))
          (is (= (history @src) (history @tgt)))
          (finally (teardown src) (teardown tgt)))))))
