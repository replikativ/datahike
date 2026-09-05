(ns datahike.test.index-compatibility-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.index :as di]
            [datahike.index.compatibility :as compatibility]
            [datahike.migrate :as migrate]
            [datahike.migrate.fs :as fs]
            [datahike.writing :as writing]
            [konserve.core :as k]))

(defn- config []
  {:store {:backend :file :id (random-uuid)
           :path (fs/temp-store-path! "nan-format-")}
   :index :datahike.index/persistent-set
   :keep-history? true :schema-flexibility :write})

(def schema
  [{:db/ident :n/value :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one :db/index true}])

(defn- error-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(def ^:private current-compare dd/compare-value)

(defn- legacy-compare [a b]
  ;; Preserve the previous recursive tuple/array/type rules. The captured
  ;; comparator's sequential branch recurses through the rebound Var, so only
  ;; numeric comparisons (including numbers inside tuples) use the old rule.
  (if (and (number? a) (number? b)) (compare a b) (current-compare a b)))

(defn- legacy-db!
  "Write actual disk roots with the previous numeric comparator and no marker.
   The rest of the persistence machinery is unchanged. This reproduces the
   old AVET order rather than stripping the marker off a correctly built tree."
  ([cfg rows] (legacy-db! cfg schema rows))
  ([cfg schema rows]
   (with-redefs [dd/compare-value legacy-compare
                 compatibility/stored-marker (constantly nil)]
     (d/create-database cfg)
     (let [c (d/connect cfg)]
       (d/transact c (into schema rows))
       c))))

(deftest hitchhiker-tree-does-not-use-pss-format
  (let [cfg {:index :datahike.index/hitchhiker-tree}]
    (is (nil? (compatibility/stored-marker cfg)))
    (with-redefs [di/-seq (fn [& _] (throw (ex-info "PSS scan used for HHT" {})))]
      (is (nil? (compatibility/ensure-compatible!
                 {:config cfg :pss-comparator-version 99 :eavt-key :hht-root} nil))))))

(deftest legacy-nan-roots-refuse-materialization
  (let [cfg (config)
        c (legacy-db! cfg [{:db/id 100 :n/value Double/NaN}
                           {:db/id 101 :n/value 0.0}])]
    (try
      (let [store (:store @c)
            stored (k/get store :db nil {:sync? true})]
        (is (not (contains? stored :pss-comparator-version)))
        (is (= [100 101] (mapv :e (filter #(= :n/value (:a %))
                                          (di/-seq (:avet @c)))))
            "Legacy physical order puts NaN before zero because entity IDs break the old value tie")
        (doseq [restore [writing/stored->db writing/stored->db-read-only]]
          (is (= :index/comparator-migration-required
                 (:error (error-data #(restore stored store))))))
        (doseq [root [:eavt-key :aevt-key :avet-key
                      :temporal-eavt-key :temporal-aevt-key :temporal-avet-key]]
          (let [single (-> (apply dissoc stored [:eavt-key :aevt-key :avet-key
                                                 :temporal-eavt-key :temporal-aevt-key :temporal-avet-key])
                           (assoc root (:eavt-key stored)))]
            (is (= root (:index (error-data #(compatibility/ensure-compatible! single store)))))))
        (is (not (contains? (k/get store :db nil {:sync? true}) :pss-comparator-version))
            "Failed reads do not stamp a legacy head"))
      (finally (d/release c) (d/delete-database cfg)))))

(deftest clean-legacy-roots-upgrade-on-commit
  (let [cfg (assoc (config) :crypto-hash? true)
        c (legacy-db! cfg [{:db/id 100 :n/value 1.0}])
        before (k/get (:store @c) :db nil {:sync? true})
        old-cid (get-in before [:meta :datahike/commit-id])]
    (d/release c)
    (try
      (let [c (d/connect cfg)]
        (try
          (let [after-read (k/get (:store @c) :db nil {:sync? true})]
            (is (not (contains? after-read :pss-comparator-version)))
            (is (= old-cid (get-in after-read [:meta :datahike/commit-id])))
            (is (= old-cid (writing/create-commit-id after-read after-read))))
          (d/transact c [[:db/add 100 :n/value 2.0]])
          (let [historical (k/get (:store @c) old-cid nil {:sync? true})]
            (is (not (contains? historical :pss-comparator-version)))
            (is (= old-cid (writing/create-commit-id historical historical)))
            (is (= old-cid (get-in (writing/stored->db-read-only historical (:store @c))
                                   [:meta :datahike/commit-id]))))
          (let [stored (k/get (:store @c) :db nil {:sync? true})]
            (is (= 1 (:pss-comparator-version stored)))
            (doseq [unknown [nil 0 2 "1"]
                    restore [writing/stored->db writing/stored->db-read-only]]
              (is (= :index/unsupported-comparator-version
                     (:error (error-data #(restore
                                           (assoc stored :pss-comparator-version unknown) (:store @c))))))))
          (finally (d/release c))))
      (finally (d/delete-database cfg)))))

(deftest legacy-tuple-nan-refuses-public-connect
  (let [cfg (config)
        c (legacy-db! cfg
                      [{:db/ident :n/tuple :db/valueType :db.type/tuple
                        :db/tupleTypes [:db.type/double :db.type/string]
                        :db/cardinality :db.cardinality/one :db/index true}]
                      [{:db/id 100 :n/tuple [Double/NaN "x"]}
                       {:db/id 101 :n/tuple [0.0 "x"]}])]
    (is (= [100 101] (mapv :e (filter #(= :n/tuple (:a %)) (di/-seq (:avet @c))))))
    (d/release c)
    (try
      (is (= :index/comparator-migration-required
             (:error (error-data #(d/connect cfg)))))
      (finally (d/delete-database cfg)))))

(deftest history-only-nan-refuses-legacy-open
  (let [cfg (config)
        c (legacy-db! cfg [{:db/id 100 :n/value Double/NaN}])]
    (try
      (with-redefs [dd/compare-value legacy-compare
                    compatibility/stored-marker (constantly nil)
                    compatibility/ensure-compatible! (fn [& _])]
        (d/transact c [[:db/retract 100 :n/value Double/NaN]]))
      (is (empty? (filter #(= :n/value (:a %)) (di/-seq (:eavt @c)))))
      (let [stored (k/get (:store @c) :db nil {:sync? true})]
        (is (= :temporal-eavt-key
               (:index (error-data #(writing/stored->db stored (:store @c)))))))
      (finally (d/release c) (d/delete-database cfg)))))

(deftest comparator-marker-is-bound-to-commit-identity
  (let [cfg (assoc (config) :crypto-hash? true)]
    (d/create-database cfg)
    (try
      (let [c (d/connect cfg)]
        (try
          (let [stored (k/get (:store @c) :db nil {:sync? true})
                cid (get-in stored [:meta :datahike/commit-id])]
            (is (= cid (writing/create-commit-id stored stored)))
            (is (not= cid (writing/create-commit-id stored (dissoc stored :pss-comparator-version))))
            (is (not= cid (writing/create-commit-id stored (assoc stored :pss-comparator-version 2)))))
          (finally (d/release c))))
      (finally (d/delete-database cfg)))))

(deftest legacy-nan-export-rebuilds-under-new-comparator
  (let [cfg (config)
        target-cfg (config)
        dump (fs/temp-dir! "nan-migration-")
        c (legacy-db! cfg [{:db/id 100 :n/value Double/NaN}
                           {:db/id 101 :n/value 0.0}])]
    (try
      ;; Export while running the old comparator, just as an operator must do
      ;; before switching runtimes. No materialization bypass is added to API.
      (with-redefs [dd/compare-value legacy-compare]
        (migrate/export-db @c dump {:history? true}))
      (d/create-database target-cfg)
      (let [target (d/connect target-cfg)]
        (try
          (is (:verified? (migrate/import-db target dump {:build-indexes? true :eids :preserve})))
          (is (= [101 100] (mapv :e (d/datoms @target :avet :n/value))))
          (is (= [100] (mapv :e (d/datoms @target :avet :n/value Double/NaN))))
          (is (= 1 (:pss-comparator-version (k/get (:store @target) :db nil {:sync? true}))))
          (finally (d/release target))))
      (testing "regular import also rebuilds numeric order"
        (let [regular-cfg (config)]
          (d/create-database regular-cfg)
          (try
            (let [target (d/connect regular-cfg)]
              (try
                (is (:verified? (migrate/import-db target dump {:eids :preserve})))
                (is (= [101 100] (mapv :e (d/datoms @target :avet :n/value))))
                (finally (d/release target))))
            (finally (d/delete-database regular-cfg)))))
      (let [target (d/connect target-cfg)]
        (try
          (is (= [101 100] (mapv :e (d/datoms @target :avet :n/value))))
          (finally (d/release target))))
      (finally
        (d/release c)
        (d/delete-database cfg)
        (d/delete-database target-cfg)
        (doseq [f (reverse (file-seq (io/file dump)))] (io/delete-file f true))))))
