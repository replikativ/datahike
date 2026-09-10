(ns datahike.test.backfill-avet-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.backfill.avet :as avet]
            [datahike.backfill.effects :as effects]
            [datahike.backfill.sort :as sort]
            [datahike.backfill.storage :as storage]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.constants :as constants]
            [datahike.test.utils :as utils]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [org.replikativ.persistent_sorted_set PersistentSortedSet Branch]))

(defn- with-db [overrides f]
  (let [conn (utils/setup-db (merge {:crypto-hash? false :index :datahike.index/persistent-set
                                     :schema-flexibility :write
                                     :allow-index-backfill? true
                                     :writer {:writer-ownership :exclusive}}
                                    overrides))
        config (:config @conn)]
    (try (f conn) (finally (d/release conn) (d/delete-database config)))))

(defn- seed! [conn]
  (let [refs? (get-in @conn [:config :attribute-refs?])]
  ;; The persisted refs-mode system schema has no :db.type/bytes entity. Keep
  ;; byte codec coverage in non-ref cases; do not fabricate a system type ref.
    (d/transact conn (remove #(and refs? (= :bytes (:db/ident %)))
                             [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
                               :db/unique :db.unique/identity}
                              {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                              {:db/ident :old :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
                              {:db/ident :many :db/valueType :db.type/long :db/cardinality :db.cardinality/many}
                              {:db/ident :bytes :db/valueType :db.type/bytes :db/cardinality :db.cardinality/one}
                              {:db/ident :nohist :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/noHistory true}]))
    (d/transact conn (map #(if refs? (dissoc % :bytes) %)
                          [{:name "one" :score 9 :old 10 :many [1 2] :bytes (byte-array [1 2]) :nohist "before"}
                           {:name "two" :score 3 :old 30 :many [4 5] :bytes (byte-array [3 4])}]))
    (d/transact conn [{:db/id [:name "one"] :score 8 :old 11 :nohist "after"}])
    (d/transact conn [[:db/retract [:name "one"] :many 1]
                      [:db/add [:name "one"] :many 1]])
    (d/transact conn [{:db/id [:db/ident :old] :db/index false}])))

(defn- resolved [db ident]
  (if (get-in db [:config :attribute-refs?]) (get-in db [:ident-ref-map ident]) ident))

(defn- comparator-equal? [left right]
  (let [cmp (dd/index-type->cmp-quick :avet false)]
    (and (= (count left) (count right))
         (every? zero? (map cmp left right)))))

(deftest candidate-matches-synchronous-current-and-raw-history
  (doseq [refs? [false true] history? [false true]]
    (with-db {:attribute-refs? refs? :keep-history? history?}
      (fn [conn]
        (seed! conn)
        (let [source @conn
              old-schema (:schema source)
              attrs (cond-> #{:score :many :nohist :name} (not refs?) (conj :bytes))
              patch (mapv (fn [ident] {:db/id [:db/ident ident] :db/index true}) (disj attrs :name))
              oracle (d/db-with source patch)
              user-attrs (set (map #(resolved source %) (conj attrs :old)))
              user-only (fn [tree] (filterv #(contains? user-attrs (:a %)) tree))
              candidate (avet/build! source attrs {:sort {:window-records 2 :fan-in 2}
                                                   :storage {:pending-node-limit 2 :cache-node-limit 2}})]
          (try
            (is (= old-schema (:schema source)))
            (is (comparator-equal? (user-only (:avet oracle)) (user-only (:current candidate))))
            (is (every? #(contains? (:current candidate) %) (:avet source)))
            (if history?
              (do
                (is (comparator-equal? (user-only (:temporal-avet oracle))
                                       (user-only (:temporal candidate))))
                (is (seq (filter #(= (resolved source :old) (:a %)) (:temporal candidate))))
                (is (every? #(contains? (:temporal candidate) %) (:temporal-avet source))))
              (is (nil? (:temporal candidate))))
            (finally (avet/close! candidate))))))))

(deftest already-indexed-and-repeated-attributes-do-not-duplicate
  (with-db {:keep-history? true}
    (fn [conn]
      (seed! conn)
      (let [source @conn candidate (avet/build! source [:name :name])]
        (try
          (is (comparator-equal? (:avet source) (:current candidate)))
          (is (comparator-equal? (:temporal-avet source) (:temporal candidate)))
          (finally (avet/close! candidate)))))))

(deftest failures-close-private-storage-and-sort-scratch
  (with-db {:keep-history? true}
    (fn [conn]
      (seed! conn)
      (let [directory (Files/createTempDirectory "avet-builder-test-" (make-array FileAttribute 0))
            created (atom nil)
            original-create storage/create!
            original-sort sort/with-sorted-records!]
        (try
          (with-redefs [storage/create! (fn [& args]
                                          (let [private (apply original-create args)]
                                            (reset! created private) private))
                        sort/with-sorted-records! (fn [& args]
                                                    (apply original-sort args)
                                                    (throw (ex-info "cleanup failed" {:type :injected-cleanup})))]
            (is (= :injected-cleanup
                   (try (avet/build! @conn #{:score} {:sort {:directory directory :window-records 1}})
                        nil (catch Exception e (:type (ex-data e)))))))
          (is (:closed? (storage/resources @created)))
          (is (= 0 (:pending-nodes (storage/resources @created))
                 (:cached-nodes (storage/resources @created))))
          (with-open [^java.util.stream.Stream paths (Files/list directory)] (is (zero? (.count paths))))
          (finally (Files/delete directory)))))))

(deftest invalid-request-is-rejected-before-storage-or-sort
  (with-db {}
    (fn [conn]
      (with-redefs [storage/create! (fn [& _] (throw (AssertionError. "Unexpected storage allocation")))
                    sort/with-sorted-records! (fn [& _] (throw (AssertionError. "Unexpected scratch")))]
        (doseq [[attrs options] [[#{:missing} nil] [true nil] [#{} {:unknown 1}]
                                 [#{} {:sort {:fan-in 1}}]]]
          (is (thrown? clojure.lang.ExceptionInfo (avet/build! @conn attrs options))))))))

(deftest multilevel-roots-reopen-after-candidate-close
  (with-db {:keep-history? true}
    (fn [conn]
      (d/transact conn [{:db/ident :value :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                        {:db/ident :anchor :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}])
      (d/transact conn (mapv (fn [i] {:db/id (+ 10000 i) :value i :anchor (- i)}) (range 2300)))
      (d/transact conn (mapv (fn [i] {:db/id (+ 10000 i) :value (+ 5000 i)}) (range 300)))
      (let [source (assoc-in @conn [:store :datahike/branching-factor] 32)
            live (get-in source [:store :storage])
            before-cache @(:cache live)
            before-stats @(:stats live)
            before-roots (mapv (fn [key]
                                 (let [^PersistentSortedSet tree (get source key)
                                       reference (.-_root tree)
                                       root (.readReference (.-_settings tree) reference)]
                                   [tree reference root (when (instance? Branch root) (.-_state ^Branch root))]))
                               [:aevt :avet :temporal-aevt :temporal-avet])
            candidate (avet/build! source #{:value} {:sort {:window-records 64 :fan-in 2}
                                                     :storage {:cache-node-limit 2 :pending-node-limit 2}})
            roots (try
                    (let [roots [(pss/store (:current candidate)) (pss/store (:temporal candidate))]]
                      (storage/flush! (:storage candidate))
                      roots)
                    (finally (avet/close! candidate)))
            _ (is (= before-cache @(:cache live)))
            _ (is (= before-stats @(:stats live)))
            _ (doseq [[^PersistentSortedSet tree reference root state] before-roots]
                (is (identical? reference (.-_root tree)))
                (when state (is (identical? state (.-_state ^Branch root)))))
            cmp (dd/index-type->cmp-quick :avet false)
            [current temporal] (mapv #(pss/restore-by cmp % (get-in source [:store :storage])
                                                      {:branching-factor 32}) roots)
            select-value (fn [tree]
                           (di/-slice tree (dd/datom constants/e0 :value nil constants/tx0)
                                      (dd/datom constants/emax :value nil constants/txmax) :avet))
            current-values (mapv :v (select-value current))
            temporal-values (mapv :v (select-value temporal))]
        (is (= (vec (concat (range 300 2300) (range 5000 5300))) current-values))
        (is (= (vec (concat (mapcat #(repeat (if (< % 300) 2 1) %) (range 2300))
                            (range 5000 5300))) temporal-values))
        (is (every? #(contains? current %) (:avet source)))
        (is (every? #(contains? temporal %) (:temporal-avet source)))
        (is (= 2000 (count (filter #(< (:v %) 5000) (select-value current)))))
        (is (:closed? (storage/resources (:storage candidate))))))))

(deftest purge-of-disabled-index-removes-retained-history
  (with-db {:keep-history? true}
    (fn [conn]
      (seed! conn)
      (doseq [attrs [#{:old} #{:score}]]
        (let [source @conn
              old-value? #(and (= :old (:a %)) (= 10 (:v %)))
              active (assoc source :avet-build {:id (random-uuid) :attrs attrs})
              candidate (avet/build! active attrs)]
          (try
            (is (seq (filter old-value? (:temporal-avet source))))
            (let [purged (d/db-with active [[:db/purge [:name "one"] :old 10]])
                  replayed (reduce effects/replay candidate (:avet-build-effects purged))
                  synchronous (d/db-with (dissoc purged :avet-build :avet-build-effects)
                                         (mapv (fn [ident] {:db/id [:db/ident ident] :db/index true}) attrs))
                  old-only #(filterv (fn [datom] (= :old (:a datom))) %)]
              (is (empty? (filter old-value? (:temporal-aevt purged))))
            ;; Native purge must clear retained AVET entries even while the
            ;; effective schema no longer indexes this attribute.
              (is (empty? (filter old-value? (:temporal-avet purged))))
              (is (empty? (filter old-value? (:temporal replayed))))
              (is (comparator-equal? (old-only (:temporal-avet synchronous))
                                     (old-only (:temporal replayed)))))
            (finally (avet/close! candidate))))))))
