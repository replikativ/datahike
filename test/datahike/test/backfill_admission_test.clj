(ns datahike.test.backfill-admission-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.backfill.admission :as admission]
            [datahike.backfill.avet :as avet]
            [datahike.backfill.effects :as effects]
            [datahike.backfill.unique :as unique]
            [datahike.core :as core]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.db :as db]
            [datahike.db.transaction :as transaction]
            [datahike.db.utils :as dbu]
            [datahike.index.interface :as di]
            [datahike.index.secondary :as sec]
            [datahike.index.persistent-set :as persistent-set]
            [datahike.writing :as writing]
            [datahike.test.backfill-effects-test :as effect-fixtures]
            [datahike.test.utils :as utils])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet]))

(defn- base [refs? history?]
  (:db-after
   (core/with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? refs? :keep-history? history?
                                :schema-flexibility :write})
              [{:db/ident :value :db/valueType :db.type/long
                :db/cardinality :db.cardinality/one}
               {:db/ident :other :db/valueType :db.type/long :db/index true
                :db/cardinality :db.cardinality/one}
               {:db/id 10000 :value 1 :other 10}
               {:db/id 10001 :value 2 :other 20}])))

(defn- active [database attrs]
  (let [generation (random-uuid)
        cursor {:generation generation :journal-id (random-uuid)
                :sequence 1 :position (Object.)}]
    (assoc database :avet-build {:id generation
                                 :attrs (into attrs
                                              (keep (fn [[ident entry]]
                                                      (when (and (keyword? ident) (map? entry)
                                                                 (or (:db/index entry) (:db/unique entry)))
                                                        ident)))
                                              (:schema database))}
           :avet-build-journal {:cursor cursor})))

(defn- candidate [source attrs]
  (let [resolved (set (map #(dbu/attr-ref-or-ident source %) attrs))
        additions (fn [k] (filter #(contains? resolved (:a %)) (get source k)))]
    {:current (reduce #(di/-insert %1 %2 :avet 0) (:avet source) (additions :aevt))
     :temporal (when (:temporal-avet source)
                 (reduce #(di/-temporal-insert %1 %2 :avet 0)
                         (:temporal-avet source) (additions :temporal-aevt)))}))

(defn- mint [source patch]
  (admission/mint! source (candidate source (set (keys patch))) patch
                   (get-in source [:avet-build-journal :cursor])))

(defn- tuples [tree] (mapv #(vec (seq %)) tree))

(deftest prepared-activation-matches-synchronous-oracle
  (doseq [refs? [false true] history? [false true]
          flags [{:db/index true} {:db/unique :db.unique/value}
                 {:db/index true :db/unique :db.unique/identity}]]
    (let [source (active (base refs? history?) #{:value :other :db/ident :db/txInstant})
          patch {:value flags}
          certificate (mint source patch)
          instant (java.util.Date. 2000000000000)]
      (with-redefs-fn {#'transaction/next-tx-instant (constantly instant)}
        (fn []
          (let [report (core/with-prepared-avet source certificate)
                actual (:db-after report)
                expected (:db-after (core/with source (admission/transaction-data certificate)
                                               nil {:allow-index-backfill? true}))]
            (is (identical? source (:db-before report)))
            (is (= (:schema expected) (:schema actual)))
            (doseq [key [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet]]
              (is (= (tuples (get expected key)) (tuples (get actual key))) (str [refs? history? flags key])))
            (is (nil? (:avet-build actual)))
            (is (nil? (:avet-build-journal actual)))
            (is (empty? (effects/observations actual)))
            (is (not (effects/enabled? actual)))
            (is (= (:avet-build source) (:avet-build (:db-before report))))))))))

(deftest strict-request-and-local-source-binding
  (let [source (active (base false true) #{:value})
        patch {:value {:db/index true}}
        certificate (mint source patch)]
    (doseq [invalid [nil {} {:missing {:db/index true}} {:value {:db/index false}}
                     {:value {:db/cardinality :db.cardinality/many}}
                     {:value {:db/unique :unrecognized}}
                     {:other {:db/index true}} {10000 {:db/index true}}
                     {:db/txInstant {:db/unique :db.unique/value}}]]
      (is (thrown? clojure.lang.ExceptionInfo (admission/plan source invalid))))
    (is (thrown? clojure.lang.ExceptionInfo (core/with-prepared-avet source {:checked? true})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (core/with-prepared-avet (assoc source :unrelated true) certificate)))
    (is (thrown? clojure.lang.ExceptionInfo
                 (admission/check! certificate source
                                   (conj (admission/transaction-data certificate) [:db/add 10000 :value 8]))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (core/with source (admission/transaction-data certificate) nil {:prepared certificate})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (core/with source (admission/transaction-data certificate) {:prepared certificate})))
    (is (= 1 (get-in source [:avet-build-journal :cursor :sequence])))
    (is (not (get-in source [:schema :value :db/index])))))

(deftest checked-unique-candidate-and-stale-cursor-rejected
  (let [source (active (base false true) #{:value})
        duplicate (:db-after (core/with source [[:db/add 10001 :value 1]]))
        duplicate (dissoc duplicate :avet-build-effects)
        patch {:value {:db/unique :db.unique/value}}]
    (is (thrown? clojure.lang.ExceptionInfo (mint duplicate patch)))
    (doseq [changed [(assoc-in source [:avet-build-journal :cursor :sequence] 2)
                     (assoc source :avet-build-invalidated? true)
                     (assoc source :avet-build-effects [[:current :remove nil nil]])]]
      (is (thrown? clojure.lang.ExceptionInfo
                   (admission/mint! changed (candidate changed #{:value}) patch
                                    (get-in source [:avet-build-journal :cursor])))))))

(deftest prepared-path-does-not-rescan-certified-attributes
  (let [source (active (base false true) #{:value})
        certificate (mint source {:value {:db/index true :db/unique :db.unique/value}})]
    (with-redefs-fn
      {#'transaction/schema-attr-current-datoms (fn [& _] (throw (ex-info "Unexpected full current sweep" {})))
       #'transaction/schema-attr-history-datoms (fn [& _] (throw (ex-info "Unexpected full history sweep" {})))
       #'transaction/validate-unique-avet! (fn [& _] (throw (ex-info "Unexpected full unique scan" {})))
       #'unique/validate! (fn [& _] (throw (ex-info "Unexpected certificate mint on writer" {})))}
      #(is (get-in (core/with-prepared-avet source certificate) [:db-after :schema :value :db/index])))))

(deftest advancement-replays-complete-transaction-before-checking
  (let [source (active (base false true) #{:value})
        certificate (mint source {:value {:db/unique :db.unique/value}})
        after (:db-after (core/with source [[:db/add 10000 :value 2] [:db/add 10001 :value 1]]))
        cursor (-> (get-in source [:avet-build-journal :cursor])
                   (update :sequence inc) (assoc :position (Object.)))
        frame {:kind :transaction :generation (:generation cursor) :sequence (:sequence cursor)
               :max-tx (:max-tx after) :effects (:avet-build-effects after)}
        next-source (-> after (dissoc :avet-build-effects) (assoc-in [:avet-build-journal :cursor] cursor))
        advanced (admission/advance certificate next-source frame cursor)]
    (is (get-in (core/with-prepared-avet next-source advanced) [:db-after :schema :value :db/unique]))
    (is (thrown? clojure.lang.ExceptionInfo (admission/advance advanced next-source frame cursor)))
    (is (thrown? clojure.lang.ExceptionInfo (core/with-prepared-avet next-source certificate)))
    (is (thrown? clojure.lang.ExceptionInfo
                 (admission/advance certificate next-source (update frame :sequence inc) cursor)))))

(deftest range-advancement-checks-each-complete-frame
  (let [source (active (base false true) #{:value})
        certificate (mint source {:value {:db/unique :db.unique/value}})
        after (:db-after (core/with source [[:db/add 10000 :value 3]]))
        later (:db-after (core/with (dissoc after :avet-build-effects) [[:db/add 10001 :value 4]]))
        c1 (-> (admission/cursor certificate) (update :sequence inc) (assoc :position (Object.)))
        c2 (-> c1 (update :sequence inc) (assoc :position (Object.)))
        frame (fn [database cursor]
                {:kind :transaction :generation (:generation cursor) :sequence (:sequence cursor)
                 :max-tx (:max-tx database) :effects (:avet-build-effects database)})
        f1 (frame after c1) f2 (frame later c2)
        final-source (-> later (dissoc :avet-build-effects) (assoc-in [:avet-build-journal :cursor] c2))
        read-range (fn [f init] (f (f init f1 c1) f2 c2))
        advanced (admission/advance-range certificate final-source c2 read-range)]
    (is (= c2 (admission/cursor advanced)))
    (is (get-in (core/with-prepared-avet final-source advanced) [:db-after :schema :value :db/unique]))
    (is (thrown? clojure.lang.ExceptionInfo
                 (admission/advance-range certificate final-source c2 (fn [f init] (f init f1 c1)))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (admission/advance-range certificate final-source c2 (fn [f init] (f init f2 c2)))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (admission/advance-range certificate final-source c2
                                          (fn [f init] (f (f init f1 c1) (assoc f2 :sequence 90) c2)))))))

(deftest activation-derived-tuples-use-complete-replayed-candidate
  (doseq [refs? [false true] duplicate? [false true]]
    (let [initial (:db-after
                   (core/with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? refs? :keep-history? true
                                                :schema-flexibility :write})
                              [{:db/ident :aaa :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                               {:db/ident :bbb :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                               {:db/ident :tag :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                               {:db/ident :pair :db/valueType :db.type/tuple :db/tupleAttrs [:db/index :tag]
                                :db/cardinality :db.cardinality/one}]))
          ;; The tuple also covers built-in indexed schema entities. Give
          ;; those distinct tags before testing the deliberate aaa/bbb pair.
          tagged (:db-after
                  (core/with initial
                             (into [] (map (fn [eid] [:db/add eid :tag (+ 1000000 eid)]))
                                   (into #{} (keep #(when (= (dbu/attr-ref-or-ident initial :db/index) (:a %))
                                                      (:e %))) (:eavt initial)))))
          ;; Composite tuples initialize unchanged components to nil; they do
          ;; not backfill source values predating the tuple definition. Assert
          ;; bbb's index flag now so its tuple genuinely starts [true tag].
          populated (:db-after (core/with tagged [[:db/add (dbu/entid initial :bbb) :db/index true]
                                                  [:db/add (dbu/entid initial :aaa) :tag 1]
                                                  [:db/add (dbu/entid initial :bbb) :tag (if duplicate? 1 2)]]))
          source (active populated #{:aaa :pair :bbb :db/ident :db/txInstant})
          pair-value (fn [ident]
                       (:v (first (filter #(and (= (dbu/entid source ident) (:e %))
                                                (= (dbu/attr-ref-or-ident source :pair) (:a %)))
                                          (:eavt source)))))
          _ (is (= [nil 1] (pair-value :aaa)))
          _ (is (= [true (if duplicate? 1 2)] (pair-value :bbb)))
          certificate (mint source {:aaa {:db/index true} :pair {:db/unique :db.unique/value}})
          original-source (mapv #(tuples (get source %)) [:eavt :avet :temporal-avet])
          original-candidate (mapv #(tuples (get (admission/candidate certificate) %)) [:current :temporal])]
      (if duplicate?
        (is (thrown? clojure.lang.ExceptionInfo (core/with-prepared-avet source certificate)))
        (with-redefs-fn {#'transaction/next-tx-instant (constantly (java.util.Date. 2000000000000))}
          (fn []
            (let [actual (:db-after (core/with-prepared-avet source certificate))
                  expected (:db-after (core/with source (admission/transaction-data certificate)
                                                 nil {:allow-index-backfill? true}))]
              (is (= (tuples (:avet expected)) (tuples (:avet actual))))
              (is (= (tuples (:temporal-avet expected)) (tuples (:temporal-avet actual))))))))
      (is (= original-source (mapv #(tuples (get source %)) [:eavt :avet :temporal-avet])))
      (is (= original-candidate (mapv #(tuples (get (admission/candidate certificate) %)) [:current :temporal]))))))

(deftest authoritative-wrapper-rebinding-requires-roots-and-fence
  (let [source (-> (active (base false true) #{:value})
                   (assoc-in [:meta :datahike/commit-id] (random-uuid))
                   (assoc :datahike.writing/head-revision (random-uuid)))
        certificate (mint source {:value {:db/index true}})
        wrapper (assoc source :datahike.writing/head-revision (:datahike.writing/head-revision source))
        rebound (admission/rebind-source certificate wrapper)]
    (is (not (identical? source wrapper)))
    (is (thrown? clojure.lang.ExceptionInfo (core/with-prepared-avet wrapper certificate)))
    (is (identical? wrapper (:db-before (core/with-prepared-avet wrapper rebound))))
    (doseq [changed [(assoc source :datahike.writing/head-revision (random-uuid))
                     (assoc-in source [:meta :datahike/commit-id] (random-uuid))
                     (assoc-in source [:config :branch] :other)
                     (assoc-in source [:schema :value :db/doc] "changed")
                     (assoc-in source [:ident-ref-map :value] 123456)
                     (assoc-in source [:secondary-index-keys :opaque/index] (random-uuid))
                     (assoc source :aevt (:avet source))
                     (assoc-in source [:avet-build :id] (random-uuid))
                     (update-in source [:avet-build-journal :cursor :sequence] inc)
                     (dissoc source :datahike.writing/head-revision)]]
      (is (thrown? clojure.lang.ExceptionInfo (admission/rebind-source certificate changed))))))

(deftest range-rejects-duplicate-before-a-later-repair
  (let [source (active (base false true) #{:value})
        certificate (mint source {:value {:db/unique :db.unique/value}})
        duplicate (:db-after (core/with source [[:db/add 10000 :value 2]]))
        repaired (:db-after (core/with (dissoc duplicate :avet-build-effects) [[:db/add 10001 :value 3]]))
        c1 (-> (admission/cursor certificate) (update :sequence inc) (assoc :position (Object.)))
        c2 (-> c1 (update :sequence inc) (assoc :position (Object.)))
        final-source (-> repaired (dissoc :avet-build-effects) (assoc-in [:avet-build-journal :cursor] c2))
        reached-repair? (atom false)
        frame (fn [database cursor]
                {:kind :transaction :generation (:generation cursor) :sequence (:sequence cursor)
                 :max-tx (:max-tx database) :effects (:avet-build-effects database)})]
    (is (thrown? clojure.lang.ExceptionInfo
                 (admission/advance-range
                  certificate final-source c2
                  (fn [f init]
                    (let [first-result (f init (frame duplicate c1) c1)]
                      (reset! reached-repair? true)
                      (f first-result (frame repaired c2) c2))))))
    (is (false? @reached-repair?))))

(defn- same-tree? [left right]
  (let [cmp (dd/index-type->cmp-quick :avet false)]
    (and (= (count left) (count right)) (every? zero? (map cmp left right)))))

(deftest private-storage-admission-preserves-owned-roots-on-success-and-failure
  (let [conn (utils/setup-db {:crypto-hash? false :index :datahike.index/persistent-set
                              :schema-flexibility :write :attribute-refs? false :keep-history? true
                              :writer {:writer-ownership :exclusive}})
        config (:config @conn)]
    (try
      (d/transact conn [{:db/ident :value :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}
                        {:db/id 10000 :value 1} {:db/id 10001 :value 2}])
      (let [source (active @conn #{:value})
            private (avet/build! source #{:value} {:sort {:window-records 2 :fan-in 2}
                                                   :storage {:pending-node-limit 2 :cache-node-limit 2}})]
        (try
          (let [unflushed (admission/mint! source private {:value {:db/index true :db/unique :db.unique/value}}
                                           (get-in source [:avet-build-journal :cursor]))
                _ (is (thrown? clojure.lang.ExceptionInfo (core/with-prepared-avet source unflushed)))
                certificate (admission/flush! unflushed)
                original-source (mapv #(tuples (get source %)) [:eavt :avet :temporal-avet])
                original-private (mapv #(tuples (get private %)) [:current :temporal])]
            ;; Fail after native transients and complete activation replay have
            ;; both run. No published/private source may have been mutated.
            (with-redefs [unique/validate-effects! (fn [& _] (throw (ex-info "Rejected final activation" {})))]
              (is (thrown? clojure.lang.ExceptionInfo (core/with-prepared-avet source certificate))))
            (is (= original-source (mapv #(tuples (get source %)) [:eavt :avet :temporal-avet])))
            (is (= original-private (mapv #(tuples (get private %)) [:current :temporal])))
            (with-redefs-fn {#'transaction/next-tx-instant (constantly (java.util.Date. 2000000000000))}
              (fn []
                (let [actual (:db-after (core/with-prepared-avet source certificate))
                      expected (:db-after (core/with source (admission/transaction-data certificate)
                                                     nil {:allow-index-backfill? true}))]
                  (is (same-tree? (:avet expected) (:avet actual)))
                  (is (same-tree? (:temporal-avet expected) (:temporal-avet actual)))
                  (let [store (:store source)
                        expected-rows (mapv #(tuples (get actual %)) [:avet :temporal-avet])
                        roots (mapv #(di/-flush (get actual %) store) [:avet :temporal-avet])]
                    (doseq [^PersistentSortedSet root roots]
                      (is (identical? (:storage store) (.-_storage root))))
                    (is (= original-private (mapv #(tuples (get private %)) [:current :temporal])))
                    (writing/write-pending-kvs! store (writing/get-and-clear-pending-kvs! store) true)
                    (avet/close! private)
                    ;; Reconstruct without any resident root or shared live
                    ;; cache. Every node read must survive private disposal.
                    (let [cold-storage (persistent-set/create-storage
                                        (dissoc store :storage di/node-cache-key) (:config source))
                          cold (mapv (fn [^PersistentSortedSet root]
                                       (PersistentSortedSet. (meta root) (dd/index-type->cmp-quick :avet false)
                                                             (.-_address root) cold-storage nil
                                                             (.-_count root) (.-_settings root) (.-_version root))) roots)]
                      (is (= expected-rows (mapv tuples cold))))))))
            (is (= original-source (mapv #(tuples (get source %)) [:eavt :avet :temporal-avet]))))
          (finally (avet/close! private))))
      (finally (d/release conn) (d/delete-database config)))))

(deftest arrays-and-tuples-retain-native-uniqueness-semantics-on-admission
  (doseq [[properties value equal-value other-value]
          [[{} (byte-array [1 2]) (byte-array [1 2]) (byte-array [1 3])]
           [{} (float-array [1 2]) (float-array [1 2]) (float-array [1 3])]
           [{} (double-array [1 2]) (double-array [1 2]) (double-array [1 3])]
           [{:db/valueType :db.type/tuple :db/tupleTypes [:db.type/long :db.type/string]}
            [1 "a"] [1 "a"] [1 "b"]]]]
    (let [initial (:db-after
                   (core/with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? false :keep-history? true :schema-flexibility :read})
                              [(merge {:db/ident :payload :db/cardinality :db.cardinality/one} properties)
                               [:db/add 10000 :payload value] [:db/add 10001 :payload other-value]]))
          source (active initial #{:payload})
          patch {:payload {:db/unique :db.unique/value}}
          certificate (mint source patch)]
      (with-redefs-fn {#'transaction/next-tx-instant (constantly (java.util.Date. 2000000000000))}
        (fn []
          (let [actual (:db-after (core/with-prepared-avet source certificate))
                expected (:db-after (core/with source (admission/transaction-data certificate)
                                               nil {:allow-index-backfill? true}))]
            (is (same-tree? (:avet expected) (:avet actual)))
            (is (same-tree? (:temporal-avet expected) (:temporal-avet actual))))))
      (let [duplicate (-> (:db-after (core/with source [[:db/add 10001 :payload equal-value]]))
                          (dissoc :avet-build-effects))]
        (is (thrown? clojure.lang.ExceptionInfo (mint duplicate patch)))))))

(deftest ordinary-path-still-runs-full-backfill-and-unique-validation
  (let [source (base false true)
        current-scan @#'transaction/schema-attr-current-datoms
        history-scan @#'transaction/schema-attr-history-datoms
        unique-scan @#'transaction/validate-unique-avet!
        calls (atom {})
        counted (fn [label f] (fn [& args] (swap! calls update label (fnil inc 0)) (apply f args)))]
    (with-redefs-fn {#'transaction/schema-attr-current-datoms (counted :current current-scan)
                     #'transaction/schema-attr-history-datoms (counted :history history-scan)
                     #'transaction/validate-unique-avet! (counted :unique unique-scan)}
      #(core/with source [[:db/add (dbu/entid source :value) :db/unique :db.unique/value]]
                  nil {:allow-index-backfill? true}))
    (doseq [label [:current :history :unique]] (is (pos? (get @calls label 0))))))

(deftest secondary-only-admission-indexes-primary-hashes
  (doseq [refs? [false true]]
    (let [initial (:db-after (core/with (base refs? true)
                                        [{:db/ident :body :db/valueType :db.type/string
                                          :db/cardinality :db.cardinality/one :db.secondary/only true}]))
          ready (-> initial
                    (assoc-in [:schema :idx/recorder] {:db.secondary/status :ready
                                                       :db.secondary/type :test/effect-recorder
                                                       :db.secondary/attrs [:body]})
                    (assoc-in [:rschema :db.secondary/index :body] #{:idx/recorder})
                    (assoc-in [:secondary-indices :idx/recorder] (effect-fixtures/->PrimaryHashRecorder [])))]
      (binding [sec/*durable-secondary-write-context* :commit]
        (let [source (active (:db-after (core/with ready [[:db/add 10000 :body "full first"]
                                                          [:db/add 10001 :body "full second"]])) #{:body})
              certificate (mint source {:body {:db/index true}})]
          (with-redefs-fn {#'transaction/next-tx-instant (constantly (java.util.Date. 2000000000000))}
            (fn []
              (let [actual (:db-after (core/with-prepared-avet source certificate))
                    expected (:db-after (core/with source (admission/transaction-data certificate)
                                                   nil {:allow-index-backfill? true}))
                    values (map :v (filter #(= (dbu/attr-ref-or-ident source :body) (:a %)) (:avet actual)))]
                (is (= #{(sec/secondary-only-hash "full first") (sec/secondary-only-hash "full second")}
                       (set values)))
                (is (same-tree? (:avet expected) (:avet actual)))
                (is (same-tree? (:temporal-avet expected) (:temporal-avet actual)))))))))))
