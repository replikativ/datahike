(ns datahike.reachability
  "Read-only reachability of stored records. Root retention and sweep authority
   remain the collector's responsibility; this module does not create pins or
   publish roots."
  (:require [clojure.set :as set]
            [datahike.datom :as dd]
            [datahike.constants :as c]
            [datahike.gc-reference :as gc-ref]
            [datahike.store :as ds]
            [datahike.index.interface :refer [-seed-root! -slice with-storage mark-shared]]
            [datahike.index.secondary :as sec]
            [datahike.schema :as schema]
            [konserve.core :as k]
            [clojure.core.async :as async]
            [replikativ.logging :as log]
            #?(:clj [konserve.utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer-macros [go-try- <?-]]))
  #?(:clj (:import [java.util Date])))

;; meta-data does not get passed in macros
(defn get-time [d]
  (.getTime ^Date d))

(defn- attr-store-refs
  "The object ids named by the VALUES of `attr` in the AEVT index `aevt`. For a
   key-bearing value type THE VALUE IS THE KEY, so this is just the attribute's
   values.

   Slices exactly the attribute's range, so the cost is O(its datoms), not O(the
   database)."
  [aevt attr]
  (into #{}
        (map :v)
        (-slice aevt
                (dd/datom c/e0 attr nil c/tx0)
                (dd/datom c/emax attr nil c/txmax)
                :aevt)))

(defn value-references [config schema ident-ref-map aevt taevt context]
  (reduce
   (fn [acc [ident {:keys [db/valueType]}]]
     (if (and (contains? schema/key-bearing-value-types valueType)
              (or (not (:discovery-only? context)) (= :db.type/gc-ref valueType)))
       (let [attr (if (:attribute-refs? config) (get ident-ref-map ident) ident)
             _ (when (and (:attribute-refs? config) (not (integer? attr)))
                 (throw (ex-info "Reference attribute lacks its numeric schema identity."
                                 {:type :datahike/gc-reference-invalid-schema :attribute ident})))
             values (cond-> (attr-store-refs aevt attr)
                      taevt (set/union (attr-store-refs taevt attr)))]
         (reduce (fn [acc value]
                   (gc-ref/merge-contributions
                    acc (gc-ref/expand-reference
                         (if (= :db.type/store-ref valueType)
                           {:type :object :key value}
                           (gc-ref/decode-reference value)) context)))
                 acc values))
       acc))
   (gc-ref/merge-contributions) schema))

(defmethod gc-ref/expand-reference :secondary [{:keys [key-map]} {:keys [store discovery-only?]}]
  {:reachable (if discovery-only? #{} (sec/mark-from-key-map key-map store))
   :external-secondary-roots (if-let [root (sec/external-root-from-key-map key-map)] #{root} #{})})

(defn mark-failure [e]
  #?(:clj (if (instance? Exception e) e
              (ex-info "Reachability marking failed; refusing to sweep."
                       {:type :datahike/gc-mark-failed} e))
     :cljs (if (instance? js/Error e) e
               (ex-info "Reachability marking failed; refusing to sweep."
                        {:type :datahike/gc-mark-failed :thrown-value e}))))

(declare require-reference-policy!)

(defn- target-config [record caller-config key required?]
  (if-not required?
    caller-config
    (let [config (:config record)]
      (when-not (and (map? config) (boolean? (:crypto-hash? config))
                     (boolean? (:keep-history? config)) (boolean? (:attribute-refs? config))
                     (keyword? (:index config)))
        (throw (ex-info "Exact snapshot target lacks its persisted traversal policy."
                        {:type :datahike/gc-reference-invalid-target :record-key key})))
      (require-reference-policy! config)
      config)))

(defn- validate-target! [record key config schema required?]
  (when required?
    (when (or (not= key (get-in record [:meta :datahike/commit-id]))
              (nil? schema)
              (not-every? #(some? (get record %)) [:eavt-key :aevt-key :avet-key])
              (and (:keep-history? config)
                   (not-every? #(some? (get record %))
                               [:temporal-eavt-key :temporal-aevt-key :temporal-avet-key]))
              (= :building (get-in record [:avet-build :status]))
              (some #(= :building (:db.secondary/status %)) (vals schema)))
      (throw (ex-info "Incomplete or mismatched exact snapshot record."
                      {:type :datahike/gc-reference-invalid-target :record-key key})))
    (doseq [[ident entry] schema :when (:db.secondary/type entry)]
      (when (and (sec/durable-index-type? (:db.secondary/type entry))
                 (nil? (get-in record [:secondary-index-keys ident])))
        (throw (ex-info "Required snapshot secondary generation is missing."
                        {:type :datahike/gc-reference-invalid-target
                         :record-key key :index-ident ident}))))))

(defn- record-edges [store branch key record config schema-meta opts required?]
  (let [schema-key (:schema-meta-key record)
        schema (or (:schema schema-meta) (:schema record))
        _ (when (and schema-key (nil? schema))
            (log/raise "Schema metadata missing from store; cannot enumerate value dependencies."
                       {:type :schema-meta-missing :schema-meta-key schema-key :record key :branch branch}))
        _ (validate-target! record key config schema required?)
        context {:store store :config config :discovery-only? (:discovery-only? opts)
                 :store-id (ds/canonical-store-id store (:store config))}
        secondary (reduce (fn [acc [_ key-map]]
                            (gc-ref/merge-contributions
                             acc (gc-ref/expand-reference {:type :secondary :key-map key-map} context)))
                          (gc-ref/merge-contributions) (:secondary-index-keys record))
        _ (when required?
            ;; Same-store adapter contributions name physical Konserve objects;
            ;; unlike fused primary roots, none are virtual inline addresses.
            (doseq [address (:reachable secondary)]
              (when-not (k/exists? store address {:sync? true})
                (throw (ex-info "Required snapshot secondary object is missing."
                                {:type :datahike/gc-reference-missing-target :object-key address})))))
        ;; Seed an owned with-storage copy, never the stored/cached index. The
        ;; same seeded AEVT serves structural marking and reference slicing.
        bind (fn [idx root]
               (when idx (cond-> (with-storage (:index config) idx (:storage store))
                           root (-seed-root! root))))
        aevt (bind (:aevt-key record) (:aevt-root record))
        taevt (when (:keep-history? config)
                (bind (:temporal-aevt-key record) (:temporal-aevt-root record)))
        values (value-references config schema (or (:ident-ref-map schema-meta) (:ident-ref-map record)) aevt taevt context)
        dependencies (gc-ref/merge-contributions secondary values)
        families (cond-> [[:eavt-key :eavt-root] [:aevt-key :aevt-root] [:avet-key :avet-root]]
                   (:keep-history? config)
                   (into [[:temporal-eavt-key :temporal-eavt-root]
                          [:temporal-aevt-key :temporal-aevt-root]
                          [:temporal-avet-key :temporal-avet-root]]))
        primary (if (:discovery-only? opts)
                  #{}
                  (reduce (fn [acc [idx-key root-key]]
                            (if-let [idx (case idx-key :aevt-key aevt :temporal-aevt-key taevt
                                               (bind (get record idx-key) (get record root-key)))]
                              (set/union acc (mark-shared idx (:mark-context opts)))
                              acc)) #{} families))
        extras (set (:datahike.gc/keys record))]
    {:reachable (set/union #{key} extras primary (:reachable dependencies)
                           (if schema-key #{schema-key} #{}))
     :store-refs (set/union extras (:store-refs dependencies))
     :records (:records dependencies)
     :external-secondary-roots (:external-secondary-roots dependencies)
     ;; A historical building schema must not defer collection indefinitely.
     :building-secondary? (and (= key branch)
                               (or (= :building (get-in record [:avet-build :status]))
                                   (some #(= :building (:db.secondary/status %)) (vals schema))))}))

(defn reachable-in-branch [store branch after-date config schema-cache opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (try
                 (let [head-cid (<?- (k/get-in store [branch :meta :datahike/commit-id] nil opts))]
                   (loop [[{:keys [key parents? required?] :as entry} & pending]
                          [{:key branch :parents? (get opts :include-parents? true) :required? (:required-root? opts)}]
                          visited #{}
                          acc {:reachable (set (remove nil? [branch head-cid])) :store-refs #{}
                               :external-secondary-roots #{} :building-secondary? false}]
                     (if-not entry
                       acc
                       (let [visit-key [key parents? required?]]
                         (if (visited visit-key)
                           (recur pending visited acc)
                           (if-let [record (<?- (k/get store key nil opts))]
                             (let [record-config (target-config record config key required?)
                                   schema-key (:schema-meta-key record)
                                   schema-meta (when schema-key
                                                 (or (get @schema-cache schema-key)
                                                     (let [value (<?- (k/get store schema-key nil opts))]
                                                       (swap! schema-cache assoc schema-key value)
                                                       value)))
                                   edges (record-edges store branch key record record-config schema-meta opts required?)
                                   date (or (get-in record [:meta :datahike/updated-at])
                                            (get-in record [:meta :datahike/created-at]))
                                   parents (when (and parents? date (> (get-time date) (get-time after-date)))
                                             (map #(hash-map :key % :parents? true :required? false)
                                                  (get-in record [:meta :datahike/parents])))]
                               (recur (concat pending (:records edges) parents)
                                      (conj visited visit-key)
                                      {:reachable (set/union (:reachable acc) (:reachable edges))
                                       :store-refs (set/union (:store-refs acc) (:store-refs edges))
                                       :external-secondary-roots (set/union (:external-secondary-roots acc)
                                                                            (:external-secondary-roots edges))
                                       :building-secondary? (or (:building-secondary? acc) (:building-secondary? edges))}))
                             (if required?
                               (throw (ex-info "Required snapshot record is missing."
                                               {:type :datahike/gc-reference-missing-target :record-key key}))
                      ;; Missing optional ancestry is normal after a prior sweep
                      ;; or with the commit graph disabled. Exact edges are strict.
                               (recur pending (conj visited visit-key) acc))))))))
                 (catch #?(:clj Throwable :cljs :default) e
                   (throw (mark-failure e)))))))

(defn external-secondary-roots-in-branch
  "Discover external roots, including snapshot edges, without structurally marking
   primary indices. Only declared reference attributes require a datom scan."
  [store branch after-date config opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (:external-secondary-roots
                (<?- (reachable-in-branch store branch after-date config (atom {})
                                          (assoc opts :discovery-only? true)))))))

(defn reference-schema? [schema]
  (boolean (some #(= :db.type/gc-ref (:db/valueType %)) (vals schema))))

(defn require-reference-policy! [config]
  (when (or (not (:crypto-hash? config)) (get-in config [:online-gc :enabled?]))
    (throw (ex-info "GC reference values require immutable crypto addresses and full reachability GC."
                    {:type :datahike/gc-reference-unsafe-config}))))

(defn validate-db-references
  "Validate complete same-store dependencies before publication. This check is
   read-only and is not a barrier against a concurrent collector."
  [db opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (try
                 (when (reference-schema? (:schema db))
                   (require-reference-policy! (:config db))
                   (let [store (:store db)
                         context {:store store :config (:config db) :discovery-only? true
                                  :store-id (ds/canonical-store-id store (get-in db [:config :store]))}
                         contributions (value-references (:config db) (:schema db) (:ident-ref-map db)
                                                         (:aevt db) (:temporal-aevt db) context)]
                     (doseq [{:keys [key]} (:records contributions)]
                       (<?- (reachable-in-branch store key (#?(:clj Date. :cljs js/Date.) 0)
                                                 (:config db) (atom {})
                                                 (assoc opts :include-parents? false :required-root? true))))))
                 true
                 (catch #?(:clj Throwable :cljs :default) e
                   (throw (mark-failure e)))))))
