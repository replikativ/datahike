(ns ^:no-doc datahike.backfill.avet
  "Private AVET-family construction from an exact pinned source. The caller
   owns durable build sweep deferral and the local GC guard throughout building
   and publication. This is not schema activation or uniqueness validation."
  (:require [datahike.backfill.sort :as sort]
            [datahike.backfill.control :as control]
            [datahike.backfill.storage :as storage]
            [datahike.constants :as constants]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [datahike.datom Datom]))

(defn- tuple [^Datom d]
  [(.-e d) (.-a d) (.-v d) (dd/datom-tx d) (dd/datom-added d)])

(defn- datom [record]
  (dd/datom (nth record 0) (nth record 1) (nth record 2) (nth record 3) (nth record 4)))

(defn- merged-distinct
  "Merge sorted sequences with comparator equality, retaining one prior key."
  [cmp left right]
  (letfn [(step [left right previous]
            (lazy-seq
             (loop [left (seq left) right (seq right)]
               (when (or left right)
                 (control/check!)
                 (let [choose-left? (or (nil? right) (and left (<= (cmp (first left) (first right)) 0)))
                       value (if choose-left? (first left) (first right))
                       next-left (if choose-left? (next left) left)
                       next-right (if choose-left? right (next right))]
                   (if (and previous (zero? (cmp previous value)))
                     (recur next-left next-right)
                     (cons value (step next-left next-right value))))))))]
    (step left right nil)))

(defn- source-slice [source tree-key attribute]
  (di/-slice (get source tree-key)
             (dd/datom constants/e0 attribute nil constants/tx0)
             (dd/datom constants/emax attribute nil constants/txmax)
             :aevt))

(defn- build-tree! [source private attrs current? sort-options]
  (let [primary-key (if current? :aevt :temporal-aevt)
        avet-key (if current? :avet :temporal-avet)
        cmp (dd/index-type->cmp-quick :avet false)
        additions (mapcat #(map tuple (source-slice source primary-key %)) attrs)
        store (storage/build-store private)]
    (sort/with-sorted-records!
      additions (fn [a b] (cmp (datom a) (datom b))) sort-options
      (fn [records]
        (let [entries (merged-distinct cmp (get source avet-key) (map datom records))
              ;; Unlike init-index-sorted's :indexed filter, this native builder
              ;; preserves ALL old entries, including disabled historical attrs.
              tree (pss/from-sorted-seq
                    cmp entries {:storage private
                                 :meta (assoc (meta (get source avet-key)) :index-type :avet)
                                 :branching-factor (get store :datahike/branching-factor 512)
                                 :ref-type :weak
                                 :diff-buf-size 0})]
          (storage/flush! private)
          (di/with-storage :datahike.index/persistent-set tree private))))))

(defn close!
  "Release candidate buffers. Written private nodes remain protected by the
   caller until publication/cancellation and otherwise become ordinary GC garbage."
  [candidate]
  (.close ^java.io.Closeable (:storage candidate)))

(defn build!
  "Build current/temporal AVET without changing source schema or primary roots.
   attrs are existing attribute idents to add (already-indexed attrs are safe).
   Returns {:current tree :temporal tree-or-nil :storage owned-private-storage}.
   Caller closes successful results. Both sort scopes close before return.

   Sort and storage options retain their own documented budgets; source decoding,
   the builder frontier and caller-retained DBs are outside those buffer limits.
   Requires the private storage capability (JVM PSS, non-crypto, no diff buffers)."
  ([source attrs] (build! source attrs nil))
  ([source attrs options]
   (when-not (and (or (nil? options) (map? options))
                  (every? #{:sort :storage} (keys options))
                  (or (set? attrs) (sequential? attrs)))
     (throw (ex-info "Invalid AVET build options or attributes." {:type ::invalid-options})))
   (let [attrs (reduce (fn [seen ident]
                         (when-not (and (keyword? ident) (map? (get-in source [:schema ident])))
                           (throw (ex-info "AVET build requires an existing attribute ident."
                                           {:type ::invalid-attribute :attribute ident})))
                         (conj seen ident))
                       #{} attrs)
         resolved (mapv (fn [ident]
                          (if (get-in source [:config :attribute-refs?])
                            (or (get-in source [:ident-ref-map ident])
                                (throw (ex-info "Missing attribute reference." {:type ::invalid-attribute :attribute ident})))
                            ident)) attrs)
         sort-options (sort/validate-options! (:sort options))
         private (storage/create! (:store source) (:config source) (:storage options))]
     (try
       (let [source (reduce (fn [source key]
                              (if-let [tree (get source key)]
                                (assoc source key (storage/copy-index!
                                                   private tree
                                                   {:resident-source? (= :memory (get-in source [:config :store :backend]))}))
                                source))
                            source [:aevt :avet :temporal-aevt :temporal-avet])]
         {:current (build-tree! source private resolved true sort-options)
          :temporal (when (get-in source [:config :keep-history?])
                      (build-tree! source private resolved false sort-options))
          :storage private})
       (catch Throwable e
         (try (.close ^java.io.Closeable private)
              (catch Throwable cleanup-error (.addSuppressed e cleanup-error)))
         (throw e))))))
