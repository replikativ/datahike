(ns ^:no-doc datahike.db
  (:require
   #?(:cljs [goog.array :as garray])
   [clojure.walk]
   [clojure.core.cache.wrapped :as cw]
   [clojure.data]
   #?(:clj [clojure.pprint :as pp])
   [datahike.array :refer [a=]]
   [datahike.index :refer [-slice -seq -count -all -persistent! -transient] :as di]
   [datahike.datom :as dd :refer [datom datom-tx datom-added datom?]]
   [datahike.constants :as c :refer [ue0 e0 tx0 utx0 emax txmax system-schema]]
   [datahike.tools :refer [get-time case-tree raise] :as tools]
   [datahike.schema :as ds]
   [datahike.lru :refer [lru-datom-cache-factory]]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [datahike.config :as dc]
   [environ.core :refer [env]]
   [clojure.spec.alpha :as s]
   [taoensso.timbre :refer [warn]])
  #?(:cljs (:require-macros [datahike.db :refer [defrecord-updatable cond+]]
                            [datahike.datom :refer [combine-cmp datom]]
                            [datahike.tools :refer [case-tree raise]]))
  (:refer-clojure :exclude [seqable?])
  #?(:clj (:import [clojure.lang AMapEntry]
                   [java.util Date]
                   [datahike.datom Datom])))

;; ----------------------------------------------------------------------------

#?(:cljs
   (do
     (def Exception js/Error)
     (def IllegalArgumentException js/Error)
     (def UnsupportedOperationException js/Error)))

;; ----------------------------------------------------------------------------

(defn #?@(:clj [^Boolean seqable?]
          :cljs [^boolean seqable?])
  [x]
  (and (not (string? x))
       #?(:cljs (or (cljs.core/seqable? x)
                    (arrays/array? x))
          :clj (or (seq? x)
                   (instance? clojure.lang.Seqable x)
                   (nil? x)
                   (instance? Iterable x)
                   (arrays/array? x)
                   (instance? java.util.Map x)))))

;; ----------------------------------------------------------------------------
;; macros and funcs to support writing defrecords and updating
;; (replacing) builtins, i.e., Object/hashCode, IHashEq hasheq, etc.
;; code taken from prismatic:
;;  https://github.com/Prismatic/schema/commit/e31c419c56555c83ef9ee834801e13ef3c112597
;;

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

#?(:clj
   (defn- get-sig [method]
     ;; expects something like '(method-symbol [arg arg arg] ...)
     ;; if the thing matches, returns [fully-qualified-symbol arity], otherwise nil
     (and (sequential? method)
          (symbol? (first method))
          (vector? (second method))
          (let [sym (first method)
                ns (or (some->> sym resolve meta :ns str) "clojure.core")]
            [(symbol ns (name sym)) (-> method second count)]))))

#?(:clj
   (defn- dedupe-interfaces [deftype-form]
     ;; get the interfaces list, remove any duplicates, similar to remove-nil-implements in potemkin
     ;; verified w/ deftype impl in compiler:
     ;; (deftype* tagname classname [fields] :implements [interfaces] :tag tagname methods*)
     (let [[deftype* tagname classname fields implements interfaces & rest] deftype-form]
       (when (or (not= deftype* 'deftype*) (not= implements :implements))
         (throw (IllegalArgumentException. "deftype-form mismatch")))
       (list* deftype* tagname classname fields implements (vec (distinct interfaces)) rest))))

#?(:clj
   (defn- make-record-updatable-clj [name fields & impls]
     (let [impl-map (->> impls (map (juxt get-sig identity)) (filter first) (into {}))
           body (macroexpand-1 (list* 'defrecord name fields impls))]
       (clojure.walk/postwalk
        (fn [form]
          (if (and (sequential? form) (= 'deftype* (first form)))
            (->> form
                 dedupe-interfaces
                 (remove (fn [method]
                           (when-let [impl (-> method get-sig impl-map)]
                             (not= method impl)))))
            form))
        body))))

#?(:clj
   (defn- make-record-updatable-cljs [name fields & impls]
     `(do
        (defrecord ~name ~fields)
        (extend-type ~name ~@impls))))

#?(:clj
   (defmacro defrecord-updatable [name fields & impls]
     `(if-cljs
       ~(apply make-record-updatable-cljs name fields impls)
       ~(apply make-record-updatable-clj name fields impls))))

;; ----------------------------------------------------------------------------

;;;;;;;;;; Searching

(defprotocol ISearch
  (-search [data pattern]))

(defprotocol IIndexAccess
  (-datoms [db index components])
  (-seek-datoms [db index components])
  (-rseek-datoms [db index components])
  (-index-range [db attr start end]))

(defprotocol IDB
  (-schema [db])
  (-rschema [db])
  (-system-entities [db])
  (-attrs-by [db property])
  (-max-tx [db])
  (-max-eid [db])
  (-temporal-index? [db])                                   ;;deprecated
  (-keep-history? [db])
  (-config [db])
  (-ref-for [db a-ident])
  (-ident-for [db a-ref]))

(defprotocol IHistory
  (-time-point [db])
  (-origin [db]))

;; ----------------------------------------------------------------------------

(declare hash-datoms equiv-db empty-db resolve-datom validate-attr validate-attr-ident components->pattern indexing? no-history? multival? search-current-indices search-temporal-indices)
#?(:cljs (declare pr-db))

(defn db-transient [db]
  (-> db
      (update :eavt -transient)
      (update :aevt -transient)
      (update :avet -transient)))

(defn db-persistent! [db]
  (-> db
      (update :eavt -persistent!)
      (update :aevt -persistent!)
      (update :avet -persistent!)))

(defn- search-indices
  "Assumes correct pattern form, i.e. refs for ref-database"
  [eavt aevt avet pattern indexed? temporal-db?]
  (let [[e a v tx added?] pattern]
    (if (and (not temporal-db?) (false? added?))
      '()
      (case-tree [e a (some? v) tx]
                 [(-slice eavt (datom e a v tx) (datom e a v tx) :eavt) ;; e a v tx
                  (-slice eavt (datom e a v tx0) (datom e a v txmax) :eavt) ;; e a v _
                  (->> (-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt) ;; e a _ tx
                       (filter (fn [^Datom d] (= tx (datom-tx d)))))
                  (-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt) ;; e a _ _
                  (->> (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ v tx
                       (filter (fn [^Datom d] (and (a= v (.-v d))
                                                   (= tx (datom-tx d))))))
                  (->> (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ v _
                       (filter (fn [^Datom d] (a= v (.-v d)))))
                  (->> (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ _ tx
                       (filter (fn [^Datom d] (= tx (datom-tx d)))))
                  (-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt) ;; e _ _ _
                  (if indexed?                              ;; _ a v tx
                    (->> (-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet)
                         (filter (fn [^Datom d] (= tx (datom-tx d)))))
                    (->> (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
                         (filter (fn [^Datom d] (and (a= v (.-v d))
                                                     (= tx (datom-tx d)))))))
                  (if indexed?                              ;; _ a v _
                    (-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet)
                    (->> (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
                         (filter (fn [^Datom d] (a= v (.-v d))))))
                  (->> (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt) ;; _ a _ tx
                       (filter (fn [^Datom d] (= tx (datom-tx d)))))
                  (-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt) ;; _ a _ _
                  (filter (fn [^Datom d] (and (a= v (.-v d)) (= tx (datom-tx d)))) (-all eavt)) ;; _ _ v tx
                  (filter (fn [^Datom d] (a= v (.-v d))) (-all eavt)) ;; _ _ v _
                  (filter (fn [^Datom d] (= tx (datom-tx d))) (-all eavt)) ;; _ _ _ tx
                  (-all eavt)]))))

(defrecord-updatable DB [schema eavt aevt avet temporal-eavt temporal-aevt temporal-avet max-eid max-tx op-count rschema hash config system-entities ident-ref-map ref-ident-map meta]
  #?@(:cljs
      [IHash (-hash [db] hash)
       IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (-seq (.-eavt db)))
       IReversible (-rseq [db] (-rseq (.-eavt db)))
       ICounted (-count [db] (count (.-eavt db)))
       IEmptyableCollection (-empty [db] (empty-db (ds/get-user-schema db)))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))
       IEditableCollection (-as-transient [db] (db-transient db))
       ITransientCollection (-conj! [db key] (throw (ex-info "datahike.DB/conj! is not supported" {})))
       (-persistent! [db] (db-persistent! db))]

      :clj
      [Object (hashCode [db] hash)
       clojure.lang.IHashEq (hasheq [db] hash)
       clojure.lang.Seqable (seq [db] (-seq eavt))
       clojure.lang.IPersistentCollection
       (count [db] (-count eavt))
       (equiv [db other] (equiv-db db other))
       (empty [db] (empty-db (ds/get-user-schema db)))
       clojure.lang.IEditableCollection
       (asTransient [db] (db-transient db))
       clojure.lang.ITransientCollection
       (conj [db key] (throw (ex-info "datahike.DB/conj! is not supported" {})))
       (persistent [db] (db-persistent! db))])

  IDB
  (-schema [db] (.-schema db))
  (-rschema [db] (.-rschema db))
  (-system-entities [db] (.-system-entities db))
  (-attrs-by [db property] ((.-rschema db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-> db -config :keep-history?))
  (-max-tx [db] (.-max-tx db))
  (-max-eid [db] (.-max-eid db))
  (-config [db] (.-config db))
  (-ref-for [db a-ident]
            (if (:attribute-refs? (.-config db))
              (let [ref (get (.-ident-ref-map db) a-ident)]
                (when (nil? ref)
                  (warn (str "Attribute " a-ident " has not been found in database")))
                ref)
              a-ident))
  (-ident-for [db a-ref]
              (if (:attribute-refs? (.-config db))
                (let [a-ident (get (.-ref-ident-map db) a-ref)]
                  (when (nil? a-ident)
                    (warn (str "Attribute with reference number " a-ref " has not been found in database")))
                  a-ident)
                a-ref))

  ISearch
  (-search [db pattern]
           (search-current-indices db pattern))

  IIndexAccess
  (-datoms [db index-type cs]
           (-slice (get db index-type)
                   (components->pattern db index-type cs e0 tx0)
                   (components->pattern db index-type cs emax txmax)
                   index-type))

  (-seek-datoms [db index-type cs]
                (-slice (get db index-type)
                        (components->pattern db index-type cs e0 tx0)
                        (datom emax nil nil txmax)
                        index-type))

  (-rseek-datoms [db index-type cs]
                 (-> (-slice (get db index-type)
                             (components->pattern db index-type cs e0 tx0)
                             (datom emax nil nil txmax)
                             index-type)
                     vec
                     rseq))

  (-index-range [db attr start end]
                (when-not (indexing? db attr)
                  (raise "Attribute" attr "should be marked as :db/index true" {}))
                (validate-attr attr (list '-index-range 'db attr start end) db)
                (-slice avet
                        (resolve-datom db nil attr start nil e0 tx0)
                        (resolve-datom db nil attr end nil emax txmax)
                        :avet))

  clojure.data/EqualityPartition
  (equality-partition [x] :datahike/db)

  clojure.data/Diff
  (diff-similar [a b]
                (let [datoms-a (-slice (:eavt a) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt)
                      datoms-b (-slice (:eavt b) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt)]
                  (dd/diff-sorted datoms-a datoms-b dd/cmp-datoms-eavt-quick))))

(defn db? [x]
  (and (satisfies? ISearch x)
       (satisfies? IIndexAccess x)
       (satisfies? IDB x)))

;; ----------------------------------------------------------------------------
(defrecord-updatable FilteredDB [unfiltered-db pred]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (-datoms db :eavt []))
       ICounted (-count [db] (count (-datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on FilteredDB")))
       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on FilteredDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on FilteredDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on FilteredDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on FilteredDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on FilteredDB")))]

      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (-datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on FilteredDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on FilteredDB")))

       clojure.lang.Seqable (seq [db] (-datoms db :eavt []))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on FilteredDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on FilteredDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on FilteredDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on FilteredDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on FilteredDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on FilteredDB")))])

  IDB
  (-schema [db] (-schema (.-unfiltered-db db)))
  (-rschema [db] (-rschema (.-unfiltered-db db)))
  (-system-entities [db] (-system-entities (.-unfiltered-db db)))
  (-attrs-by [db property] (-attrs-by (.-unfiltered-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-unfiltered-db db)))
  (-max-tx [db] (-max-tx (.-unfiltered-db db)))
  (-max-eid [db] (-max-eid (.-unfiltered-db db)))
  (-config [db] (-config (.-unfiltered-db db)))
  (-ref-for [db a-ident] (-ref-for (.-unfiltered-db db) a-ident))
  (-ident-for [db a-ref] (-ident-for (.-unfiltered-db db) a-ref))

  ISearch
  (-search [db pattern]
           (filter (.-pred db) (-search (.-unfiltered-db db) pattern)))

  IIndexAccess
  (-datoms [db index cs]
           (filter (.-pred db) (-datoms (.-unfiltered-db db) index cs)))

  (-seek-datoms [db index cs]
                (filter (.-pred db) (-seek-datoms (.-unfiltered-db db) index cs)))

  (-rseek-datoms [db index cs]
                 (filter (.-pred db) (-rseek-datoms (.-unfiltered-db db) index cs)))

  (-index-range [db attr start end]
                (filter (.-pred db) (-index-range (.-unfiltered-db db) attr start end))))

(defn distinct-datoms [db current-datoms history-datoms]
  (if  (-keep-history? db)
    (concat (filter #(or (no-history? db (:a %))
                         (multival? db (:a %)))
                    current-datoms)
            history-datoms)
    current-datoms))

(defn temporal-search [^DB db pattern]
  (distinct-datoms db
                   (search-current-indices db pattern)
                   (search-temporal-indices db pattern)))

(defn temporal-datoms [^DB db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (components->pattern db index-type cs e0 tx0)
        to (components->pattern db index-type cs emax txmax)]
    (distinct-datoms db
                     (-slice index from to index-type)
                     (-slice temporal-index from to index-type))))

(defn temporal-seek-datoms [^DB db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (components->pattern db index-type cs e0 tx0)
        to (datom emax nil nil txmax)]
    (distinct-datoms db
                     (-slice index from to index-type)
                     (-slice temporal-index from to index-type))))

(defn temporal-rseek-datoms [^DB db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (components->pattern db index-type cs e0 tx0)
        to (datom emax nil nil txmax)]
    (concat
     (-> (distinct-datoms db
                          (-slice index from to index-type)
                          (-slice temporal-index from to index-type))
         vec
         rseq))))

(defn temporal-index-range [^DB db current-db attr start end]
  (when-not (indexing? db attr)
    (raise "Attribute" attr "should be marked as :db/index true" {}))
  (validate-attr attr (list '-index-range 'db attr start end) db)
  (let [from (resolve-datom current-db nil attr start nil e0 tx0)
        to (resolve-datom current-db nil attr end nil emax txmax)]
    (distinct-datoms db
                     (-slice (get db :avet) from to :avet)
                     (-slice (get db :temporal-avet) from to :avet))))

(defrecord-updatable HistoricalDB [origin-db]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (-datoms db :eavt []))
       ICounted (-count [db] (count (-datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on HistoricalDB")))
       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on HistoricalDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on HistoricalDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on HistoricalDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on HistoricalDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on HistoricalDB")))]
      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (-datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on HistoricalDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on HistoricalDB")))

       clojure.lang.Seqable (seq [db] (-datoms db :eavt []))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on HistoricalDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on HistoricalDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on HistoricalDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on HistoricalDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on HistoricalDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on HistoricalDB")))])

  IDB
  (-schema [db] (-schema (.-origin-db db)))
  (-rschema [db] (-rschema (.-origin-db db)))
  (-system-entities [db] (-system-entities (.-origin-db db)))
  (-attrs-by [db property] (-attrs-by (.-origin-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-origin-db db)))
  (-max-tx [db] (-max-tx (.-origin-db db)))
  (-max-eid [db] (-max-eid (.-origin-db db)))
  (-config [db] (-config (.-origin-db db)))
  (-ref-for [db a-ident] (-ref-for (.-origin-db db) a-ident))
  (-ident-for [db a-ref] (-ident-for (.-origin-db db) a-ref))

  IHistory
  (-time-point [db] (.-time-point db))
  (-origin [db] (.-origin-db db))

  ISearch
  (-search [db pattern]
           (temporal-search (.-origin-db db) pattern))

  IIndexAccess
  (-datoms [db index-type cs] (temporal-datoms (.-origin-db db) index-type cs))

  (-seek-datoms [db index-type cs] (temporal-seek-datoms (.-origin-db db) index-type cs))

  (-rseek-datoms [db index-type cs] (temporal-rseek-datoms (.-origin-db db) index-type cs))

  (-index-range [db attr start end] (temporal-index-range (.-origin-db db) db attr start end)))

(defn filter-txInstant [datoms pred db]
  (let [txInstant (if (:attribute-refs? (.-config db))
                    (-ref-for db :db/txInstant)
                    :db/txInstant)]
    (into #{}
          (comp
           (map datom-tx)
           (distinct)
           (mapcat (fn [tx] (temporal-datoms db :eavt [tx])))
           (keep (fn [^Datom d]
                   (when (and (= txInstant (.-a d)) (pred d))
                     (.-e d)))))
          datoms)))

(defn get-current-values [rschema datoms]
  (->> datoms
       (filter datom-added)
       (group-by (fn [^Datom datom] [(.-e datom) (.-a datom)]))
       (mapcat
        (fn [[[_ a] entities]]
          (if (contains? (get-in rschema [:db.cardinality/many]) a)
            entities
            [(reduce (fn [^Datom datom-0 ^Datom datom-1]
                       (if (> (datom-tx datom-0) (datom-tx datom-1))
                         datom-0
                         datom-1)) entities)])))))

(defn- date? [d]
  #?(:cljs (instance? js/Date d)
     :clj (instance? Date d)))

(defn filter-as-of-datoms [datoms time-point db]
  (let [as-of-pred (fn [^Datom d]
                     (if (date? time-point)
                       (.before ^Date (.-v d) ^Date time-point)
                       (<= (dd/datom-tx d) time-point)))

        filtered-tx-ids (filter-txInstant datoms as-of-pred db)
        filtered-datoms (->> datoms
                             (filter (fn [^Datom d] (contains? filtered-tx-ids (datom-tx d))))
                             (get-current-values (-rschema db)))]
    filtered-datoms))

(defrecord-updatable AsOfDB [origin-db time-point]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (-datoms db :eavt []))
       ICounted (-count [db] (count (-datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on AsOfDB")))
       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on AsOfDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on AsOfDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on AsOfDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on AsOfDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on AsOfDB")))]
      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (-datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on AsOfDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on AsOfDB")))

       clojure.lang.Seqable (seq [db] (-datoms db :eavt []))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on AsOfDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on AsOfDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on AsOfDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on AsOfDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on AsOfDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on AsOfDB")))])

  IDB
  (-schema [db] (-schema (.-origin-db db)))
  (-rschema [db] (-rschema (.-origin-db db)))
  (-system-entities [db] (-system-entities (.-origin-db db)))
  (-attrs-by [db property] (-attrs-by (.-origin-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-origin-db db)))
  (-max-tx [db] (-max-tx (.-origin-db db)))
  (-max-eid [db] (-max-eid (.-origin-db db)))
  (-config [db] (-config (.-origin-db db)))
  (-ref-for [db a-ident] (-ref-for (.-origin-db db) a-ident))
  (-ident-for [db a-ref] (-ident-for (.-origin-db db) a-ref))

  IHistory
  (-time-point [db] (.-time-point db))
  (-origin [db] (.-origin-db db))

  ISearch
  (-search [db pattern]
           (let [origin-db (.-origin-db db)]
             (-> (temporal-search origin-db pattern)
                 (filter-as-of-datoms (.-time-point db) origin-db))))

  IIndexAccess
  (-datoms [db index-type cs]
           (let [origin-db (.-origin-db db)]
             (-> (temporal-datoms origin-db index-type cs)
                 (filter-as-of-datoms (.-time-point db) origin-db))))

  (-seek-datoms [db index-type cs]
                (let [origin-db (.-origin-db db)]
                  (-> (temporal-seek-datoms origin-db index-type cs)
                      (filter-as-of-datoms (.-time-point db) origin-db))))

  (-rseek-datoms [db index-type cs]
                 (let [origin-db (.-origin-db db)]
                   (-> (temporal-rseek-datoms origin-db index-type cs)
                       (filter-as-of-datoms (.-time-point db) origin-db))))

  (-index-range [db attr start end]
                (let [origin-db (.-origin-db db)]
                  (-> (temporal-index-range origin-db db attr start end)
                      (filter-as-of-datoms (.-time-point db) origin-db)))))

(defn- filter-since [datoms time-point db]
  (let [since-pred (fn [^Datom d]
                     (if (date? time-point)
                       (.after ^Date (.-v d) ^Date time-point)
                       (>= (.-tx d) time-point)))
        filtered-tx-ids (filter-txInstant datoms since-pred db)]
    (->> datoms
         (filter datom-added)
         (filter (fn [^Datom d] (contains? filtered-tx-ids (datom-tx d)))))))

(defn- filter-before [datoms ^Date before-date db]
  (let [before-pred (fn [^Datom d]
                      (.before ^Date (.-v d) before-date))
        filtered-tx-ids (filter-txInstant datoms before-pred db)]
    (filter
     (fn [^Datom d]
       (contains? filtered-tx-ids (datom-tx d)))
     datoms)))

(defrecord-updatable SinceDB [origin-db time-point]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (-datoms db :eavt []))
       ICounted (-count [db] (count (-datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on SinceDB")))
       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on SinceDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on SinceDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on SinceDB"))))

       IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on SinceDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on SinceDB")))]
      :clj
      [clojure.lang.IPersistentCollection
       (count [db] (count (-datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on SinceDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on SinceDB")))

       clojure.lang.Seqable (seq [db] (-datoms db :eavt []))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on SinceDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on SinceDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on SinceDB")))

       clojure.lang.Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on SinceDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on SinceDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on SinceDB")))])

  IDB
  (-schema [db] (-schema (.-origin-db db)))
  (-rschema [db] (-rschema (.-origin-db db)))
  (-system-entities [db] (-system-entities (.-origin-db db)))
  (-attrs-by [db property] (-attrs-by (.-origin-db db) property))
  (-temporal-index? [db] (-keep-history? db))
  (-keep-history? [db] (-keep-history? (.-origin-db db)))
  (-max-tx [db] (-max-tx (.-origin-db db)))
  (-max-eid [db] (-max-eid (.-origin-db db)))
  (-config [db] (-config (.-origin-db db)))
  (-ref-for [db a-ident] (-ref-for (.-origin-db db) a-ident))
  (-ident-for [db a-ref] (-ident-for (.-origin-db db) a-ref))

  IHistory
  (-time-point [db] (.-time-point db))
  (-origin [db] (.-origin-db db))

  ISearch
  (-search [db pattern]
           (let [origin-db (.-origin-db db)]
             (-> (temporal-search origin-db pattern)
                 (filter-since (.-time-point db) origin-db))))

  IIndexAccess
  (-datoms [db index-type cs]
           (let [origin-db (.-origin-db db)]
             (-> (temporal-datoms origin-db index-type cs)
                 (filter-since (.-time-point db) origin-db))))

  (-seek-datoms [db index-type cs]
                (let [origin-db (.-origin-db db)]
                  (-> (temporal-seek-datoms origin-db index-type cs)
                      (filter-since (.-time-point db) origin-db))))

  (-rseek-datoms [db index-type cs]
                 (let [origin-db (.-origin-db db)]
                   (-> (temporal-rseek-datoms origin-db index-type cs)
                       (filter-since (.-time-point db) origin-db))))

  (-index-range [db attr start end]
                (let [origin-db (.-origin-db db)]
                  (-> (temporal-index-range origin-db db attr start end)
                      (filter-since (.-time-point db) origin-db)))))

;; ----------------------------------------------------------------------------

(def db-caches (cw/lru-cache-factory {} :threshold (:datahike-max-db-caches env 5)))

(defn memoize-for [^DB db key f]
  (if (or (zero? (or (:cache-size (.-config db)) 0))
          (zero? (.-hash db))) ;; empty db
    (f)
    (let [db-cache (cw/lookup-or-miss db-caches
                                      (.-hash db)
                                      (fn [_] (lru-datom-cache-factory {} :threshold (:cache-size (.-config db)))))]
      (cw/lookup-or-miss db-cache key (fn [_] (f))))))

(defn- search-current-indices [^DB db pattern]
  (memoize-for db [:search pattern]
               #(let [[_ a _ _] pattern]
                  (search-indices (.-eavt db)
                                  (.-aevt db)
                                  (.-avet db)
                                  pattern
                                  (indexing? db a)
                                  false))))

(defn- search-temporal-indices [^DB db pattern]
  (memoize-for db [:temporal-search pattern]
               #(let [[_ a _ _ added] pattern
                      result (search-indices (.-temporal-eavt db)
                                             (.-temporal-aevt db)
                                             (.-temporal-avet db)
                                             pattern
                                             (indexing? db a)
                                             true)]
                  (case added
                    true (filter datom-added result)
                    false (remove datom-added result)
                    nil result))))

(defn attr->properties [k v]
  (case v
    :db.unique/identity [:db/unique :db.unique/identity :db/index]
    :db.unique/value [:db/unique :db.unique/value :db/index]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref [:db.type/ref :db/index]
    :db.type/tuple [:db.type/tuple]
    (if (= k :db/ident)
      [:db/ident]
      (when (true? v)
        (case k
          :db/isComponent [:db/isComponent]
          :db/index [:db/index]
          :db/noHistory [:db/noHistory]
          [])))))

(defn reduce-indexed
  "Same as reduce, but `f` takes [acc el idx]"
  [f init xs]
  (first
   (reduce
    (fn [[acc idx] x]
      (let [res (f acc x idx)]
        (if (reduced? res)
          (reduced [res idx])
          [res (inc idx)])))
    [init 0]
    xs)))

(defn- attrTuples
  "For each attribute involved in a composite tuple, returns a map made of the tuple attribute it is involved in, plus its position in the tuple.
  E.g. {:a => {:a+b+c 0, :a+d 0}
        :b => {:a+b+c 1}
        ... }"
  [schema rschema]
  (reduce
   (fn [m tuple-attr]
     (reduce-indexed
      (fn [m attr idx]
        (update m attr assoc tuple-attr idx))
      m
      (-> schema tuple-attr :db/tupleAttrs)))
   {}
   (:db.type/tuple rschema)))

(defn- rschema [schema]
  (let [rschema (reduce-kv
                 (fn [m attr keys->values]
                   (if (keyword? keys->values)
                     m
                     (reduce-kv
                      (fn [m key value]
                        (reduce
                         (fn [m prop]
                           (assoc m prop (conj (get m prop #{}) attr)))
                         m (attr->properties key value)))
                      (update m :db/ident (fn [coll] (if coll (conj coll attr) #{attr}))) keys->values)))
                 {} schema)]
    (assoc rschema :db/attrTuples (attrTuples schema rschema))))

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for " (pr-str {a {k v}}) ", expected one of " expected)
                    {:error :schema/validation
                     :attribute a
                     :key k
                     :value v}))))

(defn- validate-tuple-schema [a kv]
  (when (= :db.type/tuple (:db/valueType kv))
    (case (some #{:db/tupleAttrs :db/tupleTypes :db/tupleType} (keys kv))
      :db/tupleAttrs (when (not (vector? (:db/tupleAttrs kv)))
                       (throw (ex-info (str "Bad attribute specification for " a ": {:db/tupleAttrs ...} should be a vector}")
                                       {:error     :schema/validation
                                        :attribute a
                                        :key       :db/tupleAttrs})))
      :db/tupleTypes (when (not (vector? (:db/tupleTypes kv)))
                       (throw (ex-info (str "Bad attribute specification for " a ": {:db/tupleTypes ...} should be a vector}")
                                       {:error     :schema/validation
                                        :attribute a
                                        :key       :db/tupleTypes})))
      :db/tupleType  (when (not (keyword? (:db/tupleType kv)))
                       (throw (ex-info (str "Bad attribute specification for " a ": {:db/tupleType ...} should be a keyword}")
                                       {:error     :schema/validation
                                        :attribute a
                                        :key       :db/tupleType}))))))

(defn- validate-schema [schema]
  (doseq [[a-ident kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a-ident :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not= (:db/valueType kv) :db.type/ref))
        (throw (ex-info (str "Bad attribute specification for " a-ident ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}")
                        {:error :schema/validation
                         :attribute a-ident
                         :key :db/isComponent}))))
    (validate-schema-key a-ident :db/unique (:db/unique kv) #{:db.unique/value :db.unique/identity})
    (validate-schema-key a-ident :db/valueType (:db/valueType kv) #{:db.type/ref :db.type/tuple})
    (validate-schema-key a-ident :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many})
    (validate-tuple-schema a-ident kv)))

(defn to-old-schema [new-schema]
  (if (or (vector? new-schema) (seq? new-schema))
    (reduce
     (fn [acc {:keys [:db/ident] :as schema-entity}]
       (assoc acc ident schema-entity))
     {}
     new-schema)
    new-schema))

(defn- validate-write-schema [schema]
  (when-not (ds/old-schema-valid? schema)
    (raise "Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
           (ds/explain-old-schema schema))))

(defn- max-system-eid []
  (->> system-schema
       rest
       (map first)
       (apply max)))

(defn init-max-eid [eavt]
  ;; solved with reserse slice first in datascript
  (if-let [datoms (-slice
                   eavt
                   (datom e0 nil nil tx0)
                   (datom (dec tx0) nil nil txmax)
                   :eavt)]
    (-> datoms vec rseq first :e)                           ;; :e of last datom in slice
    e0))

(defn get-max-tx [eavt]
  (transduce (map (fn [^Datom d] (datom-tx d))) max tx0 (-all eavt)))

(def ref-datoms                                             ;; maps enums as well
  (let [idents (reduce (fn [m {:keys [db/ident db/id]}]
                         (assoc m ident id))
                       {}
                       system-schema)]
    (->> system-schema
         (mapcat
          (fn [{:keys [db/id] :as i}]
            (reduce-kv
             (fn [coll k v]
               (let [k-ref (idents k)]
                 (if (= k :db/ident)
                   (conj coll (dd/datom id k-ref v tx0))
                   (if-let [v-ref (idents v)]
                     (conj coll (dd/datom id k-ref v-ref tx0))
                     (conj coll (dd/datom id k-ref v tx0))))))
             []
             (dissoc i :db/id))))
         vec)))

(defn get-ident-ref-map
  "Maps IDs of system entities to their names (keyword) and attribute names to the attribute's specification"
  [schema]
  (reduce
   (fn [m [a {:keys [db/id]}]]
     (when a
       (assoc m a id)))
   {}
   schema))

(defn ^DB empty-db
  "Prefer create-database in api, schema only in index for attribute reference database."
  ([] (empty-db nil nil))
  ([schema] (empty-db schema nil))
  ([schema user-config]
   {:pre [(or (nil? schema) (map? schema) (coll? schema))]}
   (let [complete-config (merge (dc/storeless-config) user-config)
         _ (dc/validate-config complete-config)
         {:keys [keep-history? index schema-flexibility attribute-refs?]} complete-config
         on-read? (= :read schema-flexibility)
         schema (to-old-schema schema)
         _ (if on-read?
             (validate-schema schema)
             (validate-write-schema schema))
         complete-schema (merge schema
                                (if attribute-refs?
                                  c/ref-implicit-schema
                                  c/non-ref-implicit-schema))
         rschema (rschema complete-schema)
         ident-ref-map (if attribute-refs? (get-ident-ref-map complete-schema) {})
         ref-ident-map (if attribute-refs? (clojure.set/map-invert ident-ref-map) {})
         system-entities (if attribute-refs? c/system-entities #{})
         indexed (if attribute-refs?
                   (set (map ident-ref-map (:db/index rschema)))
                   (:db/index rschema))
         index-config (merge (:index-config complete-config)
                             {:indexed indexed})
         eavt (if attribute-refs?
                (di/init-index index ref-datoms :eavt 0 index-config)
                (di/empty-index index :eavt index-config))
         aevt (if attribute-refs?
                (di/init-index index ref-datoms :aevt 0 index-config)
                (di/empty-index index :aevt index-config))
         indexed-datoms (filter (fn [[_ a _ _]] (contains? indexed a)) ref-datoms)
         avet (if attribute-refs?
                (di/init-index index indexed-datoms :avet 0 index-config)
                (di/empty-index index :avet index-config))
         max-eid (if attribute-refs? ue0 e0)
         max-tx (if attribute-refs? utx0 tx0)]
     (map->DB
      (merge
       {:schema complete-schema
        :rschema rschema
        :config complete-config
        :eavt eavt
        :aevt aevt
        :avet avet
        :max-eid max-eid
        :max-tx max-tx
        :hash 0
        :system-entities system-entities
        :ref-ident-map ref-ident-map
        :ident-ref-map ident-ref-map
        :meta (tools/meta-data)
        :op-count (if attribute-refs? (count ref-datoms) 0)}
       (when keep-history?                                  ;; no difference for attribute references since no update possible
         {:temporal-eavt (di/empty-index index :eavt index-config)
          :temporal-aevt (di/empty-index index :aevt index-config)
          :temporal-avet (di/empty-index index :avet index-config)}))))))

(defn advance-all-datoms [datoms offset]
  (map
   (fn [^Datom d]
     (datom (+ (.-e d) offset) (.-a d) (.-v d) (.-tx d)))
   datoms))

(defn get-max-tx [eavt]
  (transduce (map (fn [^Datom d] (datom-tx d))) max tx0 (-all eavt)))

(defn ^DB init-db
  ([datoms] (init-db datoms nil nil))
  ([datoms schema] (init-db datoms schema nil))
  ([datoms schema user-config]
   (validate-schema schema)
   (let [{:keys [index schema-flexibility keep-history? attribute-refs?] :as complete-config}  (merge (dc/storeless-config) user-config)
         _ (dc/validate-config complete-config)
         complete-schema (merge schema
                                (if attribute-refs?
                                  c/ref-implicit-schema
                                  c/non-ref-implicit-schema))
         rschema (rschema complete-schema)
         ident-ref-map (if attribute-refs? (get-ident-ref-map schema) {})
         ref-ident-map (if attribute-refs? (clojure.set/map-invert ident-ref-map) {})
         system-entities (if attribute-refs? c/system-entities #{})

         indexed (if attribute-refs?
                   (set (map ident-ref-map (:db/index rschema)))
                   (:db/index rschema))
         new-datoms (if attribute-refs? (concat ref-datoms datoms) datoms)
         indexed-datoms (filter (fn [[_ a _ _]] (contains? indexed a)) new-datoms)
         op-count 0
         index-config (assoc (:index-config complete-config)
                             :indexed indexed)
         avet (di/init-index index indexed-datoms :avet op-count index-config)
         eavt (di/init-index index new-datoms :eavt op-count index-config)
         aevt (di/init-index index new-datoms :aevt op-count index-config)
         max-eid (init-max-eid eavt)
         max-tx (get-max-tx eavt)
         op-count (count new-datoms)]
     (map->DB (merge {:schema complete-schema
                      :rschema rschema
                      :config complete-config
                      :eavt eavt
                      :aevt aevt
                      :avet avet
                      :max-eid max-eid
                      :max-tx max-tx
                      :op-count op-count
                      :hash (hash-datoms datoms)
                      :system-entities system-entities
                      :meta (tools/meta-data)
                      :ref-ident-map ref-ident-map
                      :ident-ref-map ident-ref-map}
                     (when keep-history?
                       {:temporal-eavt (di/empty-index index :eavt index-config)
                        :temporal-aevt (di/empty-index index :aevt index-config)
                        :temporal-avet (di/empty-index index :avet index-config)}))))))

(defn- equiv-db-index [x y]
  (loop [xs (seq x)
         ys (seq y)]
    (cond
      (nil? xs) (nil? ys)
      (= (first xs) (first ys)) (recur (next xs) (next ys))
      :else false)))

(defn- hash-datoms
  [datoms]
  (reduce #(+ %1 (hash %2)) 0 datoms))

(defn- equiv-db [db other]
  (and (or (instance? DB other) (instance? FilteredDB other))
       (= (-schema db) (-schema other))
       (equiv-db-index (-datoms db :eavt []) (-datoms other :eavt []))))

#?(:cljs
   (defn pr-db [db w opts]
     (-write w "#datahike/DB {")
     (-write w (str ":max-tx " (-max-tx db) " "))
     (-write w (str ":max-eid " (-max-eid db) " "))
     (-write w "}")))

#?(:clj
   (do
     (defn pr-db [db, ^java.io.Writer w]
       (.write w (str "#datahike/DB {"))
       (.write w (str ":max-tx " (-max-tx db) " "))
       (.write w (str ":max-eid " (-max-eid db)))
       (.write w "}"))

     (defn pr-hist-db [db ^java.io.Writer w flavor time-point?]
       (.write w (str "#datahike/" flavor " {"))
       (.write w ":origin ")
       (binding [*out* w]
         (pr (-origin db)))
       (when time-point?
         (.write w " :time-point ")
         (binding [*out* w]
           (pr (-time-point db))))
       (.write w "}"))

     (defmethod print-method DB [db w] (pr-db db w))
     (defmethod print-method FilteredDB [db w] (pr-db db w))
     (defmethod print-method HistoricalDB [db w] (pr-hist-db db w "HistoricalDB" false))
     (defmethod print-method AsOfDB [db w] (pr-hist-db db w "AsOfDB" true))
     (defmethod print-method SinceDB [db w] (pr-hist-db db w "SinceDB" true))

     (defmethod pp/simple-dispatch Datom [^Datom d]
       (pp/pprint-logical-block :prefix "#datahike/Datom [" :suffix "]"
                                (pp/write-out (.-e d))
                                (.write ^java.io.Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (.-a d))
                                (.write ^java.io.Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (.-v d))
                                (.write ^java.io.Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (datom-tx d))))

     (defn- pp-db [db ^java.io.Writer w]
       (pp/pprint-logical-block :prefix "#datahike/DB {" :suffix "}"
                                (pp/pprint-logical-block
                                 (pp/write-out :max-tx)
                                 (.write ^java.io.Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out (-max-tx db))
                                 (.write ^java.io.Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out :max-eid)
                                 (.write ^java.io.Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out (-max-eid db)))
                                (pp/pprint-newline :linear)))

     (defmethod pp/simple-dispatch DB [db] (pp-db db *out*))
     (defmethod pp/simple-dispatch FilteredDB [db] (pp-db db *out*))))

(defn db-from-reader [{:keys [schema datoms]}]
  (init-db (map (fn [[e a v tx]] (datom e a v tx)) datoms) schema))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn attr-info
  "Returns identifier name and reference value of an attributes. Both values are identical for non-reference databases."
  [db attr]
  (if (and (:attribute-refs? (-config db))
           (not (nil? attr)))
    (if (number? attr)
      {:ident (-ident-for db attr) :ref attr}
      {:ident attr :ref (-ref-for db attr)})
    {:ident attr :ref attr}))

(defn- resolve-datom [db e a v t default-e default-tx]
  (let [{a-ident :ident a-db :ref} (attr-info db a)]
    (when a-ident (validate-attr-ident a-ident (list 'resolve-datom 'db e a v t) db))
    (datom
     (or (entid-some db e) default-e)                       ;; e
     a-db                                                   ;; a
     (if (and (some? v) (ref? db a-ident))                  ;; v
       (entid-strict db v)
       v)
     (or (entid-some db t) default-tx))))                   ;; t

(defn components->pattern [db index [c0 c1 c2 c3] default-e default-tx]
  (case index
    :eavt (resolve-datom db c0 c1 c2 c3 default-e default-tx)
    :aevt (resolve-datom db c1 c0 c2 c3 default-e default-tx)
    :avet (resolve-datom db c2 c0 c1 c3 default-e default-tx)))

;; ----------------------------------------------------------------------------

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

(defn #?@(:clj [^Boolean is-attr?]
          :cljs [^boolean is-attr?]) [db attr property]
  (let [a-ident (if (and (:attribute-refs? (-config db))
                         (number? attr))
                  (-ident-for db attr)
                  attr)]
    (contains? (-attrs-by db property) a-ident)))

(defn #?@(:clj [^Boolean multival?]
          :cljs [^boolean multival?]) [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn #?@(:clj [^Boolean ref?]
          :cljs [^boolean ref?]) [db attr]
  (is-attr? db attr :db.type/ref))

(defn #?@(:clj [^Boolean component?]
          :cljs [^boolean component?]) [db attr]
  (is-attr? db attr :db/isComponent))

(defn #?@(:clj [^Boolean indexing?]
          :cljs [^boolean indexing?]) [db attr]
  (is-attr? db attr :db/index))

(defn #?@(:clj [^Boolean no-history?]
          :cljs [^boolean no-history?]) [db attr]
  (is-attr? db attr :db/noHistory))

(defn #?@(:clj  [^Boolean tuple-source?]
          :cljs [^boolean tuple-source?])
  "Returns true if 'attr' is an attribute basis of a tuple attribute.
   E.g. :a is an attribute part of the tuple attribute :a+b.
   (tuple-source? :a) returns true."
  [db attr]
  (is-attr? db attr :db/attrTuples))

(defn #?@(:clj  [^Boolean tuple?]
          :cljs [^boolean tuple?])
  "Returns true if 'attr' is a tuple attribute.
   I.e., if 'attr' value is of type ':db.type/tuple'"
  [db attr]
  (is-attr? db attr :db.type/tuple))

(defn #?@(:clj  [^Boolean homogeneous-tuple-attr?]
          :cljs [^boolean homogeneous-tuple-attr?])
  "Returns true if 'attr' is a homogeneous tuple attribute."
  [db attr]
  (and (tuple? db attr)
       (-> db -schema attr :db/tupleType)))

(defn #?@(:clj  [^Boolean heterogeneous-tuple-attr?]
          :cljs [^boolean heterogeneous-tuple-attr?])
  "Returns true if 'attr' is an heterogeneous tuple attribute."
  [db attr]
  (and (tuple? db attr)
       (-> db -schema attr :db/tupleTypes)))

(defn #?@(:clj  [^Boolean composite-tuple-attr?]
          :cljs [^boolean composite-tuple-attr?])
  "Returns true if 'attr' is a composite tuple attribute."
  [db attr]
  (and (tuple? db attr)
       (-> db -schema attr :db/tupleAttrs)))

(defn entid [db eid]
  {:pre [(db? db)]}
  (cond
    (and (number? eid) (pos? eid))
    eid

    (sequential? eid)
    (let [[attr value] eid]
      (cond
        (not= (count eid) 2)
        (raise "Lookup ref should contain 2 elements: " eid
               {:error :lookup-ref/syntax, :entity-id eid})
        (not (is-attr? db attr :db/unique))
        (raise "Lookup ref attribute should be marked as :db/unique: " eid
               {:error :lookup-ref/unique, :entity-id eid})
        (nil? value)
        nil
        :else
        (-> (-datoms db :avet eid) first :e)))

    #?@(:cljs [(array? eid) (recur db (array-seq eid))])

    (keyword? eid)
    (-> (-datoms db :avet [:db/ident eid]) first :e)

    :else
    (raise "Expected number or lookup ref for entity id, got " eid
           {:error :entity-id/syntax, :entity-id eid})))

(defn entid-strict [db eid]
  (or (entid db eid)
      (raise "Nothing found for entity id " eid
             {:error :entity-id/missing
              :entity-id eid})))

(defn entid-some [db eid]
  (when eid
    (entid-strict db eid)))

;;;;;;;;;; Transacting
(defn #?@(:clj [^Boolean reverse-ref?]
          :cljs [^boolean reverse-ref?]) [ident]
  (cond
    (keyword? ident)
    (= \_ (nth (name ident) 0))

    (string? ident)
    (boolean (re-matches #"(?:([^/]+)/)?_([^/]+)" ident))

    (number? ident)
    false

    :else
    (raise "Bad attribute type: " ident ", expected keyword or string"
           {:error :transact/syntax, :attribute ident})))

(defn reverse-ref [ident]
  (cond
    (keyword? ident)
    (if (reverse-ref? ident)
      (keyword (namespace ident) (subs (name ident) 1))
      (keyword (namespace ident) (str "_" (name ident))))

    (string? ident)
    (let [[_ ns name] (re-matches #"(?:([^/]+)/)?([^/]+)" ident)]
      (if (= \_ (nth name 0))
        (if ns (str ns "/" (subs name 1)) (subs name 1))
        (if ns (str ns "/_" name) (str "_" name))))

    :else
    (raise "Bad attribute type: " ident ", expected keyword or string"
           {:error :transact/syntax, :attribute ident})))

(defn validate-datom [db ^Datom datom]
  (when (and (datom-added datom)
             (is-attr? db (.-a datom) :db/unique))
    (when-let [found (not-empty (-datoms db :avet [(.-a datom) (.-v datom)]))]
      (raise "Cannot add " datom " because of unique constraint: " found
             {:error :transact/unique
              :attribute (.-a datom)
              :datom datom}))))

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn validate-attr-ident [a-ident at db]
  (when-not (or (keyword? a-ident) (string? a-ident))
    (raise "Bad entity attribute " a-ident " at " at ", expected keyword or string"
           {:error :transact/syntax, :attribute a-ident, :context at}))
  (when (and (= :write (:schema-flexibility (-config db)))
             (not (or (ds/meta-attr? a-ident) (ds/schema-attr? a-ident) (ds/entity-spec-attr? a-ident))))
    (if-let [db-idents (:db/ident (-rschema db))]
      (let [attr (if (reverse-ref? a-ident)
                   (reverse-ref a-ident)
                   a-ident)]
        (when-not (db-idents attr)
          (raise "Bad entity attribute " a-ident " at " at ", not defined in current schema"
                 {:error :transact/schema :attribute a-ident :context at})))
      (raise "No schema found in db."
             {:error :transact/schema :attribute a-ident :context at}))))

(defn- validate-attr [attr at db]
  (if (:attribute-refs? (-config db))
    (do (when-not (number? attr)
          (raise "Bad entity attribute " attr " at " at ", expected reference number"
                 {:error :transact/syntax, :attribute attr, :context at}))
        (if-let [a-ident (get-in db [:ref-ident-map attr])]
          (validate-attr-ident a-ident at db)
          (raise "Bad entity attribute " attr " at " at ", not defined in current schema"
                 {:error :transact/schema :attribute attr :context at})))
    (validate-attr-ident attr at db)))

(defn- validate-val [v [_ _ a _ _ :as at] {:keys [config schema ref-ident-map] :as db}]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at}))
  (let [{:keys [attribute-refs? schema-flexibility]} config
        a-ident (if (and attribute-refs? (number? a)) (-ident-for db a) a)
        v-ident (if (and attribute-refs?
                         (contains? (-system-entities db) a)
                         (not (nil? (ref-ident-map v))))
                  (ref-ident-map v)
                  v)]

    (when (= :write schema-flexibility)
      (let [schema-spec (if (or (ds/meta-attr? a-ident) (ds/schema-attr? a-ident))
                          ds/implicit-schema-spec
                          schema)]
        (when-not (ds/value-valid? a-ident v-ident schema)
          (raise "Bad entity value " v-ident " at " at ", value does not match schema definition. Must be conform to: "
                 (ds/describe-type (get-in schema-spec [a-ident :db/valueType]))
                 {:error :transact/schema :value v-ident :attribute a-ident :schema (get-in db [:schema a-ident])}))))))

(defn- current-tx [report]
  (inc (get-in report [:db-before :max-tx])))

(defn next-eid [db]
  (inc (:max-eid db)))

(defn- #?@(:clj [^Boolean tx-id?]
           :cljs [^boolean tx-id?])
  [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx")                                ;; for datahike.js interop
      (= e "datomic.tx")
      (= e "datahike.tx")))

(defn- #?@(:clj [^Boolean tempid?]
           :cljs [^boolean tempid?])
  [x]
  (or (and (number? x) (neg? x)) (string? x)))

(defn advance-max-eid [db eid]
  (cond-> db
    (and (> eid (:max-eid db))
         (< eid tx0))                                 ;; do not trigger advance if transaction id was referenced
    (assoc :max-eid eid)))

(defn advance-max-tid [db tid]
  (assoc db :max-tx tid))

(defn- allocate-eid
  ([report eid]
   (update-in report [:db-after] advance-max-eid eid))
  ([report e eid]
   (cond-> report
     (tx-id? e)
     (assoc-in [:tempids e] eid)
     (tempid? e)
     (assoc-in [:tempids e] eid)
     true
     (update-in [:db-after] advance-max-eid eid))))

(defn update-schema [db ^Datom datom]
  (let [schema (-schema db)
        attribute-refs? (:attribute-refs? (-config db))
        e (.-e datom)
        a (.-a datom)
        v (.-v datom)
        a-ident (if attribute-refs? (-ident-for db a) a)
        v-ident (if (and attribute-refs? (contains? (-system-entities db) v))
                  (-ident-for db v)
                  v)]
    (when (and attribute-refs? (contains? (-system-entities db) e))
      (raise "System schema entity cannot be changed"
             {:error :transact/schema :entity-id e}))
    (if (= a-ident :db/ident)
      (if (schema v-ident)
        (raise (str "Schema with attribute " v-ident " already exists")
               {:error :transact/schema :attribute v-ident})
        (-> (assoc-in db [:schema v-ident] (merge (or (schema e) {}) (hash-map a-ident v-ident)))
            (assoc-in [:schema e] v-ident)
            (assoc-in [:ident-ref-map v-ident] e)
            (assoc-in [:ref-ident-map e] v-ident)))
      (if-let [schema-entry (schema e)]
        (if (schema schema-entry)
          (update-in db [:schema schema-entry a-ident] (fn [old]
                                                         (if (ds/entity-spec-attr? a-ident)
                                                           (if old
                                                             (conj old v-ident)
                                                             [v-ident])
                                                           v-ident)))
          (assoc-in db [:schema e a-ident] v-ident))
        (assoc-in db [:schema e] (hash-map a-ident v-ident))))))

(defn update-rschema [db]
  (assoc db :rschema (rschema (:schema db))))

(defn remove-schema [db ^Datom datom]
  (let [schema (-schema db)
        attribute-refs? (:attribute-refs? (-config db))
        e (.-e datom)
        a (.-a datom)
        v (.-v datom)
        a-ident (if attribute-refs? (-ident-for db a) a)
        v-ident (if (and attribute-refs? (contains? (-system-entities db) v))
                  (-ident-for db v)
                  v)]
    (when (and attribute-refs? (contains? (-system-entities db) e))
      (raise "System schema entity cannot be changed"
             {:error :retract/schema :entity-id e}))
    (if (= a-ident :db/ident)
      (if-not (schema v-ident)
        (let [err-msg (str "Schema with attribute " v-ident " does not exist")
              err-map {:error :retract/schema :attribute v-ident}]
          (throw #?(:clj (ex-info err-msg err-map)
                    :cljs (error err-msg err-map))))
        (-> (assoc-in db [:schema e] (dissoc (schema v-ident) a-ident))
            (update-in [:schema] #(dissoc % v-ident))
            (update-in [:ident-ref-map] #(dissoc % v-ident))
            (update-in [:ref-ident-map] #(dissoc % e))))
      (if-let [schema-entry (schema e)]
        (if (schema schema-entry)
          (update-in db [:schema schema-entry] #(dissoc % a-ident))
          (update-in db [:schema e] #(dissoc % a-ident v-ident)))
        (let [err-msg (str "Schema with entity id " e " does not exist")
              err-map {:error :retract/schema :entity-id e :attribute a :value e}]
          (throw #?(:clj (ex-info err-msg err-map)
                    :cljs (error err-msg err-map))))))))

;; In context of `with-datom` we can use faster comparators which
;; do not check for nil (~10-15% performance gain in `transact`)

(defn- with-datom [db ^Datom datom]
  (validate-datom db datom)
  (let [{a-ident :ident} (attr-info db (.-a datom))
        indexing? (indexing? db a-ident)
        schema? (or (ds/schema-attr? a-ident) (ds/entity-spec-attr? a-ident))
        keep-history? (and (-keep-history? db) (not (no-history? db a-ident)))
        op-count (.op-count db)]
    (if (datom-added datom)
      (cond-> db
        true (update-in [:eavt] #(di/-insert % datom :eavt op-count))
        true (update-in [:aevt] #(di/-insert % datom :aevt op-count))
        indexing? (update-in [:avet] #(di/-insert % datom :avet op-count))
        true (advance-max-eid (.-e datom))
        true (update :hash + (hash datom))
        schema? (-> (update-schema datom)
                    update-rschema)
        true (update :op-count inc))

      (if-some [removing ^Datom (first (-search db [(.-e datom) (.-a datom) (.-v datom)]))]
        (cond-> db
          true (update-in [:eavt] #(di/-remove % removing :eavt op-count))
          true (update-in [:aevt] #(di/-remove % removing :aevt op-count))
          indexing? (update-in [:avet] #(di/-remove % removing :avet op-count))
          true (update :hash - (hash removing))
          schema? (-> (remove-schema datom) update-rschema)
          keep-history? (update-in [:temporal-eavt] #(di/-temporal-insert % removing :eavt op-count))
          keep-history? (update-in [:temporal-eavt] #(di/-temporal-insert % datom :eavt (inc op-count)))
          keep-history? (update-in [:temporal-aevt] #(di/-temporal-insert % removing :aevt op-count))
          keep-history? (update-in [:temporal-aevt] #(di/-temporal-insert % datom :aevt (inc op-count)))
          keep-history? (update :hash + (hash datom))
          (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-insert % removing :avet op-count))
          (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-insert % datom :avet (inc op-count)))
          true (update :op-count + (if (or keep-history? indexing?) 2 1)))
        db))))

(defn- with-temporal-datom [db ^Datom datom]
  (let [{a-ident :ident} (attr-info db (.-a datom))
        indexing? (indexing? db a-ident)
        schema? (ds/schema-attr? a-ident)
        current-datom ^Datom (first (-search db [(.-e datom) (.-a datom) (.-v datom)]))
        history-datom ^Datom (first (search-temporal-indices db [(.-e datom) (.-a datom) (.-v datom) (.-tx datom)]))
        current? (not (nil? current-datom))
        history? (not (nil? history-datom))
        op-count (.op-count db)]
    (cond-> db
      current? (update-in [:eavt] #(di/-remove % current-datom :eavt op-count))
      current? (update-in [:aevt] #(di/-remove % current-datom :aevt op-count))
      (and current? indexing?) (update-in [:avet] #(di/-remove % current-datom :avet op-count))
      current? (update :hash - (hash current-datom))
      (and current? schema?) (-> (remove-schema datom) update-rschema)
      history? (update-in [:temporal-eavt] #(di/-remove % history-datom :eavt op-count))
      history? (update-in [:temporal-aevt] #(di/-remove % history-datom :aevt op-count))
      (and history? indexing?) (update-in [:temporal-avet] #(di/-remove % history-datom :avet op-count))
      (or current? history?) (update :op-count inc))))

(defn- queue-tuple [queue tuple idx db e v]
  (let [tuple-value  (or (get queue tuple)
                         (:v (first (-datoms db :eavt [e tuple])))
                         (vec (repeat (-> db (-schema) (get tuple) :db/tupleAttrs count) nil)))
        tuple-value' (assoc tuple-value idx v)]
    (assoc queue tuple tuple-value')))

(defn- queue-tuples
  "Assuming the attribute we are concerned with is :a and its associated value is 'a',
   returns {:a+b+c [a nil nil], :a+d [a, nil]}"
  [queue tuples db e v]
  (reduce-kv
   (fn [queue tuple idx]
     (queue-tuple queue tuple idx db e v))
   queue
   tuples))
(defn validate-datom-upsert [db ^Datom datom]
  (when (is-attr? db (.-a datom) :db/unique)
    (when-let [old (first (-datoms db :avet [(.-a datom) (.-v datom)]))]
      (when-not (= (.-e datom) (.-e old))
        (raise "Cannot add " datom " because of unique constraint: " old
               {:error     :transact/unique
                :attribute (.-a datom)
                :datom     datom})))))

(defn- with-datom-upsert [db ^Datom datom]
  (validate-datom-upsert db datom)
  (let [indexing?     (indexing? db (.-a datom))
        {a-ident :ident} (attr-info db (.-a datom))
        schema?       (ds/schema-attr? a-ident)
        keep-history? (and (-keep-history? db) (not (no-history? db a-ident))
                           (not= :db/txInstant a-ident))
        op-count      (.op-count db)]
    (cond-> db
      ;; Optimistic removal of the schema entry (because we don't know whether it is already present or not)
      schema? (try
                (-> db (remove-schema datom) update-rschema)
                (catch clojure.lang.ExceptionInfo e
                  db))

      keep-history? (update-in [:temporal-eavt] #(di/-temporal-upsert % datom :eavt op-count))
      true          (update-in [:eavt] #(di/-upsert % datom :eavt op-count))

      keep-history? (update-in [:temporal-aevt] #(di/-temporal-upsert % datom :aevt op-count))
      true          (update-in [:aevt] #(di/-upsert % datom :aevt op-count))

      (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-upsert % datom :avet op-count))
      indexing?                     (update-in [:avet] #(di/-upsert % datom :avet op-count))

      true    (update :op-count inc)
      true    (advance-max-eid (.-e datom))
      true    (update :hash + (hash datom))
      schema? (-> (update-schema datom) update-rschema))))

(defn- transact-report
  ([report datom] (transact-report report datom false))
  ([report datom upsert?]
   (let [db      (:db-after report)
         a       (:a datom)
         update-fn (if upsert? with-datom-upsert with-datom)
         report' (-> report
                     (update-in [:db-after] update-fn datom)
                     (update-in [:tx-data] conj datom))]
     (if (tuple-source? db a)
       (let [e      (:e datom)
             v      (if (datom-added datom) (:v datom) nil)
             queue  (or (-> report' ::queued-tuples (get e)) {})
             tuples (get (-attrs-by db :db/attrTuples) a)
             queue' (queue-tuples queue tuples db e v)]
         (update report' ::queued-tuples assoc e queue'))
       report'))))

(defn- check-upsert-conflict [entity acc]
  (let [[e a v] acc
        _e (:db/id entity)]
    (if (or (nil? _e)
            (tempid? _e)
            (nil? acc)
            (== _e e))
      acc
      (raise "Conflicting upsert: " [a v] " resolves to " e
             ", but entity already has :db/id " _e
             {:error :transact/upsert
              :entity entity
              :assertion acc}))))

(defn- upsert-eid [db entity]
  (when-let [unique-idents (not-empty (-attrs-by db :db.unique/identity))]
    (->>
     (reduce-kv
      (fn [acc a-ident v]                                 ;; acc = [e a v]
        (if (contains? unique-idents a-ident)
          (let [a (if (:attribute-refs? (-config db)) (-ref-for db a-ident) a-ident)]
            (validate-val v [nil nil a v nil] db)
            (if-some [e (:e (first (-datoms db :avet [a v])))]
              (cond
                (nil? acc) [e a v]                    ;; first upsert
                (= (get acc 0) e) acc                 ;; second+ upsert, but does not conflict
                :else
                (let [[_e _a _v] acc]
                  (raise "Conflicting upserts: " [_a _v] " resolves to " _e
                         ", but " [a v] " resolves to " e
                         {:error :transact/upsert
                          :entity entity
                          :assertion [e a v]
                          :conflict [_e _a _v]})))
              acc))                                   ;; upsert attr, but resolves to nothing
          acc))                                       ;; non-upsert attr
      nil
      entity)
     (check-upsert-conflict entity)
     first)))                                         ;; getting eid from acc

;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a-ident vs]
  (cond
    ;; not a multival context
    (not (or (reverse-ref? a-ident)
             (multival? db a-ident)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (or (arrays/array? vs)
             (and (coll? vs) (not (map? vs)))))
    [vs]

    ;; probably lookup ref, but not an entity spec
    (and (= (count vs) 2)
         (keyword? (first vs))
         (is-attr? db (first vs) :db.unique/identity)
         (not (ds/entity-spec-attr? a-ident)))
    [vs]

    :else vs))

(defn- explode [db entity]
  (let [eid (:db/id entity)
        attribute-refs? (:attribute-refs? (-config db))
        _ (when (and attribute-refs? (contains? (-system-entities db) eid))
            (raise "Entity with ID " eid " is a system attribute " (-ident-for db eid) " and cannot be changed"
                   {:error :transact/syntax, :eid eid, :attribute (-ident-for db eid) :context entity}))
        ensure (:db/ensure entity)
        entities (for [[a-ident vs] entity
                       :when (not (or (= a-ident :db/id) (= a-ident :db/ensure)))
                       :let [_ (validate-attr-ident a-ident {:db/id eid, a-ident vs} db)
                             reverse? (reverse-ref? a-ident)
                             straight-a-ident (if reverse? (reverse-ref a-ident) a-ident)
                             straight-a (if attribute-refs?
                                          (-ref-for db straight-a-ident) ;; translation to datom format
                                          straight-a-ident)
                             _ (when (and reverse? (not (ref? db straight-a-ident)))
                                 (raise "Bad attribute " a-ident ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                                        {:error :transact/syntax, :attribute a-ident, :context {:db/id eid, a-ident vs}}))]
                       v (maybe-wrap-multival db a-ident vs)]
                   (if (and (ref? db straight-a-ident) (map? v)) ;; another entity specified as nested map
                     (assoc v (reverse-ref a-ident) eid)
                     (if reverse?
                       [:db/add v straight-a eid]
                       [:db/add eid straight-a (if (and attribute-refs? (ds/is-system-keyword? v)) ;; translation of system enums
                                                 (-ref-for db v)
                                                 v)])))]
    (if ensure
      (let [{:keys [:db.entity/attrs :db.entity/preds]} (-> db :schema ensure)]
        (if (empty? attrs)
          (if (empty? preds)
            entities
            (concat entities [[:db.ensure/preds eid ensure preds]]))
          (if (empty? preds)
            (concat entities [[:db.ensure/attrs eid ensure attrs]])
            (concat entities [[:db.ensure/attrs eid ensure attrs]
                              [:db.ensure/preds eid ensure preds]]))))
      entities)))

(defn- transact-add [{:keys [db-after] :as report} [_ e a v tx :as ent]]
  (validate-attr a ent db-after)
  (validate-val v ent db-after)
  (let [attribute-refs? (:attribute-refs? (-config db-after))
        tx (or tx (current-tx report))
        db db-after
        e (entid-strict db e)
        a-ident (if attribute-refs? (-ident-for db a) a)
        v (if (ref? db a-ident) (entid-strict db v) v)
        new-datom (datom e a v tx)
        upsert? (not (multival? db a))]
    (transact-report report new-datom upsert?)))

(defn- transact-retract-datom [report ^Datom d]
  (transact-report report (datom (.-e d) (.-a d) (.-v d) (current-tx report) false)))

(defn- transact-purge-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (update-in report [:db-after] with-temporal-datom d)))

#_(defn- transact-purge-datom [report ^Datom d]
    (update-in report [:db-after] with-temporal-datom
             ;d
               (datom (.-e d) (.-a d) (.-v d) (datom-tx d) false)))

(defn- retract-components [db datoms]
  (into #{} (comp
             (filter (fn [^Datom d] (component? db (.-a d))))
             (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

(defn- purge-components [db datoms]
  (let [xf (comp
            (filter (fn [^Datom d] (component? db (.-a d))))
            (map (fn [^Datom d] [:db.purge/entity (.-v d)])))]
    (into #{} xf datoms)))

#?(:clj
   (defmacro cond+ [& clauses]
     (when-some [[test expr & rest] clauses]
       (case test
         :let `(let ~expr (cond+ ~@rest))
         `(if ~test ~expr (cond+ ~@rest))))))

#?(:clj
   (defmacro some-of
     ([] nil)
     ([x] x)
     ([x & more]
      `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#)))))

(declare transact-tx-data)

(defn- retry-with-tempid [initial-report report es tempid upserted-eid]
  (if (contains? (:tempids initial-report) tempid)
    (raise "Conflicting upsert: " tempid " resolves"
           " both to " upserted-eid " and " (get-in initial-report [:tempids tempid])
           {:error :transact/upsert})
    ;; try to re-run from the beginning
    ;; but remembering that `tempid` will resolve to `upserted-eid`
    (let [tempids' (-> (:tempids report)
                       (assoc tempid upserted-eid))
          report' (assoc initial-report :tempids tempids')]
      (transact-tx-data report' es))))

(defn assert-preds [db [_ e _ preds]]
  (reduce
   (fn [coll pred]
     (if ((resolve pred) db e)
       coll
       (conj coll pred)))
   #{} preds))

(def builtin-fn?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity
    :db/purge
    :db.ensure/attrs
    :db.ensure/preds
    :db.purge/entity
    :db.purge/attribute
    :db.history.purge/before})

(defn flush-tuples
  "Generates all the add or retract operations needed for updating the states of composite tuples.
  E.g., if '::queued-tuples' contains {100 {:a+b+c [123 nil nil]}}, this function creates this vector [:db/add 100 :a+b+c [123 nil nil]]"
  [report]
  (let [db (:db-after report)]
    (reduce-kv
     (fn [entities eid tuples+values]
       (reduce-kv
        (fn [entities tuple value]
          (let [value   (if (every? nil? value) nil value)
                current (:v (first (-datoms db :eavt [eid tuple])))]
            (cond
              (= value current) entities
                ;; adds ::internal to meta-data to mean that these datoms were generated internally.
              (nil? value)      (conj entities ^::internal [:db/retract eid tuple current])
              :else             (conj entities ^::internal [:db/add eid tuple value]))))
        entities
        tuples+values))
     []
     (::queued-tuples report))))

(defn transact-tx-data [initial-report initial-es]
  (when-not (or (nil? initial-es)
                (sequential? initial-es))
    (raise "Bad transaction data " initial-es ", expected sequential collection"
           {:error :transact/syntax, :tx-data initial-es}))
  (let [db-before (:db-before initial-report)
        {:keys [attribute-refs? schema-flexibility]} (-config db-before)
        tx-instant (if attribute-refs? (-ref-for db-before :db/txInstant) :db/txInstant)
        has-tuples? (seq (-attrs-by (:db-after initial-report) :db.type/tuple))
        initial-es' (if has-tuples?
                      (interleave initial-es (repeat ::flush-tuples))
                      initial-es)]
    (loop [report (update initial-report :db-after transient)
           es (if (-keep-history? (:db-before initial-report))
                (concat [[:db/add (current-tx report) tx-instant (get-time) (current-tx report)]]
                        initial-es')
                initial-es')]
      (let [[entity & entities] es
            {:keys [tempids db-after]} report
            db db-after]
        (cond
          (empty? es)
          (-> report
              (assoc-in [:tempids :db/current-tx] (current-tx report))
              (update-in [:db-after :max-tx] inc)
              (update :db-after persistent!))

          (nil? entity)
          (recur report entities)

          (= ::flush-tuples entity)
          (if (contains? report ::queued-tuples)
            (recur
             (dissoc report ::queued-tuples)
             (concat (flush-tuples report) entities))
            (recur report entities))

          (map? entity)
          (let [old-eid (:db/id entity)]
            (cond+
              ;; :db/current-tx / "datomic.tx" => tx
             (tx-id? old-eid)
             (let [id (current-tx report)]
               (recur (allocate-eid report old-eid id)
                      (cons (assoc entity :db/id id) entities)))

             ;; lookup-ref => resolved | error
             (sequential? old-eid)
             (let [id (entid-strict db old-eid)]
               (recur report
                      (cons (assoc entity :db/id id) entities)))

             ;; upserted => explode | error
             :let [upserted-eid (upsert-eid db entity)]

             (some? upserted-eid)
             (if (and (tempid? old-eid)
                      (contains? tempids old-eid)
                      (not= upserted-eid (get tempids old-eid)))
               (retry-with-tempid initial-report report initial-es old-eid upserted-eid)
               (do
                 ;; schema update tx
                 (when (ds/schema-entity? entity)
                   (when (and (contains? entity :db/ident)
                              (ds/is-system-keyword? (:db/ident entity)))
                     (raise "Using namespace 'db' for attribute identifiers is not allowed"
                            {:error :transact/schema :entity entity}))
                   (if-let [attr-name (get-in db [:schema upserted-eid])]
                     (when-let [invalid-updates (ds/find-invalid-schema-updates entity (get-in db [:schema attr-name]))]
                       (when-not (empty? invalid-updates)
                         (raise "Update not supported for these schema attributes"
                                {:error :transact/schema :entity entity :invalid-updates invalid-updates})))
                     (when (= :write schema-flexibility)
                       (when (or (:db/cardinality entity) (:db/valueType entity))
                         (when-not (ds/schema? entity)
                           (raise "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
                                  {:error :transact/schema :entity entity}))))))
                 (recur (allocate-eid report old-eid upserted-eid)
                        (concat (explode db (assoc entity :db/id upserted-eid)) entities))))

             ;; resolved | allocated-tempid | tempid | nil => explode
             (or (number? old-eid)
                 (nil? old-eid)
                 (string? old-eid))
             (let [new-eid (cond
                             (nil? old-eid) (next-eid db)
                             (tempid? old-eid) (or (get tempids old-eid)
                                                   (next-eid db))
                             :else old-eid)
                   new-entity (assoc entity :db/id new-eid)]
               (when (ds/schema-entity? entity)
                 (when (and (contains? entity :db/ident)
                            (ds/is-system-keyword? (:db/ident entity)))
                   (raise "Using namespace 'db' for attribute identifiers is not allowed"
                          {:error :transact/schema :entity entity}))
                 (if-let [attr-name (get-in db [:schema new-eid])]
                   (when-let [invalid-updates (ds/find-invalid-schema-updates entity (get-in db [:schema attr-name]))]
                     (when-not (empty? invalid-updates)
                       (raise "Update not supported for these schema attributes"
                              {:error :transact/schema :entity entity :invalid-updates invalid-updates})))
                   (when (= :write schema-flexibility)
                     (when (or (:db/cardinality entity) (:db/valueType entity))
                       (when-not (ds/schema? entity)
                         (raise "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
                                {:error :transact/schema :entity entity}))))))
               (recur (allocate-eid report old-eid new-eid)
                      (concat (explode db new-entity) entities)))

             ;; trash => error
             :else
             (raise "Expected number, string or lookup ref for :db/id, got " old-eid
                    {:error :entity-id/syntax, :entity entity})))

          (sequential? entity)
          (let [[op e a v] entity]
            (cond
              (= op :db.fn/call)
              (let [[_ f & args] entity]
                (recur report (concat (apply f db args) entities)))

              (and (keyword? op)
                   (not (builtin-fn? op)))
              (if-some [ident (entid db op)]
                (let [fun (-> (-search db [ident :db/fn]) first :v)
                      args (next entity)]
                  (if (fn? fun)
                    (recur report (concat (apply fun db args) entities))
                    (raise "Entity " op " expected to have :db/fn attribute with fn? value"
                           {:error :transact/syntax, :operation :db.fn/call, :tx-data entity})))
                (raise "Can’t find entity for transaction fn " op
                       {:error :transact/syntax, :operation :db.fn/call, :tx-data entity}))

              (and (tempid? e) (not= op :db/add))
              (raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
                     {:error :transact/syntax, :op entity})

              (or (= op :db.fn/cas)
                  (= op :db/cas))
              (let [[_ e a ov nv] entity
                    e (entid-strict db e)
                    _ (validate-attr a entity db)
                    nv (if (ref? db a) (entid-strict db nv) nv)
                    datoms (-search db [e a])]
                (if (nil? ov)
                  (if (empty? datoms)
                    (recur (transact-add report [:db/add e a nv]) entities)
                    (raise ":db.fn/cas failed on datom [" e " " a " " (if (multival? db a) (map :v datoms) (:v (first datoms))) "], expected nil"
                           {:error :transact/cas, :old (if (multival? db a) datoms (first datoms)), :expected ov, :new nv}))
                  (let [ov (if (ref? db a) (entid-strict db ov) ov)
                        _ (validate-val nv entity db)]
                    (if (multival? db a)
                      (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
                        (recur (transact-add report [:db/add e a nv]) entities)
                        (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                               {:error :transact/cas, :old datoms, :expected ov, :new nv}))
                      (let [v (:v (first datoms))]
                        (if (= v ov)
                          (recur (transact-add report [:db/add e a nv]) entities)
                          (raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                                 {:error :transact/cas, :old (first datoms), :expected ov, :new nv})))))))

              (tx-id? e)
              (recur (allocate-eid report e (current-tx report)) (cons [op (current-tx report) a v] entities))

              (and (ref? db a) (tx-id? v))
              (recur (allocate-eid report v (current-tx report)) (cons [op e a (current-tx report)] entities))

              (tempid? e)
              (let [upserted-eid (when (is-attr? db a :db.unique/identity)
                                   (:e (first (-datoms db :avet [a v]))))
                    allocated-eid (get tempids e)]
                (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                  (retry-with-tempid initial-report report initial-es e upserted-eid)
                  (let [eid (or upserted-eid allocated-eid (next-eid db))]
                    (recur (allocate-eid report e eid) (cons [op eid a v] entities)))))

              (and (ref? db a) (tempid? v))
              (if-let [vid (get tempids v)]
                (recur report (cons [op e a vid] entities))
                (recur (allocate-eid report v (next-eid db)) es))

              (and (homogeneous-tuple-attr? db a)
                   (> (count v) 8))
              (raise "Cannot store more than 8 values for homogeneous tuple: " entity
                     {:error :transact/syntax, :tx-data entity})

              (and (homogeneous-tuple-attr? db a)
                   (not (apply = (map type v))))
              (raise "Cannot store homogeneous tuple with values of different type: " entity
                     {:error :transact/syntax, :tx-data entity})

              (and (homogeneous-tuple-attr? db a)
                   (not (s/valid? (-> db -schema a :db/tupleType) (first v))))
              (raise "Cannot store homogeneous tuple. Values are of wrong type: " entity
                     {:error :transact/syntax, :tx-data entity})

              (and (heterogeneous-tuple-attr? db a)
                   (not (= (count v) (count (-> db -schema a :db/tupleTypes)))))
              (raise (str "Cannot store heterogeneous tuple: expecting " (count (-> db -schema a :db/tupleTypes)) " values, got " (count v))
                     {:error :transact/syntax, :tx-data entity})

              (and (heterogeneous-tuple-attr? db a)
                   (not (apply = (map s/valid? (-> db -schema a :db/tupleTypes) v))))
              (raise (str "Cannot store heterogeneous tuple: there is a mismatch between values " v " and their types " (-> db -schema a :db/tupleTypes))
                     {:error :transact/syntax, :tx-data entity})

              (and (not (::internal (meta entity)))
                   (composite-tuple-attr? db a))
              (raise "Can’t modify tuple attrs directly: " entity
                     {:error :transact/syntax, :tx-data entity})

              (= op :db/add)
              (recur (transact-add report entity) entities)

              (= op :db/retract)
              (if-some [e (entid db e)]
                (let [_ (validate-attr a entity db)
                      pattern (if (nil? v)
                                [e a]
                                (let [v (if (ref? db a) (entid-strict db v) v)]
                                  (validate-val v entity db)
                                  [e a v]))]
                  (recur (reduce transact-retract-datom report (-search db pattern)) entities))
                (recur report entities))

              (= op :db.fn/retractAttribute)
              (if-let [e (entid db e)]
                (let [_ (validate-attr a entity db)
                      datoms (vec (-search db [e a]))]
                  (recur (reduce transact-retract-datom report datoms)
                         (concat (retract-components db datoms) entities)))
                (recur report entities))

              (or (= op :db.fn/retractEntity)
                  (= op :db/retractEntity))
              (if-let [e (entid db e)]
                (let [e-datoms (vec (-search db [e]))
                      v-datoms (->> (map (partial -ident-for db)
                                         (-attrs-by db :db.type/ref))
                                    (mapcat (fn [a] (-search db [nil a e])))
                                    vec)
                      retracted-comps (retract-components db e-datoms)]
                  (recur (reduce transact-retract-datom report (concat e-datoms v-datoms))
                         (concat retracted-comps entities)))
                (recur report entities))

              (= op :db/purge)
              (if (-keep-history? db)
                (let [history (HistoricalDB. db)]
                  (if-some [e (entid history e)]
                    (let [v (if (ref? history a) (entid-strict history v) v)
                          old-datoms (-search history [e a v])]
                      (recur (reduce transact-purge-datom report old-datoms) entities))
                    (raise "Can't find entity with ID " e " to be purged" {:error :transact/purge, :operation op, :tx-data entity})))
                (raise "Purge is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity}))

              (= op :db.purge/attribute)
              (if (-keep-history? db)
                (let [history (HistoricalDB. db)]
                  (if-let [e (entid history e)]
                    (let [datoms (vec (-search history [e a]))]
                      (recur (reduce transact-purge-datom report datoms)
                             (concat (purge-components history datoms) entities)))
                    (raise "Can't find entity with ID " e " to be purged" {:error :transact/purge, :operation op, :tx-data entity})))
                (raise "Purge attribute is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity}))

              (= op :db.purge/entity)
              (if (-keep-history? db)
                (let [history (HistoricalDB. db)]
                  (if-let [e (entid history e)]
                    (let [e-datoms (vec (-search history [e]))
                          v-datoms (vec (mapcat (fn [a] (-search history [nil a e]))
                                                (-attrs-by history :db.type/ref)))
                          retracted-comps (purge-components history e-datoms)]

                      (recur (reduce transact-purge-datom report (concat e-datoms v-datoms))
                             (concat retracted-comps entities)))
                    (raise "Can't find entity with ID " e " to be purged" {:error :transact/purge, :operation op, :tx-data entity})))
                (raise "Purge entity is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity}))

              (= op :db.history.purge/before)
              (if (-keep-history? db)
                (let [history (HistoricalDB. db)
                      into-sorted-set #(apply sorted-set-by dd/cmp-datoms-eavt-quick %)
                      e-datoms (-> (clojure.set/difference
                                    (into-sorted-set (search-temporal-indices db nil))
                                    (into-sorted-set (search-current-indices db nil)))
                                   (filter-before e db)
                                   vec)
                      retracted-comps (purge-components history e-datoms)]
                  (recur (reduce transact-purge-datom report e-datoms)
                         (concat retracted-comps entities)))
                (raise "Purge entity is only available in temporal databases." {:error :transact/purge :operation op :tx-data entity}))

              ;; assert required attributes
              (= op :db.ensure/attrs)
              (let [{:keys [tx-data]} report
                    asserting-datoms (filter (fn [^Datom d] (= e (.-e d))) tx-data)
                    asserting-attributes (map (fn [^Datom d] (.-a d)) asserting-datoms)
                    diff (clojure.set/difference (set v) (set asserting-attributes))]
                (if (empty? diff)
                  (recur report entities)
                  (raise "Entity " e " missing attributes " diff " of spec " a
                         {:error :transact/ensure
                          :operation op
                          :tx-data entity
                          :asserting-datoms asserting-datoms})))

              ;; assert entity predicates
              (= op :db.ensure/preds)
              (let [{:keys [db-after]} report
                    preds (assert-preds db-after entity)]
                (if-not (empty? preds)
                  (raise "Entity " e " failed predicates " preds " of spec " a
                         {:error :transact/ensure
                          :operation op
                          :tx-data entity})
                  (recur report entities)))

              :else
              (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)" {:error :transact/syntax, :operation op, :tx-data entity})))

          (datom? entity)
          (let [[e a v tx added] entity]
            (if added
              (recur (transact-add report [:db/add e a v tx]) entities)
              (recur report (cons [:db/retract e a v] entities))))

          :else
          (raise "Bad entity type at " entity ", expected map or vector"
                 {:error :transact/syntax, :tx-data entity}))))))

(defn transact-entities-directly [initial-report initial-es]
  (loop [report (update initial-report :db-after persistent!)
         es initial-es
         migration-state (or (get-in initial-report [:db-before :migration]) {})]
    (let [[entity & entities] es
          {:keys [config] :as db} (:db-after report)
          [e a v t op] entity
          a-ident (if (and (number? a) (:attribute-refs? config))
                    (-ident-for db a)
                    a)
          a (if (:attribute-refs? config)
              (-ref-for db a-ident)
              (if (number? a)
                (raise "Configuration mismatch: import data with attribute references can not be imported into a database with no attribute references." {:error :import/mismatch :data entity})
                a-ident))
          max-eid (next-eid db)
          max-tid (inc (get-in report [:db-after :max-tx]))]
      (cond
        (empty? es)
        (-> report
            (update-in [:db-after :max-tx] inc)
            (update-in [:db-after :migration] #(if %
                                                 (merge % migration-state)
                                                 migration-state))
            (update :db-after persistent!))

        (= :db.install/attribute a-ident)
        (recur report entities migration-state)

        ;; meta entity
        (ds/meta-attr? a-ident)
        (let [new-t (get-in migration-state [:tids t] max-tid)
              new-datom (dd/datom new-t a v new-t op)
              new-e (.-e new-datom)
              upsert? (not (multival? db a-ident))]
          (recur (-> (transact-report report new-datom upsert?)
                     (assoc-in [:db-after :max-tx] max-tid))
                 entities
                 (-> migration-state
                     (assoc-in [:tids e] new-e)
                     (assoc-in [:eids e] new-e))))

        ;; tx not added yet
        (nil? (get-in migration-state [:tids t]))
        (recur (update-in report [:db-after :max-tx] inc) es (assoc-in migration-state [:tids t] max-tid))

        ;; ref not added yet
        (and (ref? db a) (nil? (get-in migration-state [:eids v])))
        (recur (allocate-eid report max-eid) es (assoc-in migration-state [:eids v] max-eid))

        :else
        (let [new-datom ^Datom (dd/datom
                                (or (get-in migration-state [:eids e]) max-eid)
                                a
                                (if (ref? db a)
                                  (get-in migration-state [:eids v])
                                  v)
                                (get-in migration-state [:tids t])
                                op)
              upsert? (and (not (multival? db a-ident))
                           op)]
          (recur (transact-report report new-datom upsert?) entities (assoc-in migration-state [:eids e] (.-e new-datom))))))))
