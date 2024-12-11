(ns ^:no-doc datahike.db
  (:require
   [clojure.data :as data]
   [clojure.walk :refer [postwalk]]
   #?(:clj [clojure.pprint :as pp])
   [datahike.config :as dc]
   [datahike.constants :as c :refer [ue0 e0 tx0 utx0 emax txmax system-schema]]
   [datahike.datom :as dd :refer [datom datom-tx datom-added]]
   [datahike.db.interface :as dbi]
   [datahike.db.search :as dbs]
   [datahike.db.utils :as dbu]
   [datahike.index :as di]
   [datahike.schema :as ds]
   [datahike.store :as store]
   [datahike.tools :as tools :refer [raise group-by-step]]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [medley.core :as m]
   [taoensso.timbre :refer [warn]])
  #?(:cljs (:require-macros [datahike.db :refer [defrecord-updatable]]
                            [datahike.datom :refer [combine-cmp datom]]
                            [datahike.tools :refer [raise]]))
  (:refer-clojure :exclude [seqable?])
  #?(:clj (:import [clojure.lang AMapEntry ITransientCollection IEditableCollection IPersistentCollection Seqable
                    IHashEq Associative IKeywordLookup ILookup]
                   [datahike.datom Datom]
                   [java.io Writer]
                   [java.util Date])))

(declare equiv-db empty-db)
#?(:cljs (declare pr-db))

;; ----------------------------------------------------------------------------
;; macros and funcs to support writing defrecords and updating
;; (replacing) builtins, i.e., Object/hashCode, IHashEq hasheq, etc.
;; code taken from prismatic:
;;  https://github.com/Prismatic/schema/commit/e31c419c56555c83ef9ee834801e13ef3c112597
;;

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
                   (instance? Seqable x)
                   (nil? x)
                   (instance? Iterable x)
                   (arrays/array? x)
                   (instance? java.util.Map x)))))

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
       (postwalk
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
     `(defrecord ~name ~fields ~@impls)))

#?(:clj
   (defmacro defrecord-updatable [name fields & impls]
     `(if-cljs
       ~(apply make-record-updatable-cljs name fields impls)
       ~(apply make-record-updatable-clj name fields impls))))

;; TxReport

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

;; DB

(defn- date? [d]
  #?(:cljs (instance? js/Date d)
     :clj (instance? Date d)))

(defn- date<= [^Date a ^Date b]
  (not (.before b a)))

(defn- as-of-pred
  "Create an as-of predicate, *including* the time-point"
  [time-point]
  (if (date? time-point)
    (fn [^Datom d] (date<= (.-v d) time-point))
    (fn [^Datom d] (<= (dd/datom-tx d) time-point))))

(defn- since-pred
  "Create a since predicate, *excluding* the time-point. The opposite of `as-of-pred`."
  [time-point]
  (complement (as-of-pred time-point)))

(defn assemble-datoms-xform [db]
  (mapcat
   (fn [[[_ a] datoms]]
     (if (dbu/multival? db a)
       (->> datoms
            (sort-by datom-tx)
            (reduce (fn [current-datoms ^Datom datom]
                      (if (datom-added datom)
                        (assoc current-datoms (.-v datom) datom)
                        (dissoc current-datoms (.-v datom))))
                    {})
            vals)
       (let [last-ea-tx (apply max (map datom-tx datoms))
             current-ea-datom (first (filter #(and (datom-added %) (= last-ea-tx (datom-tx %)))
                                             datoms))]
         (if current-ea-datom
           [current-ea-datom]
           []))))))

(defn temporal-datom-filter [datoms pred db]
  (let [filtered-tx-ids (dbu/filter-txInstant datoms pred db)]
    (filter (fn [^Datom d]
              (contains? filtered-tx-ids
                         (datom-tx d))))))

(defn- post-process-datoms [datoms db context]
  (let [xform (dbi/context-xform context)
        time-pred (dbi/context-time-pred context)]
    (cond

      time-pred
      (let [time-xform (temporal-datom-filter datoms time-pred db)]
        (if (dbi/context-historical? context)
          (into [] (dbi/nil-comp time-xform xform) datoms)
          (->> datoms
               (transduce time-xform
                          (group-by-step (fn [^Datom datom]
                                           [(.-e datom) (.-a datom)])))
               (into [] (dbi/nil-comp (assemble-datoms-xform db) xform)))))

      xform (into [] xform datoms)

      :else datoms)))

(defn db-transient [db]
  (-> db
      (update :eavt di/-transient)
      (update :aevt di/-transient)
      (update :avet di/-transient)))

(defn db-persistent! [db]
  (-> db
      (update :eavt di/-persistent!)
      (update :aevt di/-persistent!)
      (update :avet di/-persistent!)))

(defn contextual-search-fn [context]
  (case (dbi/context-temporal? context)
    true dbs/temporal-search
    false dbs/search-current-indices))

(defn contextual-search [db pattern context]
  (-> ((contextual-search-fn context) db pattern)
      (post-process-datoms db context)))

(defn contextual-batch-search [db pattern-mask batch-fn context]
  (-> ((contextual-search-fn context) db pattern-mask batch-fn)
      (post-process-datoms db context)))

(defn contextual-datoms [db index-type cs context]
  (-> (case (dbi/context-temporal? context)
        true (dbu/temporal-datoms db index-type cs)
        false (di/-slice
               (get db index-type)
               (dbu/components->pattern db index-type cs e0 tx0)
               (dbu/components->pattern db index-type cs emax txmax)
               index-type))
      (post-process-datoms db context)))

(defn contextual-seek-datoms [db index-type cs context]
  (-> (case (dbi/context-temporal? context)
        true (dbs/temporal-seek-datoms db index-type cs)
        false (di/-slice (get db index-type)
                         (dbu/components->pattern db index-type cs e0 tx0)
                         (datom emax nil nil txmax)
                         index-type))
      (post-process-datoms db context)))

(defn contextual-rseek-datoms [db index-type cs context]
  (-> (case (dbi/context-temporal? context)
        true (dbs/temporal-rseek-datoms db index-type cs)
        false (-> (di/-slice (get db index-type)
                             (dbu/components->pattern db index-type cs e0 tx0)
                             (datom emax nil nil txmax)
                             index-type)
                  vec
                  rseq))
      (post-process-datoms db context)))

(defn contextual-index-range [db avet attr start end context]
  (let [temporal? (dbi/context-temporal? context)
        current-db (dbi/context-current-db context)]
    (assert (or (not temporal?) current-db))
    (-> (case temporal?
          true (dbs/temporal-index-range db current-db attr start end)
          false (do (when-not (dbu/indexing? db attr)
                      (raise "Attribute"
                             attr "should be marked as :db/index true" {}))

                    (dbu/validate-attr
                     attr (list '-index-range 'db attr start end) db)
                    (di/-slice
                     avet
                     (dbu/resolve-datom db nil attr start nil e0 tx0)
                     (dbu/resolve-datom db nil attr end nil emax txmax)
                     :avet)))
        (post-process-datoms db context))))

(defn deeper-index-range [origin-db db attr start end context]
  (dbi/-index-range origin-db
                    attr
                    start
                    end
                    (dbi/context-set-current-db-if-not-set context db)))

(defrecord-updatable DB [schema eavt aevt avet temporal-eavt temporal-aevt temporal-avet max-eid max-tx op-count rschema hash config system-entities ident-ref-map ref-ident-map meta]
  #?@(:cljs
      [IHash (-hash [db] hash)
       IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (di/-seq eavt))
       IReversible (-rseq [db] (-rseq eavt))
       ICounted (-count [db] (count eavt))
       IEmptyableCollection (-empty [db] (empty-db (ds/get-user-schema db)))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))
       IEditableCollection (-as-transient [db] (db-transient db))
       ITransientCollection (-conj! [db key] (throw (ex-info "datahike.DB/conj! is not supported" {})))
       (-persistent! [db] (db-persistent! db))]

      :clj
      [Object (hashCode [db] hash)
       clojure.lang.IHashEq (hasheq [db] hash)
       Seqable (seq [db] (di/-seq eavt))
       IPersistentCollection
       (count [db] (di/-count eavt))
       (equiv [db other] (equiv-db db other))
       (empty [db] (empty-db (ds/get-user-schema db)))
       IEditableCollection
       (asTransient [db] (db-transient db))
       ITransientCollection
       (conj [db key] (throw (ex-info "datahike.DB/conj! is not supported" {})))
       (persistent [db] (db-persistent! db))])

  dbi/IDB
  (-schema [db] schema)
  (-rschema [db] rschema)
  (-system-entities [db] system-entities)
  (-attrs-by [db property] (rschema property))
  (-temporal-index? [db] (dbi/-keep-history? db))
  (-keep-history? [db] (:keep-history? config))
  (-max-tx [db] max-tx)
  (-max-eid [db] max-eid)
  (-config [db] config)
  (-ref-for [db a-ident]
            (if (:attribute-refs? config)
              (let [ref (get ident-ref-map a-ident)]
                (when (nil? ref)
                  (warn (str "Attribute " a-ident " has not been found in database")))
                ref)
              a-ident))
  (-ident-for [db a-ref]
              (if (:attribute-refs? config)
                (let [a-ident (get ref-ident-map a-ref)]
                  (when (nil? a-ident)
                    (warn (str "Attribute with reference number " a-ref " has not been found in database")))
                  a-ident)
                a-ref))

  dbi/ISearch
  (-search-context [db] dbi/base-context)
  (-search [db pattern context]
           (contextual-search db pattern context))
  (-batch-search [db pattern-mask batch-fn context]
                 (contextual-batch-search db pattern-mask batch-fn context))

  dbi/IIndexAccess
  (-datoms [db index-type cs context]
           (contextual-datoms db index-type cs context))

  (-seek-datoms [db index-type cs context]
                (contextual-seek-datoms db index-type cs context))

  (-rseek-datoms [db index-type cs context]
                 (contextual-rseek-datoms db index-type cs context))

  (-index-range [db attr start end context]
                (contextual-index-range db avet attr start end context))

  data/EqualityPartition
  (equality-partition [x] :datahike/db)

  data/Diff
  (diff-similar [a b]
                (let [datoms-a (di/-slice (:eavt a) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt)
                      datoms-b (di/-slice (:eavt b) (datom e0 nil nil tx0) (datom emax nil nil txmax) :eavt)]
                  (dd/diff-sorted datoms-a datoms-b dd/cmp-datoms-eavt-quick))))

;; FilteredDB

(defrecord-updatable FilteredDB [unfiltered-db pred]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (dbi/datoms db :eavt []))
       ICounted (-count [db] (count (dbi/datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on FilteredDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on FilteredDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on FilteredDB"))))

       IAssociative
       (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on FilteredDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on FilteredDB")))]

      :clj
      [IPersistentCollection
       (count [db] (count (dbi/datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on FilteredDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on FilteredDB")))

       Seqable (seq [db] (dbi/datoms db :eavt []))

       clojure.lang.ILookup (valAt [db k] (throw (UnsupportedOperationException. "valAt/2 is not supported on FilteredDB")))
       (valAt [db k nf] (throw (UnsupportedOperationException. "valAt/3 is not supported on FilteredDB")))
       clojure.lang.IKeywordLookup (getLookupThunk [db k]
                                                   (throw (UnsupportedOperationException. "getLookupThunk is not supported on FilteredDB")))

       Associative
       (containsKey [e k] (throw (UnsupportedOperationException. "containsKey is not supported on FilteredDB")))
       (entryAt [db k] (throw (UnsupportedOperationException. "entryAt is not supported on FilteredDB")))
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on FilteredDB")))])

  dbi/IDB
  (-schema [db] (dbi/-schema unfiltered-db))
  (-rschema [db] (dbi/-rschema unfiltered-db))
  (-system-entities [db] (dbi/-system-entities unfiltered-db))
  (-attrs-by [db property] (dbi/-attrs-by unfiltered-db property))
  (-temporal-index? [db] (dbi/-keep-history? db))
  (-keep-history? [db] (dbi/-keep-history? unfiltered-db))
  (-max-tx [db] (dbi/-max-tx unfiltered-db))
  (-max-eid [db] (dbi/-max-eid unfiltered-db))
  (-config [db] (dbi/-config unfiltered-db))
  (-ref-for [db a-ident] (dbi/-ref-for unfiltered-db a-ident))
  (-ident-for [db a-ref] (dbi/-ident-for unfiltered-db a-ref))

  dbi/ISearch
  (-search-context [db] (dbi/context-with-xform-after
                         (dbi/-search-context unfiltered-db)
                         (filter (.-pred db))))
  (-search [db pattern context]
           (dbi/-search unfiltered-db pattern context))
  (-batch-search [db pattern-mask batch-fn context]
                 (dbi/-batch-search unfiltered-db pattern-mask batch-fn context))

  dbi/IIndexAccess
  (-datoms [db index cs context]
           (dbi/-datoms unfiltered-db index cs context))

  (-seek-datoms [db index cs context]
                (dbi/-seek-datoms unfiltered-db index cs context))

  (-rseek-datoms [db index cs context]
                 (dbi/-rseek-datoms unfiltered-db index cs context))

  (-index-range [db attr start end context]
                (deeper-index-range unfiltered-db
                                    db
                                    attr
                                    start end
                                    context)))

;; HistoricalDB

(defrecord-updatable HistoricalDB [origin-db]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (dbi/datoms db :eavt []))
       ICounted (-count [db] (count (dbi/datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on HistoricalDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on HistoricalDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on HistoricalDB"))))

       IAssociative
       (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on HistoricalDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on HistoricalDB")))]
      :clj
      [IPersistentCollection
       (count [db] (count (dbi/datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on HistoricalDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on HistoricalDB")))

       Seqable
       (seq [db] (dbi/datoms db :eavt []))

       Associative
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on HistoricalDB")))])

  dbi/IDB
  (-schema [db] (dbi/-schema origin-db))
  (-rschema [db] (dbi/-rschema origin-db))
  (-system-entities [db] (dbi/-system-entities origin-db))
  (-attrs-by [db property] (dbi/-attrs-by origin-db property))
  (-temporal-index? [db] (dbi/-keep-history? origin-db))
  (-keep-history? [db] (dbi/-keep-history? origin-db))
  (-max-tx [db] (dbi/-max-tx origin-db))
  (-max-eid [db] (dbi/-max-eid origin-db))
  (-config [db] (dbi/-config origin-db))
  (-ref-for [db a-ident] (dbi/-ref-for origin-db a-ident))
  (-ident-for [db a-ref] (dbi/-ident-for origin-db a-ref))

  dbi/IHistory
  (-time-point [db] nil)
  (-origin [db] origin-db)

  dbi/ISearch
  (-search-context [db]
                   (-> origin-db
                       dbi/-search-context
                       dbi/context-with-history))
  (-search [db pattern context]
           (dbi/-search origin-db pattern context))
  (-batch-search [db pattern-mask batch-fn context]
                 (dbi/-batch-search origin-db pattern-mask batch-fn context))

  dbi/IIndexAccess
  (-datoms [db index-type cs context] (dbi/-datoms origin-db index-type cs context))

  (-seek-datoms [db index-type cs context] (dbi/-seek-datoms origin-db index-type cs context))

  (-rseek-datoms [db index-type cs context] (dbi/-seek-datoms origin-db index-type cs context))

  (-index-range [db attr start end context] (deeper-index-range origin-db db attr start end context)))

;; AsOfDB

(defrecord-updatable AsOfDB [origin-db time-point]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (dbi/datoms db :eavt []))
       ICounted (-count [db] (count (dbi/datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on AsOfDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on AsOfDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on AsOfDB"))))

       IAssociative
       (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on AsOfDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on AsOfDB")))]
      :clj
      [IPersistentCollection
       (count [db] (count (dbi/datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on AsOfDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on AsOfDB")))

       Seqable
       (seq [db] (dbi/datoms db :eavt []))

       Associative
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on AsOfDB")))])

  dbi/IDB
  (-schema [db] (dbi/-schema origin-db))
  (-rschema [db] (dbi/-rschema origin-db))
  (-system-entities [db] (dbi/-system-entities origin-db))
  (-attrs-by [db property] (dbi/-attrs-by origin-db property))
  (-temporal-index? [db] (dbi/-keep-history? origin-db))
  (-keep-history? [db] (dbi/-keep-history? origin-db))
  (-max-tx [db] (dbi/-max-tx origin-db))
  (-max-eid [db] (dbi/-max-eid origin-db))
  (-config [db] (dbi/-config origin-db))
  (-ref-for [db a-ident] (dbi/-ref-for origin-db a-ident))
  (-ident-for [db a-ref] (dbi/-ident-for origin-db a-ref))

  dbi/IHistory
  (-time-point [db] time-point)
  (-origin [db] origin-db)

  dbi/ISearch
  (-search-context [db] (dbi/context-with-temporal-timepred
                         (dbi/-search-context origin-db)
                         (as-of-pred time-point)))
  (-search [db pattern context]
           (dbi/-search origin-db pattern context))
  (-batch-search [db pattern batch-fn context]
                 (dbi/-batch-search origin-db pattern batch-fn context))

  dbi/IIndexAccess
  (-datoms [db index-type cs context]
           (dbi/-datoms origin-db index-type cs context))

  (-seek-datoms [db index-type cs context]
                (dbi/-seek-datoms origin-db index-type cs context))

  (-rseek-datoms [db index-type cs context]
                 (dbi/-rseek-datoms origin-db index-type cs context))

  (-index-range [db attr start end context]
                (deeper-index-range origin-db db attr start end context)))

(defrecord-updatable SinceDB [origin-db time-point]
  #?@(:cljs
      [IEquiv (-equiv [db other] (equiv-db db other))
       ISeqable (-seq [db] (dbi/datoms db :eavt []))
       ICounted (-count [db] (count (dbi/datoms db :eavt [])))
       IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

       IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on SinceDB")))

       ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on SinceDB")))
                        ([_ _ _] (throw (js/Error. "-lookup is not supported on SinceDB"))))

       IAssociative
       (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on SinceDB")))
       (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on SinceDB")))]
      :clj
      [IPersistentCollection
       (count [db] (count (dbi/datoms db :eavt [])))
       (equiv [db o] (equiv-db db o))
       (cons [db [k v]] (throw (UnsupportedOperationException. "cons is not supported on SinceDB")))
       (empty [db] (throw (UnsupportedOperationException. "empty is not supported on SinceDB")))

       Seqable
       (seq [db] (dbi/datoms db :eavt []))

       Associative
       (assoc [db k v] (throw (UnsupportedOperationException. "assoc is not supported on SinceDB")))])

  dbi/IDB
  (-schema [db] (dbi/-schema origin-db))
  (-rschema [db] (dbi/-rschema origin-db))
  (-system-entities [db] (dbi/-system-entities origin-db))
  (-attrs-by [db property] (dbi/-attrs-by origin-db property))
  (-temporal-index? [db] (dbi/-keep-history? db))
  (-keep-history? [db] (dbi/-keep-history? origin-db))
  (-max-tx [db] (dbi/-max-tx origin-db))
  (-max-eid [db] (dbi/-max-eid origin-db))
  (-config [db] (dbi/-config origin-db))
  (-ref-for [db a-ident] (dbi/-ref-for origin-db a-ident))
  (-ident-for [db a-ref] (dbi/-ident-for origin-db a-ref))

  dbi/IHistory
  (-time-point [db] time-point)
  (-origin [db] origin-db)

  dbi/ISearch
  (-search-context [db] (dbi/context-with-temporal-timepred
                         (dbi/-search-context origin-db)
                         (since-pred time-point)))
  (-search [db pattern context]
           (dbi/-search origin-db pattern context))
  (-batch-search [db pattern batch-fn context]
                 (dbi/-batch-search origin-db pattern batch-fn context))

  dbi/IIndexAccess
  (dbi/-datoms [db index-type cs context]
               (dbi/-datoms origin-db index-type cs context))

  (dbi/-seek-datoms [db index-type cs context]
                    (dbi/-seek-datoms origin-db index-type cs context))

  (dbi/-rseek-datoms [db index-type cs context]
                     (dbi/-rseek-datoms origin-db index-type cs context))

  (dbi/-index-range [db attr start end context]
                    (deeper-index-range origin-db
                                        db
                                        attr
                                        start end
                                        context)))

(defn- equiv-db-index [x y]
  (loop [xs (seq x)
         ys (seq y)]
    (cond
      (nil? xs) (nil? ys)
      (= (first xs) (first ys)) (recur (next xs) (next ys))
      :else false)))

(defn- equiv-db [db other]
  (and (or (instance? DB other) (instance? FilteredDB other))
       (or (not (instance? DB other)) (= (hash db) (hash other)))
       (= (dbi/-schema db) (dbi/-schema other))
       (equiv-db-index (dbi/datoms db :eavt []) (dbi/datoms other :eavt []))))

#?(:cljs
   (defn pr-db [db w opts]
     (-write w "#datahike/DB {")
     (-write w (str ":max-tx " (dbi/-max-tx db) " "))
     (-write w (str ":max-eid " (dbi/-max-eid db) " "))
     (-write w "}")))

#?(:clj
   (do
     (defn pr-db [db, ^Writer w]
       (.write w (str "#datahike/DB {"))
       (.write w (str ":store-id ["
                      (store/store-identity (:store (dbi/-config db)))
                      " " (:branch (dbi/-config db))  "] "))
       (.write w (str ":commit-id " (pr-str (:datahike/commit-id (:meta db))) " "))
       (.write w (str ":max-tx " (dbi/-max-tx db) " "))
       (.write w (str ":max-eid " (dbi/-max-eid db)))
       (.write w "}"))

     (defn pr-hist-db [db ^Writer w flavor time-point?]
       (.write w (str "#datahike/" flavor " {"))
       (.write w ":origin ")
       (binding [*out* w]
         (pr (dbi/-origin db)))
       (when time-point?
         (.write w " :time-point ")
         (binding [*out* w]
           (pr (dbi/-time-point db))))
       (.write w "}"))

     (defmethod print-method DB [db w] (pr-db db w))
     (defmethod print-method FilteredDB [db w] (pr-db db w)) ;; why not with "FilteredDB" ?
     (defmethod print-method HistoricalDB [db w] (pr-hist-db db w "HistoricalDB" false))
     (defmethod print-method AsOfDB [db w] (pr-hist-db db w "AsOfDB" true))
     (defmethod print-method SinceDB [db w] (pr-hist-db db w "SinceDB" true))

     (defmethod pp/simple-dispatch Datom [^Datom d]
       (pp/pprint-logical-block :prefix "#datahike/Datom [" :suffix "]"
                                (pp/write-out (.-e d))
                                (.write ^Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (.-a d))
                                (.write ^Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (.-v d))
                                (.write ^Writer *out* " ")
                                (pp/pprint-newline :linear)
                                (pp/write-out (datom-tx d))))

     (defn- pp-db [db ^Writer w]
       (pp/pprint-logical-block :prefix "#datahike/DB {" :suffix "}"
                                (pp/pprint-logical-block
                                 (pp/write-out :max-tx)
                                 (.write ^Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out (dbi/-max-tx db))
                                 (.write ^Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out :max-eid)
                                 (.write ^Writer *out* " ")
                                 (pp/pprint-newline :linear)
                                 (pp/write-out (dbi/-max-eid db)))
                                (pp/pprint-newline :linear)))

     (defmethod pp/simple-dispatch DB [db] (pp-db db *out*))
     (defmethod pp/simple-dispatch FilteredDB [db] (pp-db db *out*))))

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

(defn init-max-eid [eavt]
  ;; solved with reverse slice first in datascript
  (if-let [datoms (di/-slice
                   eavt
                   (datom e0 nil nil tx0)
                   (datom (dec tx0) nil nil txmax)
                   :eavt)]
    (-> datoms vec rseq first :e)                           ;; :e of last datom in slice
    e0))

(defn get-max-tx [eavt]
  (transduce (map (fn [^Datom d] (datom-tx d))) max tx0 (di/-all eavt)))

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
               (let [v-ref (idents v)
                       ;; datom val can be system schema eid (v-ref), or ident or regular (v)
                     d-val (if (and (not= k :db/ident) v-ref) v-ref v)]
                 (conj coll (dd/datom id (idents k) d-val tx0))))
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
  ([] (empty-db nil nil nil))
  ([schema] (empty-db schema nil nil))
  ([schema user-config] (empty-db schema user-config nil))
  ([schema user-config store]
   {:pre [(or (nil? schema) (map? schema) (coll? schema))]}
   (let [complete-config (merge (dc/storeless-config) user-config)
         _ (dc/validate-config complete-config)
         complete-config (dc/remove-nils complete-config)
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
         rschema (dbu/rschema complete-schema)
         ident-ref-map (if attribute-refs? (get-ident-ref-map complete-schema) {})
         ref-ident-map (if attribute-refs? (clojure.set/map-invert ident-ref-map) {})
         system-entities (if attribute-refs? c/system-entities #{})
         indexed (if attribute-refs?
                   (set (map ident-ref-map (:db/index rschema)))
                   (:db/index rschema))
         index-config (merge (:index-config complete-config)
                             {:indexed indexed})
         eavt (if attribute-refs?
                (di/init-index index store ref-datoms :eavt 0 index-config)
                (di/empty-index index store :eavt index-config))
         aevt (if attribute-refs?
                (di/init-index index store ref-datoms :aevt 0 index-config)
                (di/empty-index index store :aevt index-config))
         indexed-datoms (filter (fn [[_ a _ _]] (contains? indexed a)) ref-datoms)
         avet (if attribute-refs?
                (di/init-index index store indexed-datoms :avet 0 index-config)
                (di/empty-index index store :avet index-config))
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
         {:temporal-eavt eavt
          :temporal-aevt aevt
          :temporal-avet avet}))))))

(defn ^DB init-db
  ([datoms] (init-db datoms nil nil nil))
  ([datoms schema] (init-db datoms schema nil nil))
  ([datoms schema user-config] (init-db datoms schema user-config nil))
  ([datoms schema user-config store]
   (validate-schema schema)
   (let [{:keys [index keep-history? attribute-refs?] :as complete-config}  (merge (dc/storeless-config) user-config)
         _ (dc/validate-config complete-config)
         complete-schema (merge schema
                                (if attribute-refs?
                                  c/ref-implicit-schema
                                  c/non-ref-implicit-schema))
         rschema (dbu/rschema complete-schema)
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
         avet (di/init-index index store indexed-datoms :avet op-count index-config)
         eavt (di/init-index index store new-datoms :eavt op-count index-config)
         aevt (di/init-index index store new-datoms :aevt op-count index-config)
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
                      :hash (reduce #(+ %1 (hash %2)) 0 datoms)
                      :system-entities system-entities
                      :meta (tools/meta-data)
                      :ref-ident-map ref-ident-map
                      :ident-ref-map ident-ref-map}
                     (when keep-history?
                       {:temporal-eavt (di/empty-index index store :eavt index-config)
                        :temporal-aevt (di/empty-index index store :aevt index-config)
                        :temporal-avet (di/empty-index index store :avet index-config)}))))))

(defn metrics [^DB db]
  (let [update-count-in (fn [m ks] (update-in m ks #(if % (inc %) 1)))
        counts-map (->> (di/-seq (.-eavt db))
                        (reduce (fn [m ^Datom datom]
                                  (-> m
                                      (update-count-in [:per-attr-counts (dbi/-ident-for db (.-a datom))])
                                      (update-count-in [:per-entity-counts (.-e datom)])))
                                {:per-attr-counts    {}
                                 :per-entity-counts  {}}))
        sum-indexed-attr-counts (fn [attr-counts] (->> attr-counts
                                                       (m/filter-keys #(contains? (:db/index (.-rschema db)) %))
                                                       vals
                                                       (reduce + 0)))]
    (cond-> (merge counts-map
                   {:count (di/-count (.-eavt db))
                    :avet-count (->> (:per-attr-counts counts-map)
                                     sum-indexed-attr-counts)})
      (dbi/-keep-history? db)
      (merge {:temporal-count (di/-count (.-temporal-eavt db))
              :temporal-avet-count (->> (di/-seq (.-temporal-eavt db))
                                        (reduce (fn [m ^Datom datom] (update-count-in m [(dbi/-ident-for db (.-a datom))]))
                                                {})
                                        sum-indexed-attr-counts)}))))

