(ns ^:no-doc datahike.db.utils
  (:require
   [clojure.data]
   [clojure.walk]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :refer [datom datom-tx index-type->cmp-quick]]
   [datahike.db.interface :as dbi]
   [datahike.index :as di]
   [datahike.schema :as ds]
   [datahike.tools :refer [raise merge-distinct-sorted-seqs
                           distinct-sorted-seq?]])
  #?(:cljs (:require-macros [datahike.datom :refer [datom]]
                            [datahike.tools :refer [raise]]))
  #?(:clj (:import [datahike.datom Datom])))

(defn #?@(:clj [^Boolean is-attr?]
          :cljs [^boolean is-attr?]) [db attr property]
  (let [a-ident (if (and (:attribute-refs? (dbi/-config db))
                         (number? attr))
                  (dbi/-ident-for db attr)
                  attr)]
    (contains? (dbi/-attrs-by db property) a-ident)))

(defn #?@(:clj [^Boolean multival?]
          :cljs [^boolean multival?]) [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn #?@(:clj [^Boolean ref?]
          :cljs [^boolean ref?]) [db attr]
  (is-attr? db attr :db.type/ref))

(defn #?@(:clj [^Boolean system-attrib-ref?]
          :cljs [^boolean system-attrib-ref?]) [db attr]
  (is-attr? db attr :db/systemAttribRef))

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
   E.g. ':a' is an attribute part of the tuple attribute ':a+b'.
   (tuple-source? :a) returns true."
  [db attr]
  (is-attr? db attr :db/attrTuples))

(defn #?@(:clj  [^Boolean tuple?]
          :cljs [^boolean tuple?])
  "Returns true if 'attr' is a tuple attribute.
   I.e., if 'attr' value is of type ':db.type/tuple'"
  [db attr]
  (is-attr? db attr :db.type/tuple))

(defn #?@(:clj [^Boolean reverse-ref?]
          :cljs [^boolean reverse-ref?])
  [ident]
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

(defn db? [x]
  (and (satisfies? dbi/ISearch x)
       (satisfies? dbi/IIndexAccess x)
       (satisfies? dbi/IDB x)))

(defn numeric-entid? [x]
  (and (number? x) (pos? x)))

(defn entid
  ([db eid] (entid db eid nil))
  ([db eid error-code]
   {:pre [(db? db)]}
   (cond
     (numeric-entid? eid) eid
     (sequential? eid)
     (let [[attr value] eid]
       (cond
         (not= (count eid) 2)
         (or error-code
             (raise "Lookup ref should contain 2 elements: " eid
                    {:error :lookup-ref/syntax, :entity-id eid}))
         (not (is-attr? db attr :db/unique))
         (or error-code
             (raise "Lookup ref attribute should be marked as :db/unique: " eid
                    {:error :lookup-ref/unique, :entity-id eid}))
         (nil? value)
         nil
         :else
         (-> (dbi/datoms db :avet eid) first :e)))

     #?@(:cljs [(array? eid) (recur db (array-seq eid) error-code)])

     (keyword? eid)
     (-> (dbi/datoms db :avet [:db/ident eid]) first :e)

     :else
     (or error-code
         (raise "Expected number or lookup ref for entity id, got " eid
                {:error :entity-id/syntax, :entity-id eid})))))

(defn entid-strict
  ([db eid] (entid-strict db eid nil))
  ([db eid error-code]
   (or (entid db eid error-code)
       error-code
       (raise "Nothing found for entity id " eid
              {:error :entity-id/missing
               :entity-id eid}))))

(defn entid-some [db eid]
  (when eid
    (entid-strict db eid)))

(defn attr-has-ref? [db attr]
  (and (not (nil? attr))
       (:attribute-refs? (dbi/-config db))))

(defn attr-ref-or-ident [db attr]
  (if (and (not (number? attr))
           (attr-has-ref? db attr))
    (dbi/-ref-for db attr)
    attr))

(defn attr-info
  "Returns identifier name and reference value of an attributes. Both values are identical for non-reference databases."
  [db attr]
  (if (attr-has-ref? db attr)
    (if (number? attr)
      {:ident (dbi/-ident-for db attr) :ref attr}
      {:ident attr :ref (dbi/-ref-for db attr)})
    {:ident attr :ref attr}))

(defn ident-name? [x]
  (or (keyword? x) (string? x)))

(defn validate-attr-ident [a-ident at db]
  (when-not (ident-name? a-ident)
    (raise "Bad entity attribute " a-ident " at " at ", expected keyword or string"
           {:error :transact/syntax, :attribute a-ident, :context at}))
  (when (and (= :write (:schema-flexibility (dbi/-config db)))
             (not (or (ds/meta-attr? a-ident) (ds/schema-attr? a-ident) (ds/entity-spec-attr? a-ident))))
    (if-let [db-idents (:db/ident (dbi/-rschema db))]
      (let [attr (if (reverse-ref? a-ident)
                   (reverse-ref a-ident)
                   a-ident)]
        (when-not (db-idents attr)
          (raise "Bad entity attribute " a-ident " at " at ", not defined in current schema"
                 {:error :transact/schema :attribute a-ident :context at})))
      (raise "No schema found in db."
             {:error :transact/schema :attribute a-ident :context at}))))

(defn resolve-datom [db e a v t default-e default-tx]
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

(defn merge-datoms [index-type a b]
  (if index-type
    (merge-distinct-sorted-seqs
     (index-type->cmp-quick index-type false)
     a b)
    (concat a (lazy-seq (remove (set a) b)))))

(defn distinct-sorted-datoms? [index-type datoms]
  (when index-type
    (distinct-sorted-seq?
     (index-type->cmp-quick index-type false)
     datoms)))

(defn distinct-datoms
  ([db index-type current-datoms history-datoms]
   (if  (dbi/-keep-history? db)
     (merge-datoms
      index-type
      (filter (fn [datom]
                (let [a (:a datom)]
                  (or (no-history? db a)
                      (multival? db a))))
              current-datoms)
      history-datoms)
     current-datoms)))

(defn temporal-datoms [db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (components->pattern db index-type cs e0 tx0)
        to (components->pattern db index-type cs emax txmax)]
    (distinct-datoms db
                     index-type
                     (di/-slice index from to index-type)
                     (di/-slice temporal-index from to index-type))))

(def temporal-context (assoc dbi/base-context
                             :temporal true
                             :historical false))

(defn filter-txInstant [datoms pred db]
  (let [txInstant (if (:attribute-refs? (dbi/-config db))
                    (dbi/-ref-for db :db/txInstant)
                    :db/txInstant)]
    (into #{}
          (comp
           (map datom-tx)
           (distinct)
           (mapcat (fn [tx] (dbi/-datoms db :eavt [tx] temporal-context)))
           (keep (fn [^Datom d]
                   (when (and (= txInstant (.-a d)) (pred d))
                     (.-e d)))))
          datoms)))

(defn validate-attr [attr at db]
  (if (:attribute-refs? (dbi/-config db))
    (do (when-not (number? attr)
          (raise "Bad entity attribute " attr " at " at ", expected reference number"
                 {:error :transact/syntax, :attribute attr, :context at}))
        (if-let [a-ident (get-in db [:ref-ident-map attr])]
          (validate-attr-ident a-ident at db)
          (raise "Bad entity attribute " attr " at " at ", not defined in current schema"
                 {:error :transact/schema :attribute attr :context at})))
    (validate-attr-ident attr at db)))

(defn normalize-and-validate-attr [attr at db]
  (let [attr (attr-ref-or-ident db attr)]
    (validate-attr attr at db)
    attr))

(defn attr->properties [k v]
  (case v
    :db.unique/identity [:db/unique :db.unique/identity :db/index]
    :db.unique/value [:db/unique :db.unique/value :db/index]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref [:db.type/ref :db/index]
    :db.type/tuple [:db.type/tuple]

    :db.type/valueType [:db/systemAttribRef]
    :db.type/cardinality [:db/systemAttribRef]
    :db.type/unique [:db/systemAttribRef]

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

(defn rschema [schema]
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
