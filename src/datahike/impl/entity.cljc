(ns ^:no-doc datahike.impl.entity
  (:refer-clojure :exclude [keys get])
  (:require [#?(:cljs cljs.core :clj clojure.core)]
            [datahike.db :as db]
            [datahike.db.interface :as dbi]
            [datahike.db.utils :as dbu])
  #?(:clj (:import [datahike.java IEntity])))

(declare entity ->Entity equiv-entity lookup-entity touch)

(defn- entid [db eid]
  (when (or (number? eid)
            (sequential? eid)
            (keyword? eid))
    (dbu/entid db eid)))

(defn entity [db eid]
  {:pre [(dbu/db? db)]}
  (when-let [e (entid db eid)]
    (->Entity db e (volatile! false) (volatile! {}))))

(defn- entity-attr [db a-ident datoms]
  (if (dbu/multival? db a-ident)
    (if (dbu/ref? db a-ident)
      (reduce #(conj %1 (entity db (:v %2))) #{} datoms)
      (reduce #(conj %1 (:v %2)) #{} datoms))
    (if (dbu/ref? db a-ident)
      (entity db (:v (first datoms)))
      (:v (first datoms)))))

(defn- -lookup-backwards
  "Translate reverse attribute recording to database and find datoms"
  [db eid a-ident not-found]
  (let [a-db (if (:attribute-refs? (dbi/-config db)) (dbi/-ref-for db a-ident) a-ident)]
    (if-let [datoms (not-empty (dbi/search db [nil a-db eid]))]
      (if (dbu/component? db a-ident)
        (entity db (:e (first datoms)))
        (reduce #(conj %1 (entity db (:e %2))) #{} datoms))
      not-found)))

#?(:cljs
   (defn- multival->js [val]
     (when val (to-array val))))

#?(:cljs
   (defn- js-seq [e]
     (touch e)
     (for [[a v] @(.-cache e)]
       (if (dbu/multival? (.-db e) a)
         [a (multival->js v)]
         [a v]))))

(deftype Entity [db eid touched cache]
  #?@(:cljs
      [Object
       (toString [this]
                 (pr-str* this))
       (equiv [this other]
              (equiv-entity this other))

       ;; js/map interface
       (keys [this]
             (es6-iterator (cljs.core/keys this)))
       (entries [this]
                (es6-entries-iterator (js-seq this)))
       (values [this]
               (es6-iterator (map second (js-seq this))))
       (has [this a-ident]
            (not (nil? (.get this a-ident))))
       (get [this a-ident]
            (if (= a-ident ":db/id")
              eid
              (if (dbu/reverse-ref? a-ident)
                (-> (-lookup-backwards db eid (dbu/reverse-ref a-ident) nil)
                    multival->js)
                (cond-> (lookup-entity this a-ident)
                  (dbu/multival? db a-ident) multival->js))))
       (forEach [this f]
                (doseq [[a v] (js-seq this)]
                  (f v a this)))
       (forEach [this f use-as-this]
                (doseq [[a v] (js-seq this)]
                  (.call f use-as-this v a this)))

       ;; js fallbacks
       (key_set   [this] (to-array (cljs.core/keys this)))
       (entry_set [this] (to-array (map to-array (js-seq this))))
       (value_set [this] (to-array (map second (js-seq this))))

       IEquiv
       (-equiv [this o] (equiv-entity this o))

       IHash
       (-hash [_]
              (hash eid)) ;; db?

       ISeqable
       (-seq [this]
             (touch this)
             (seq @cache))

       ICounted
       (-count [this]
               (touch this)
               (count @cache))

       ILookup
       (-lookup [this a-ident]           (lookup-entity this a-ident nil))
       (-lookup [this a-ident not-found] (lookup-entity this a-ident not-found))

       IAssociative
       (-contains-key? [this k]
                       (not= ::nf (lookup-entity this k ::nf)))

       IFn
       (-invoke [this k]
                (lookup-entity this k))
       (-invoke [this k not-found]
                (lookup-entity this k not-found))

       IPrintWithWriter
       (-pr-writer [_ writer opts]
                   (-pr-writer (assoc @cache :db/id eid) writer opts))]

      :clj
      [Object
       IEntity
       (toString [e]      (pr-str (assoc @cache :db/id eid)))
       (hashCode [e]      (hash eid)) ; db?
       (equals [e o]      (equiv-entity e o))

       clojure.lang.Seqable
       (seq [e]           (touch e) (seq @cache))

       clojure.lang.Associative
       (equiv [e o]       (equiv-entity e o))
       (containsKey [e k] (not= ::nf (lookup-entity e k ::nf)))
       (entryAt [e k]     (some->> (lookup-entity e k) (clojure.lang.MapEntry. k)))

       (empty [e]         {})
       (assoc [e k v]     (throw (UnsupportedOperationException.)))
       (cons  [e [k v]]   (throw (UnsupportedOperationException.)))
       (count [e]         (touch e) (count @(.-cache e)))

       clojure.lang.ILookup
       (valAt [e k]       (lookup-entity e k))
       (valAt [e k not-found] (lookup-entity e k not-found))

       clojure.lang.IFn
       (invoke [e k]      (lookup-entity e k))
       (invoke [e k not-found] (lookup-entity e k not-found))]))

(defn entity? [x] (instance? Entity x))

#?(:clj
   (defmethod print-method Entity [e, ^java.io.Writer w]
     (.write w (str e))))

(defn- equiv-entity [^Entity this that]
  (and
   (instance? Entity that)
   (= (.-db ^Entity this)  (.-db ^Entity that))
   (= (.-eid this) (.-eid ^Entity that))))

(defn- lookup-entity
  ([this a-ident] (lookup-entity this a-ident nil))
  ([^Entity this a-ident not-found]
   (if (= a-ident :db/id)
     (.-eid this)
     (if (dbu/reverse-ref? a-ident)
       (-lookup-backwards (.-db this) (.-eid this) (dbu/reverse-ref a-ident) not-found)
       (if-some [v (@(.-cache this) a-ident)]
         v
         (if @(.-touched this)
           not-found
           (if-let [a-db (if (:attribute-refs? (dbi/-config (.-db this)))
                           (dbi/-ref-for (.-db this) a-ident)
                           a-ident)]
             (if-some [datoms (not-empty (dbi/search (.-db this) [(.-eid this) a-db]))]
               (let [value (entity-attr (.-db this) a-ident datoms)]
                 (vreset! (.-cache this) (assoc @(.-cache this) a-ident value))
                 value)
               not-found)
             not-found)))))))

(defn touch-components [db a->v]
  (reduce-kv (fn [acc a-ident v]
               (assoc acc a-ident
                      (if (dbu/component? db a-ident)
                        (if (dbu/multival? db a-ident)
                          (set (map touch v))
                          (touch v))
                        v)))
             {} a->v))

(defn- datoms->cache [db datoms]
  (reduce (fn [acc partition]
            (let [a-db (:a (first partition))
                  a-ident (if (:attribute-refs? (dbi/-config db))
                            (dbi/-ident-for db a-db)
                            a-db)]
              (assoc acc a-ident (entity-attr db a-ident partition))))
          {} (partition-by :a datoms)))

(defn touch [^Entity e]
  {:pre [(entity? e)]}
  (when-not @(.-touched e)
    (when-let [datoms (not-empty (dbi/search (.-db e) [(.-eid e)]))]
      (vreset! (.-cache e) (->> datoms
                                (datoms->cache (.-db e))
                                (touch-components (.-db e))))
      (vreset! (.-touched e) true)))
  e)

#?(:cljs (goog/exportSymbol "datahike.impl.entity.Entity" Entity))

(defn entity-db [^Entity entity]
  (.-db entity))
