(ns ^:no-doc datahike.impl.entity
  (:refer-clojure :exclude [keys get])
  (:require [#?(:cljs cljs.core :clj clojure.core) :as c]
            [datahike.db :as db]
            [clojure.core.async :as async]
            [hitchhiker.tree.utils.cljs.async :as ha])
  #?(:clj (:import [datahike.java IEntity])))

(declare entity ->Entity equiv-entity lookup-entity touch)

(defn- entid [db eid]
  (ha/go-try
   (when (or (number? eid)
             (sequential? eid)
             (keyword? eid))
     (ha/<? (db/entid db eid)))))

(defn entity [db eid]
  {:pre [(db/db? db)]}
  ;(println "The entity function: " db " eid " eid)
  (ha/go-try
   #_(ha/<? ;;pre-touch
    (touch))
   (when-let [e (ha/<? (entid db eid))]
     (let [return-entity (->Entity db e (volatile! false) (volatile! {}))]
      ;(println "return entity " return-entity)
      ;(touch return-entity)
       return-entity))))

(defn- entity-attr [db a datoms]
  (ha/go-try
   (if (db/multival? db a)
     (if (db/ref? db a)
       (ha/<? (ha/reduce< #(ha/go-try (conj %1 (ha/<? (entity db (:v %2))))) #{} datoms))
       (reduce #(conj %1 (:v %2)) #{} datoms))
     (if (db/ref? db a)
       (ha/<? (entity db (:v (first datoms))))
       (:v (first datoms))))))

(defn- -lookup-backwards [db eid attr not-found]
  ;; becomes async
  (ha/go-try
   (if-let [datoms (not-empty (ha/<? (db/-search db [nil attr eid])))]
     (if (db/component? db attr)
       (ha/<? (entity db (:e (first datoms))))
       (ha/<? (ha/reduce< #(ha/go-try (conj %1 (ha/<? (entity db (:e %2))))) #{} datoms)))
     not-found)))

#?(:cljs
   (defn- multival->js [val]
     (when val (to-array val))))



#?(:cljs
   (defn- js-seq [e]
     (touch e)
     (for [[a v] @(.-cache e)]
       (if (db/multival? (.-db e) a)
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
       #_(keys [this]
             (es6-iterator (c/keys this)))  ; TODO: throw exceptions
       #_(entries [this]
                (es6-entries-iterator (js-seq this)))
       #_(values [this]
               (es6-iterator (map second (js-seq this))))
       #_(has [this attr]
            (not (nil? (.get this attr))))
       (get [this attr]                     ; TODO: Needs to be async or use cache
            (ha/go-try
             (if (= attr ":db/id")
               eid
               (if (db/reverse-ref? attr)
                 (do 
                   (println "reverse lookup")
                   (-> (ha/<? (-lookup-backwards db eid (db/reverse-ref attr) nil))
                         multival->js))
                 (cond-> (ha/<? (lookup-entity this attr))
                   (db/multival? db attr) multival->js)))))
       #_(forEach [this f]
                (doseq [[a v] (js-seq this)]
                  (f v a this)))
       #_(forEach [this f use-as-this]
                (doseq [[a v] (js-seq this)]
                  (.call f use-as-this v a this)))

       ;; js fallbacks
       #_(key_set   [this] (to-array (c/keys this)))
       #_(entry_set [this] (to-array (map to-array (js-seq this))))
       #_(value_set [this] (to-array (map second (js-seq this))))

       IEquiv
       (-equiv [this o] (equiv-entity this o))

       IHash
       (-hash [_]
              (hash eid)) ;; db? ; TODO: (hash db)

       ISeqable
       (-seq [this]
             #_(touch this) ;; TODO: If not already touched throw an exception in cljs (defn ensure-touched ..)
             (if @(.-touched this) 
               (seq @cache)
               (throw (js/Error. "Entity not touched."))))

       ICounted
       (-count [this]
               #_(touch this)
               (if @(.-touched this)
                 (count @cache)
                 (throw (js/Error. "Entity not touched."))))

       ILookup
       (-lookup [this attr]           (lookup-entity this attr nil))
       (-lookup [this attr not-found] (lookup-entity this attr not-found))

       IAssociative
       (-contains-key? [this k]
                       (contains? @cache k)
                       #_(not= ::nf (lookup-entity this k ::nf)))

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

       (empty [e]         (throw (UnsupportedOperationException.)))
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
   ;; (= db  (.-db ^Entity that))
   (= (.-eid this) (.-eid ^Entity that))))

(defn- lookup-entity
  ;; becomes async
  ([this attr] (lookup-entity this attr nil))
  ([^Entity this attr not-found]
   (ha/go-try
    (if (= attr :db/id)
      (.-eid this)
      (if (db/reverse-ref? attr)
        (ha/<? (-lookup-backwards (.-db this) (.-eid this) (db/reverse-ref attr) not-found))
        (if-some [v (@(.-cache this) attr)]
          v
          (if @(.-touched this) 
            not-found
            (if-some [datoms (not-empty (ha/<? (db/-search (.-db this) [(.-eid this) attr])))]
              (let [value (ha/<? (entity-attr (.-db this) attr datoms))]
                (vreset! (.-cache this) (assoc @(.-cache this) attr value))
                value)
              not-found))))))))


(defn touch-components [db a->v]
  ;; becomes async
  (ha/reduce< (fn [acc [a v]]
                (ha/go-try
                 (assoc acc a
                        (if (db/component? db a)
                          (if (db/multival? db a)
                            (set (ha/map< touch v))
                            (ha/<? (touch v)))
                          v))))
              {} a->v))

(defn- datoms->cache [db datoms]
  (ha/reduce< (fn [acc part]
                (ha/go-try
                 (let [a (:a (first part))]
                   (assoc acc a (ha/<? (entity-attr db a part))))))
          {} (partition-by :a datoms)))

(defn touch [^Entity e]
  ;; becomes async
  {:pre [(entity? e)]}
  #_(println "inside touch")
  (ha/go-try    ; This is kind of a lingering go-try maybe it needs to be closed?
   #_(println "touched? " @(.-touched e))
   (when-not @(.-touched e)
     (do
       (when-let [datoms (not-empty (ha/<? (db/-search (.-db e) [(.-eid e)])))]
         (vreset! (.-cache e) (->> datoms
                                   (datoms->cache (.-db e))
                                   (ha/<?)
                                   (touch-components (.-db e))
                                   (ha/<?)))
         (vreset! (.-touched e) true)
         #_(println "after touched? " @(.-touched e))
         )))
   e))

#?(:cljs (goog/exportSymbol "datahike.impl.entity.Entity" Entity))


