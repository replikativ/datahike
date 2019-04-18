(ns ^:no-doc datahike.db
  (:require
    #?(:cljs [goog.array :as garray])
    [clojure.walk]
    [clojure.data]
    #?(:clj [clojure.pprint :as pp])
    [hitchhiker.tree.core :as hc :refer [<??]]
    [hitchhiker.tree.messaging :as hmsg]
    [hitchhiker.konserve :as kons]
    [hasch.core :refer [uuid]]
    [konserve.core :as k]
    [me.tonsky.persistent-sorted-set :as set]
    [me.tonsky.persistent-sorted-set.arrays :as arrays]
    [datahike.constants :refer [e0 tx0 emax txmax implicit-schema br br-sqrt]]
    #?(:clj [datahike.hash :refer [defrecord-updatable combine-hashes]])
    [datahike.datom :as dd :refer [datom case-tree datom-tx]]
    [datahike.index :refer [-slice -iterator hitchhiker-tree -insert! -retract! -count -seq]]
    #?(:clj [datahike.tools :refer [raise]]))
  #?(:cljs (:require-macros [datahike.db :refer [case-tree combine-cmp raise defrecord-updatable cond+]]))
  (:refer-clojure :exclude [seqable?])
  #?(:clj (:import [datahike.datom Datom]
                   [datahike.index HitchhikerTree])))

#?(:cljs
   (do
     (def Exception js/Error)
     (def IllegalArgumentException js/Error)
     (def UnsupportedOperationException js/Error)))


(extend-protocol hc/IKeyCompare
  clojure.lang.PersistentVector
  (compare [key1 key2]
    (if-not (= (class key2) clojure.lang.PersistentVector)
      -1                                                    ;; HACK for nil
      (let [[a b c d] key1
            [e f g h] key2]
        (dd/combine-cmp
          (hc/compare a e)
          (hc/compare b f)
          (hc/compare c g)
          (hc/compare d h)))))
  java.lang.String
  (compare [key1 key2]
    (compare key1 key2))
  clojure.lang.Keyword
  (compare [key1 key2]
    (compare key1 key2))
  nil
  (compare [key1 key2]
    (if (nil? key2)
      0 -1)))

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
  (-attrs-by [db property]))

;; ----------------------------------------------------------------------------

(declare hash-db hash-fdb equiv-db empty-db resolve-datom validate-attr components->pattern indexing?)
#?(:cljs (declare pr-db))

(defrecord-updatable DB [schema eavt aevt avet max-eid max-tx rschema hash]
                     #?@(:cljs
                         [IHash (-hash [db] (hash-db db))
                          IEquiv (-equiv [db other] (equiv-db db other))
                          ISeqable (-seq [db] (-seq (-iterator (.-eavt db))))
                          IReversible (-rseq [db] (-rseq (-iterator (.-eavt db))))
                          ICounted (-count [db] (count (-iterator (.-eavt db))))
                          IEmptyableCollection (-empty [db] (empty-db (.-schema db)))
                          IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))
                          ]
                         :clj
                         [Object (hashCode [db] (hash-db db))
                          clojure.lang.IHashEq (hasheq [db] (hash-db db))
                          clojure.lang.Seqable (seq [db] (-seq eavt))
                          clojure.lang.IPersistentCollection
                          (count [db] (-count eavt))
                          (equiv [db other] (equiv-db db other))
                          (empty [db] (empty-db schema))
                          ;;TODO: think about transient implementation of hitchhiker tree in memory
                          ])

                     IDB
                     (-schema [db] (.-schema db))
                     (-attrs-by [db property] ((.-rschema db) property))

                     ISearch
                     (-search [db pattern]
                              (let [[e a v tx] pattern
                                    create-eavt (fn [e a v t] (datom e a v t true))]
                                (case-tree [e a (some? v) tx]
                                           [(-slice eavt (datom e a v tx)) ;; e a v tx
                                            (-slice eavt (datom e a v tx0)) ;; e a v _
                                            (->> (-slice eavt (datom e a nil tx0)) ;; e a _ tx
                                                 (filter (fn [^Datom d] (= tx (datom-tx d)))))
                                            (-slice eavt (datom e a nil tx0)) ;; e a _ _
                                            (->> (-slice eavt (datom e nil nil tx0)) ;; e _ v tx
                                                 (filter (fn [^Datom d] (and (= v (.-v d))
                                                                             (= tx (datom-tx d))))))
                                            (->> (-slice eavt (datom e nil nil tx0)) ;; e _ v _
                                                 (filter (fn [^Datom d] (= v (.-v d)))))
                                            (->> (-slice eavt (datom e nil nil tx0)) ;; e _ _ tx
                                                 (filter (fn [^Datom d] (= tx (datom-tx d)))))
                                            (-slice eavt (datom e nil nil tx0)) ;; e _ _ _
                                            (if (indexing? db a) ;; _ a v tx
                                              (->> (-slice avet (datom e0 a v tx0))
                                                   (filter (fn [^Datom d] (= tx (datom-tx d)))))
                                              (->> (-slice aevt (datom e0 a nil tx0))
                                                   (filter (fn [^Datom d] (and (= v (.-v d))
                                                                               (= tx (datom-tx d)))))))
                                            (if (indexing? db a) ;; _ a v _
                                              (-slice avet (datom e0 a v tx0))
                                              (->> (-slice aevt (datom e0 a nil tx0))
                                                   (filter (fn [^Datom d] (= v (.-v d))))))
                                            (->> (-slice aevt (datom e0 a nil tx0)) ;; _ a _ tx
                                                 (filter (fn [^Datom d] (= tx (datom-tx d)))))
                                            (-slice aevt (datom e0 a nil tx0)) ;; _ a _ _
                                            (filter
                                              (fn [^Datom d] (and (= v (.-v d))
                                                                  (= tx (datom-tx d))))
                                              (map
                                                #(apply create-eavt (first %))
                                                (hc/lookup-fwd-iter (-iterator eavt) []))) ;; _ _ v tx
                                            (filter
                                              (fn [^Datom d] (= v (.-v d)))
                                              (map
                                                #(apply create-eavt (first %))
                                                (hc/lookup-fwd-iter (-iterator eavt) []))) ;; _ _ v _
                                            (filter
                                              (fn [^Datom d] (= tx (datom-tx d)))
                                              (map
                                                #(apply create-eavt (first %))
                                                (hc/lookup-fwd-iter (-iterator eavt) []))) ;; _ _ _ tx
                                            (map
                                              #(apply create-eavt (first %))
                                              (hc/lookup-fwd-iter (-iterator eavt) []))]))) ;; _ _ _ _

                     IIndexAccess
                     (-datoms [db index-type cs]
                              (let [index (get db index-type)]
                                (-slice index (components->pattern db index-type cs e0 tx0) (components->pattern db index-type cs emax txmax))))

                     (-seek-datoms [db index-type cs]
                                   (let [index (get db index-type)]
                                     (-slice index (components->pattern db index-type cs e0 tx0) (datom emax nil nil txmax))))

                     (-rseek-datoms [db index-type cs]
                                    (let [index (get db index-type)]
                                      (-slice index (components->pattern db index-type cs emax txmax) (datom e0 nil nil tx0))))

                     (-index-range [db attr start end]
                                   (when-not (indexing? db attr)
                                     (raise "Attribute" attr "should be marked as :db/index true" {}))
                                   (validate-attr attr (list '-index-range 'db attr start end))
                                   (let [avet (.-avet db)]
                                     (-slice avet
                                             (resolve-datom db nil attr start nil e0 tx0)
                                             (resolve-datom db nil attr end nil emax txmax))))

                     clojure.data/EqualityPartition
                     (equality-partition [x] :datahike/db)

                     clojure.data/Diff
                     (diff-similar [a b]
                                   (dd/diff-sorted (-seq (:eavt a)) (-seq (:eavt b)) dd/cmp-datoms-eavt-quick)))

(defn db? [x]
  (and (satisfies? ISearch x)
       (satisfies? IIndexAccess x)
       (satisfies? IDB x)))

;; ----------------------------------------------------------------------------
(defrecord-updatable FilteredDB [unfiltered-db pred hash]
                     #?@(:cljs
                         [IHash (-hash [db] (hash-fdb db))
                          IEquiv (-equiv [db other] (equiv-db db other))
                          ISeqable (-seq [db] (-datoms db :eavt []))
                          ICounted (-count [db] (count (-datoms db :eavt [])))
                          IPrintWithWriter (-pr-writer [db w opts] (pr-db db w opts))

                          IEmptyableCollection (-empty [_] (throw (js/Error. "-empty is not supported on FilteredDB")))

                          ILookup (-lookup ([_ _] (throw (js/Error. "-lookup is not supported on FilteredDB")))
                                           ([_ _ _] (throw (js/Error. "-lookup is not supported on FilteredDB"))))


                          IAssociative (-contains-key? [_ _] (throw (js/Error. "-contains-key? is not supported on FilteredDB")))
                          (-assoc [_ _ _] (throw (js/Error. "-assoc is not supported on FilteredDB")))]

                         :clj
                         [Object (hashCode [db] (hash-fdb db))

                          clojure.lang.IHashEq (hasheq [db] (hash-fdb db))

                          clojure.lang.IPersistentCollection
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
                     (-attrs-by [db property] (-attrs-by (.-unfiltered-db db) property))

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

;; ----------------------------------------------------------------------------

(defn attr->properties [k v]
  (case v
    :db.unique/identity [:db/unique :db.unique/identity :db/index]
    :db.unique/value [:db/unique :db.unique/value :db/index]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref [:db.type/ref :db/index]
    (when (true? v)
      (case k
        :db/isComponent [:db/isComponent]
        :db/index [:db/index]
        []))))

(defn- rschema [schema]
  (reduce-kv
    (fn [m attr keys->values]
      (reduce-kv
        (fn [m key value]
          (reduce
            (fn [m prop]
              (assoc m prop (conj (get m prop #{}) attr)))
            m (attr->properties key value)))
        m keys->values))
    {} schema))

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for " (pr-str {a {k v}}) ", expected one of " expected)
                    {:error     :schema/validation
                     :attribute a
                     :key       k
                     :value     v}))))

(defn- validate-schema [schema]
  (doseq [[a kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not= (:db/valueType kv) :db.type/ref))
        (throw (ex-info (str "Bad attribute specification for " a ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}")
                        {:error     :schema/validation
                         :attribute a
                         :key       :db/isComponent}))))
    (validate-schema-key a :db/unique (:db/unique kv) #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv) #{:db.type/ref})
    (validate-schema-key a :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many})))


(defn ^DB empty-db
  ([] (empty-db nil))
  ([schema & {:keys [db-type]}]
   {:pre [(or (nil? schema) (map? schema))]}
   (validate-schema schema)
   (map->DB
    {:schema  schema
     :rschema (rschema (merge implicit-schema schema))
     :eavt  (hitchhiker-tree)
     :aevt    (hitchhiker-tree :aevt)
     :avet    (hitchhiker-tree :avet)
     :max-eid e0
     :max-tx  tx0
     :hash    (atom 0)})))



(defn init-max-eid [eavt]
  ;; solved with reserse slice first in datascript
  (if-let [entries (-slice
                     eavt
                     (datom e0 nil nil tx0)
                     (datom (dec tx0) nil nil txmax))]
    (-> entries vec rseq first :e)                          ;; :e of last datom in slice
    e0))

(defn ^DB init-db
  ([datoms] (init-db datoms nil))
  ([datoms schema]
   (validate-schema schema)
   (let [rschema (rschema (merge implicit-schema schema))
         indexed (:db/index rschema)
         arr (cond-> datoms
               (not (arrays/array? datoms)) (arrays/into-array))
         _ (arrays/asort arr dd/cmp-datoms-eavt-quick)
         eavt-set (set/from-sorted-array dd/cmp-datoms-eavt arr)
         eavt-tree (<?? (hc/reduce< (fn [t ^Datom datom]
                                      (hmsg/insert t [(.-e datom)
                                                      (.-a datom)
                                                      (.-v datom)
                                                      (.-tx datom)] nil))
                                    (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))
                                    (seq datoms)))
         aevt-tree (<?? (hc/reduce< (fn [t ^Datom datom]
                                      (hmsg/insert t [(.-a datom)
                                                      (.-e datom)
                                                      (.-v datom)
                                                      (.-tx datom)] nil))
                                    (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))
                                    (seq datoms)))
         avet-tree (<?? (hc/reduce< (fn [t ^Datom datom]
                                      (hmsg/insert t [(.-a datom)
                                                      (.-v datom)
                                                      (.-e datom)
                                                      (.-tx datom)] nil))
                                    (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))
                                    (seq datoms)))
         eavt (hitchhiker-tree :eavt eavt-tree)
         ;; _ (arrays/asort arr dd/cmp-datoms-aevt-quick)
         ;;aevt (set/from-sorted-array dd/cmp-datoms-aevt arr)
         aevt (hitchhiker-tree :aevt aevt-tree)
         ;; avet-datoms (filter (fn [^Datom d] (contains? indexed (.-a d))) datoms)
         ;; avet-arr (to-array avet-datoms)
         ;; _ (arrays/asort avet-arr dd/cmp-datoms-avet-quick)
         ;;avet (set/from-sorted-array dd/cmp-datoms-avet avet-arr)
         avet (hitchhiker-tree :avet avet-tree)
         max-eid (init-max-eid eavt)
         max-tx (transduce (map (fn [^Datom d] (datom-tx d))) max tx0 eavt-set)]
     (map->DB {:schema  schema
               :rschema rschema
               :eavt    eavt
               :aevt    aevt
               :avet    avet
               :max-eid max-eid
               :max-tx  max-tx
               :hash    (atom 0)}))))

(defn ^DB copy-db [^DB {:keys [schema rschema eavt aevt avet max-eid max-tx hash store]}]
  (map->DB {:schema schema
            :rschema rschema
            :store store
            :eavt (hitchhiker-tree
                   :eavt
                   (<?? (hc/reduce< (fn [t ^Datom datom]
                                      (hmsg/insert t [(.-e datom)
                                                      (.-a datom)
                                                      (.-v datom)
                                                      (.-tx datom)] nil))
                                    (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))
                                    (-seq eavt))))
            :aevt (hitchhiker-tree
                   :aevt
                   (<?? (hc/reduce< (fn [t ^Datom datom]
                                      (hmsg/insert t [(.-a datom)
                                                      (.-e datom)
                                                      (.-v datom)
                                                      (.-tx datom)] nil))
                                    (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))
                                    (-seq eavt))))
            :avet (hitchhiker-tree
                   :avet (<?? (hc/reduce< (fn [t ^Datom datom]
                                            (hmsg/insert t [(.-a datom)
                                                            (.-v datom)
                                                            (.-e datom)
                                                            (.-tx datom)] nil))
                                          (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))
                                          (-seq eavt))))
            :max-eid max-eid
            :max-tx max-tx
            :hash hash}))

(defn- equiv-db-index [x y]
  (loop [xs (seq x)
         ys (seq y)]
    (cond
      (nil? xs) (nil? ys)
      (= (first xs) (first ys)) (recur (next xs) (next ys))
      :else false)))

(defn- hash-db [^DB db]
  (let [h @(.-hash db)]
    (if (zero? h)
      (reset! (.-hash db) (combine-hashes (hash (.-schema db))
                                          (hash (.-eavt db))))
      h)))

(defn- hash-fdb [^FilteredDB db]
  (let [h @(.-hash db)
        datoms (or (-datoms db :eavt []) #{})]
    (if (zero? h)
      (let [datoms (or (-datoms db :eavt []) #{})]
        (reset! (.-hash db) (combine-hashes (hash (-schema db))
                                            (hash-unordered-coll datoms))))
      h)))

(defn- equiv-db [db other]
  (and (or (instance? DB other) (instance? FilteredDB other))
       (= (-schema db) (-schema other))
       (equiv-db-index (-datoms db :eavt []) (-datoms other :eavt []))))

#?(:cljs
   (defn pr-db [db w opts]
     (-write w "#datahike/DB {")
     (-write w ":schema ")
     (pr-writer (-schema db) w opts)
     (-write w ", :datoms ")
     (pr-sequential-writer w
                           (fn [d w opts]
                             (pr-sequential-writer w pr-writer "[" " " "]" opts [(.-e d) (.-a d) (.-v d) (datom-tx d)]))
                           "[" " " "]" opts (-datoms db :eavt []))
     (-write w "}")))

#?(:clj
   (do
     (defn pr-db [db, ^java.io.Writer w]
       (.write w (str "#datahike/DB {"))
       (.write w ":schema ")
       (binding [*out* w]
         (pr (-schema db))
         (.write w ", :datoms [")
         (apply pr (map (fn [^Datom d] [(.-e d) (.-a d) (.-v d) (datom-tx d)]) (-datoms db :eavt []))))
       (.write w "]}"))

     (defmethod print-method DB [db w] (pr-db db w))
     (defmethod print-method FilteredDB [db w] (pr-db db w))

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
                                  (pp/write-out :schema)
                                  (.write w " ")
                                  (pp/pprint-newline :linear)
                                  (pp/write-out (:schema db)))
                                (.write w ", ")
                                (pp/pprint-newline :linear)

                                (pp/pprint-logical-block
                                  (pp/write-out :datoms)
                                  (.write w " ")
                                  (pp/pprint-newline :linear)
                                  (pp/pprint-logical-block :prefix "[" :suffix "]"
                                                           (pp/print-length-loop [aseq (seq db)]
                                                                                 (when aseq
                                                                                   (let [^Datom d (first aseq)]
                                                                                     (pp/write-out [(.-e d) (.-a d) (.-v d) (datom-tx d)])
                                                                                     (when (next aseq)
                                                                                       (.write w " ")
                                                                                       (pp/pprint-newline :linear)
                                                                                       (recur (next aseq))))))))))

     (defmethod pp/simple-dispatch DB [db] (pp-db db *out*))
     (defmethod pp/simple-dispatch FilteredDB [db] (pp-db db *out*))
     ))

(defn db-from-reader [{:keys [schema datoms]}]
  (init-db (map (fn [[e a v tx]] (datom e a v tx)) datoms) schema))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom [db e a v t default-e default-tx]
  (when a (validate-attr a (list 'resolve-datom 'db e a v t)))
  (datom
    (or (entid-some db e) default-e)                        ;; e
    a                                                       ;; a
    (if (and (some? v) (ref? db a))                         ;; v
      (entid-strict db v)
      v)
    (or (entid-some db t) default-tx)))                     ;; t

(defn- components->pattern [db index [c0 c1 c2 c3] default-e default-tx]
  (case index
    :eavt (resolve-datom db c0 c1 c2 c3 default-e default-tx)
    :aevt (resolve-datom db c1 c0 c2 c3 default-e default-tx)
    :avet (resolve-datom db c2 c0 c1 c3 default-e default-tx)))

;; ----------------------------------------------------------------------------

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

(defn #?@(:clj  [^Boolean is-attr?]
          :cljs [^boolean is-attr?]) [db attr property]
  (contains? (-attrs-by db property) attr))

(defn #?@(:clj  [^Boolean multival?]
          :cljs [^boolean multival?]) [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn #?@(:clj  [^Boolean ref?]
          :cljs [^boolean ref?]) [db attr]
  (is-attr? db attr :db.type/ref))

(defn #?@(:clj  [^Boolean component?]
          :cljs [^boolean component?]) [db attr]
  (is-attr? db attr :db/isComponent))

(defn #?@(:clj  [^Boolean indexing?]
          :cljs [^boolean indexing?]) [db attr]
  (is-attr? db attr :db/index))

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
             {:error     :entity-id/missing
              :entity-id eid})))

(defn entid-some [db eid]
  (when eid
    (entid-strict db eid)))

;;;;;;;;;; Transacting

(defn validate-datom [db ^Datom datom]
  (when (and (dd/datom-added datom)
             (is-attr? db (.-a datom) :db/unique))
    (when-let [found (not-empty (-datoms db :avet [(.-a datom) (.-v datom)]))]
      (raise "Cannot add " datom " because of unique constraint: " found
             {:error     :transact/unique
              :attribute (.-a datom)
              :datom     datom}))))

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn- validate-attr [attr at]
  (when-not (or (keyword? attr) (string? attr))
    (raise "Bad entity attribute " attr " at " at ", expected keyword or string"
           {:error :transact/syntax, :attribute attr, :context at})))

(defn- validate-val [v at]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at})))

(defn- current-tx [report]
  (inc (get-in report [:db-before :max-tx])))

(defn- next-eid [db]
  (inc (:max-eid db)))

(defn- #?@(:clj  [^Boolean tx-id?]
           :cljs [^boolean tx-id?])
  [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx")                                ;; for datahike.js interop
      (= e "datomic.tx")
      (= e "datahike.tx")))

(defn- #?@(:clj  [^Boolean tempid?]
           :cljs [^boolean tempid?])
  [x]
  (or (and (number? x) (neg? x)) (string? x)))

(defn- advance-max-eid [db eid]
  (cond-> db
          (and (> eid (:max-eid db))
               (< eid tx0))                                 ;; do not trigger advance if transaction id was referenced
          (assoc :max-eid eid)))

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

;; In context of `with-datom` we can use faster comparators which
;; do not check for nil (~10-15% performance gain in `transact`)

(defn- with-datom [{:keys [eavt aevt avet] :as db} ^Datom datom]
  (validate-datom db datom)
  (let [indexing? (indexing? db (.-a datom))]
    (if (dd/datom-added datom)
      (do
        (-insert! eavt datom)
        (-insert! aevt datom)
        (when indexing? (-insert! avet datom))
        (cond-> db
                true (advance-max-eid (.-e datom))
                true (assoc :hash (atom 0))))
      (if-some [removing ^Datom (first (-search db [(.-e datom) (.-a datom) (.-v datom)]))]
        (do
          (-retract! eavt removing)
          (-retract! aevt removing)
          (when indexing? (-retract! avet removing))
          (assoc db :hash (atom 0)))
        db))))

(defn- transact-report [report datom]
  (-> report
      (update-in [:db-after] with-datom datom)
      (update-in [:tx-data] conj datom)))

(defn #?@(:clj  [^Boolean reverse-ref?]
          :cljs [^boolean reverse-ref?]) [attr]
  (cond
    (keyword? attr)
    (= \_ (nth (name attr) 0))

    (string? attr)
    (boolean (re-matches #"(?:([^/]+)/)?_([^/]+)" attr))

    :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))

(defn reverse-ref [attr]
  (cond
    (keyword? attr)
    (if (reverse-ref? attr)
      (keyword (namespace attr) (subs (name attr) 1))
      (keyword (namespace attr) (str "_" (name attr))))

    (string? attr)
    (let [[_ ns name] (re-matches #"(?:([^/]+)/)?([^/]+)" attr)]
      (if (= \_ (nth name 0))
        (if ns (str ns "/" (subs name 1)) (subs name 1))
        (if ns (str ns "/_" name) (str "_" name))))

    :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))


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
             {:error     :transact/upsert
              :entity    entity
              :assertion acc}))))


(defn- upsert-eid [db entity]
  (when-let [idents (not-empty (-attrs-by db :db.unique/identity))]
    (->>
      (reduce-kv
        (fn [acc a v]                                       ;; acc = [e a v]
          (if (contains? idents a)
            (if-some [e (:e (first (-datoms db :avet [a v])))]
              (cond
                (nil? acc) [e a v]                          ;; first upsert
                (= (get acc 0) e) acc                       ;; second+ upsert, but does not conflict
                :else
                (let [[_e _a _v] acc]
                  (raise "Conflicting upserts: " [_a _v] " resolves to " _e
                         ", but " [a v] " resolves to " e
                         {:error     :transact/upsert
                          :entity    entity
                          :assertion [e a v]
                          :conflict  [_e _a _v]})))
              acc)                                          ;; upsert attr, but resolves to nothing
            acc))                                           ;; non-upsert attr
        nil
        entity)
      (check-upsert-conflict entity)
      first)))                                              ;; getting eid from acc


;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a vs]
  (cond
    ;; not a multival context
    (not (or (reverse-ref? a)
             (multival? db a)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (or (arrays/array? vs)
             (and (coll? vs) (not (map? vs)))))
    [vs]

    ;; probably lookup ref
    (and (= (count vs) 2)
         (is-attr? db (first vs) :db.unique/identity))
    [vs]

    :else vs))

(defn- explode [db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] entity
          :when (not= a :db/id)
          :let [_ (validate-attr a {:db/id eid, a vs})
                reverse? (reverse-ref? a)
                straight-a (if reverse? (reverse-ref a) a)
                _ (when (and reverse? (not (ref? db straight-a)))
                    (raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                           {:error :transact/syntax, :attribute a, :context {:db/id eid, a vs}}))]
          v (maybe-wrap-multival db a vs)]
      (if (and (ref? db straight-a) (map? v))               ;; another entity specified as nested map
        (assoc v (reverse-ref a) eid)
        (if reverse?
          [:db/add v straight-a eid]
          [:db/add eid straight-a v])))))

(defn- transact-add [report [_ e a v tx :as ent]]
  (validate-attr a ent)
  (validate-val v ent)
  (let [tx (or tx (current-tx report))
        db (:db-after report)
        e (entid-strict db e)
        v (if (ref? db a) (entid-strict db v) v)
        new-datom (datom e a v tx)]
    (if (multival? db a)
      (if (empty? (-search db [e a v]))
        (transact-report report new-datom)
        report)
      (if-some [^Datom old-datom (first (-search db [e a]))]
        (if (= (.-v old-datom) v)
          report
          (-> report
              (transact-report (datom e a (.-v old-datom) tx false))
              (transact-report new-datom)))
        (transact-report report new-datom)))))

(defn- transact-retract-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (transact-report report (datom (.-e d) (.-a d) (.-v d) tx false))))

(defn- retract-components [db datoms]
  (into #{} (comp
              (filter (fn [^Datom d] (component? db (.-a d))))
              (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

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

(def builtin-fn?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity})

(defn transact-tx-data [initial-report initial-es]
  (when-not (or (nil? initial-es)
                (sequential? initial-es))
    (raise "Bad transaction data " initial-es ", expected sequential collection"
           {:error :transact/syntax, :tx-data initial-es}))
  (loop [report initial-report
         es initial-es]
    (let [[entity & entities] es
          db (:db-after report)
          {:keys [tempids]} report]
      (cond
        (empty? es)
        (-> report
            (assoc-in [:tempids :db/current-tx] (current-tx report))
            (update-in [:db-after :max-tx] inc))

        (nil? entity)
        (recur report entities)

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
              (recur (allocate-eid report old-eid upserted-eid)
                     (concat (explode db (assoc entity :db/id upserted-eid)) entities)))

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
                  _ (validate-attr a entity)
                  ov (if (ref? db a) (entid-strict db ov) ov)
                  nv (if (ref? db a) (entid-strict db nv) nv)
                  _ (validate-val nv entity)
                  datoms (-search db [e a])]
              (if (multival? db a)
                (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
                  (recur (transact-add report [:db/add e a nv]) entities)
                  (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                         {:error :transact/cas, :old datoms, :expected ov, :new nv}))
                (let [v (:v (first datoms))]
                  (if (= v ov)
                    (recur (transact-add report [:db/add e a nv]) entities)
                    (raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                           {:error :transact/cas, :old (first datoms), :expected ov, :new nv})))))

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

            (= op :db/add)
            (recur (transact-add report entity) entities)

            (= op :db/retract)
            (if-some [e (entid db e)]
              (let [v (if (ref? db a) (entid-strict db v) v)]
                (validate-attr a entity)
                (validate-val v entity)
                (if-some [old-datom (first (-search db [e a v]))]
                  (recur (transact-retract-datom report old-datom) entities)
                  (recur report entities)))
              (recur report entities))

            (= op :db.fn/retractAttribute)
            (if-let [e (entid db e)]
              (let [_ (validate-attr a entity)
                    datoms (-search db [e a])]
                (recur (reduce transact-retract-datom report datoms)
                       (concat (retract-components db datoms) entities)))
              (recur report entities))

            (or (= op :db.fn/retractEntity)
                (= op :db/retractEntity))
            (if-let [e (entid db e)]
              (let [e-datoms (-search db [e])
                    v-datoms (mapcat (fn [a] (-search db [nil a e])) (-attrs-by db :db.type/ref))]
                (recur (reduce transact-retract-datom report (concat e-datoms v-datoms))
                       (concat (retract-components db e-datoms) entities)))
              (recur report entities))

            :else
            (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)" {:error :transact/syntax, :operation op, :tx-data entity})))

        (dd/datom? entity)
        (let [[e a v tx added] entity]
          (if added
            (recur (transact-add report [:db/add e a v tx]) entities)
            (recur report (cons [:db/retract e a v] entities))))

        :else
        (raise "Bad entity type at " entity ", expected map or vector"
               {:error :transact/syntax, :tx-data entity})))))

(defn db->uuid [conn]
  (let [root (-> (<?? (k/get-in (:store conn) [:db]))
                 (update :eavt-key kons/node->value)
                 (update :avet-key kons/node->value)
                 (update :aevt-key kons/node->value))]
    (uuid root)))
