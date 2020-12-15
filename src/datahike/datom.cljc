(ns datahike.datom
  #?(:cljs (:require-macros [datahike.datom :refer [combine-cmp]]))
  (:require  [clojure.walk]
             [clojure.data]
             [datahike.tools :refer [combine-hashes]]
             [datahike.constants :refer [tx0]]
             #?(:cljs [goog.array :as garray])))

(declare hash-datom equiv-datom seq-datom nth-datom assoc-datom val-at-datom)

(defprotocol IDatom
  (datom-tx [this])
  (datom-added [this]))

(deftype Datom #?(:clj  [^int e a v ^long tx ^:unsynchronized-mutable ^int _hash]
                  :cljs [^number e a v ^number tx ^:mutable ^number _hash])
  IDatom
  (datom-tx [d] (if (pos? tx) tx (- tx)))
  (datom-added [d] (pos? tx))

  #?@(:cljs
      [IHash
       (-hash [d] (if (zero? _hash)
                    (set! _hash (hash-datom d))
                    _hash))
       IEquiv
       (-equiv [d o] (and (instance? Datom o) (equiv-datom d o)))

       ISeqable
       (-seq [d] (seq-datom d))

       ILookup
       (-lookup [d k] (val-at-datom d k nil))
       (-lookup [d k nf] (val-at-datom d k nf))

       IIndexed
       (-nth [this i] (nth-datom this i))
       (-nth [this i not-found] (nth-datom this i not-found))

       IAssociative
       (-assoc [d k v] (assoc-datom d k v))

       IPrintWithWriter
       (-pr-writer [d writer opts]
                   (pr-sequential-writer writer pr-writer
                                         "#datahike/Datom [" " " "]"
                                         opts [(.-e d) (.-a d) (.-v d) (datom-tx d) (datom-added d)]))]
      :clj
      [Object
       (hashCode [d]
                 (if (zero? _hash)
                   (let [h (int (hash-datom d))]
                     (set! _hash h)
                     h)
                   _hash))
       (toString [d] (pr-str d))

       clojure.lang.IHashEq
       (hasheq [d] (.hashCode d))

       clojure.lang.Seqable
       (seq [d] (seq-datom d))

       clojure.lang.IPersistentCollection
       (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
       (empty [d] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
       (count [d] 5)
       (cons [d [k v]] (assoc-datom d k v))

       clojure.lang.Indexed
       (nth [this i] (nth-datom this i))
       (nth [this i not-found] (nth-datom this i not-found))

       clojure.lang.ILookup
       (valAt [d k] (val-at-datom d k nil))
       (valAt [d k nf] (val-at-datom d k nf))

       clojure.lang.Associative
       (entryAt [d k] (some->> (val-at-datom d k nil) (clojure.lang.MapEntry k)))
       (containsKey [e k] (#{:e :a :v :tx :added} k))
       (assoc [d k v] (assoc-datom d k v))]))

#?(:cljs (goog/exportSymbol "datahike.db.Datom" Datom))

(defn ^Datom datom
  ([e a v] (Datom. e a v tx0 0))
  ([e a v tx] (Datom. e a v tx 0))
  ([e a v tx added] (Datom. e a v (if added tx (- tx)) 0)))

(defn datom? [x] (instance? Datom x))

(defn- hash-datom [^Datom d]
  (-> (hash (.-e d))
      (combine-hashes (hash (.-a d)))
      (combine-hashes (hash (.-v d)))))

(defn- equiv-datom [^Datom d ^Datom o]
  (and (== (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       (= (.-v d) (.-v o))))

(defn- seq-datom [^Datom d]
  (list (.-e d) (.-a d) (.-v d) (datom-tx d) (datom-added d)))

;; keep it fast by duplicating for both keyword and string cases
;; instead of using sets or some other matching func
(defn- val-at-datom [^Datom d k not-found]
  (case k
    :e (.-e d) "e" (.-e d)
    :a (.-a d) "a" (.-a d)
    :v (.-v d) "v" (.-v d)
    :tx (datom-tx d)
    "tx" (datom-tx d)
    :added (datom-added d)
    "added" (datom-added d)
    not-found))

(defn- nth-datom
  ([^Datom d ^long i]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     #?(:clj  (throw (IndexOutOfBoundsException.))
        :cljs (throw (js/Error. (str "Datom/-nth: Index out of bounds: " i))))))
  ([^Datom d ^long i not-found]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     not-found)))

(defn- ^Datom assoc-datom [^Datom d k v]
  (case k
    :e (datom v (.-a d) (.-v d) (datom-tx d) (datom-added d))
    :a (datom (.-e d) v (.-v d) (datom-tx d) (datom-added d))
    :v (datom (.-e d) (.-a d) v (datom-tx d) (datom-added d))
    :tx (datom (.-e d) (.-a d) (.-v d) v (datom-added d))
    :added (datom (.-e d) (.-a d) (.-v d) (datom-tx d) v)
    #?(:clj (throw (IllegalArgumentException. (str "invalid key for #datahike/Datom: " k)))
       :cljs (throw (js/Error. (str "invalid key for #datahike/Datom: " k))))))

;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

(defn ^Datom datom-from-reader [vec]
  (apply datom vec))

#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#datahike/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (datom-tx d) (datom-added d)]))))

;; ----------------------------------------------------------------------------
;; datom cmp macros/funcs
;;

#?(:clj
   (defmacro combine-cmp [& comps]
     (loop [comps (reverse comps)
            res (num 0)]
       (if (not-empty comps)
         (recur
          (next comps)
          `(let [c# ~(first comps)]
             (if (== 0 c#)
               ~res
               c#)))
         res))))

(defn cmp [o1 o2]
  (if (nil? o1) 0
      (if (nil? o2) 0
          (compare o1 o2))))

;; Slower cmp-* fns allows for datom fields to be nil.
;; Such datoms come from slice method where they are used as boundary markers.

(defn cmp-datoms-eavt [^Datom d1, ^Datom d2]
  (combine-cmp
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp (.-a d1) (.-a d2))
   (cmp (.-v d1) (.-v d2))
   (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

(defn cmp-datoms-aevt [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp (.-a d1) (.-a d2))
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp (.-v d1) (.-v d2))
   (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

(defn cmp-datoms-avet [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp (.-a d1) (.-a d2))
   (cmp (.-v d1) (.-v d2))
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

;; fast versions without nil checks

(defn- cmp-attr-quick [a1 a2]
  ;; either both are keywords or both are strings
  #?(:cljs
     (if (keyword? a1)
       (-compare a1 a2)
       (garray/defaultCompare a1 a2))
     :clj
     (.compareTo ^Comparable a1 a2)))

(defn cmp-datoms-eavt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare (.-v d1) (.-v d2))
   (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

(defn cmp-datoms-aevt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (compare (.-v d1) (.-v d2))
   (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

(defn cmp-datoms-avet-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare (.-v d1) (.-v d2))
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

(defn diff-sorted [a b cmp]
  (loop [only-a []
         only-b []
         both []
         a a
         b b]
    (cond
      (empty? a) [(not-empty only-a) (not-empty (into only-b b)) (not-empty both)]
      (empty? b) [(not-empty (into only-a a)) (not-empty only-b) (not-empty both)]
      :else
      (let [first-a (first a)
            first-b (first b)
            diff (cmp first-a first-b)]
        (cond
          (== diff 0) (recur only-a only-b (conj both first-a) (next a) (next b))
          (< diff 0) (recur (conj only-a first-a) only-b both (next a) b)
          (> diff 0) (recur only-a (conj only-b first-b) both a (next b)))))))
