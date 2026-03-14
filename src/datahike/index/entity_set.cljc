(ns datahike.index.entity-set
  "EntityBitSet — a set of entity IDs for cross-engine filtering.
   Backed by RoaringBitmap on JVM for fast AND/OR/NOT and compression.
   Falls back to a sorted long array on CLJS."
  #?(:clj (:import [org.roaringbitmap RoaringBitmap])))

#?(:clj
   (defn entity-bitset
     "Create an empty EntityBitSet (RoaringBitmap)."
     ^RoaringBitmap []
     (RoaringBitmap.))

   :cljs
   (defn entity-bitset
     "Create an empty EntityBitSet (sorted set on CLJS)."
     []
     (sorted-set)))

#?(:clj
   (defn entity-bitset-add!
     "Add an entity ID to the bitset. Mutates in place (JVM)."
     [^RoaringBitmap bs ^long eid]
     (.add bs (int eid))
     bs)

   :cljs
   (defn entity-bitset-add!
     [bs eid]
     (conj bs eid)))

#?(:clj
   (defn entity-bitset-contains?
     "Check if an entity ID is in the bitset."
     [^RoaringBitmap bs ^long eid]
     (.contains bs (int eid)))

   :cljs
   (defn entity-bitset-contains?
     [bs eid]
     (contains? bs eid)))

#?(:clj
   (defn entity-bitset-and
     "Intersect two bitsets. Returns a new bitset."
     ^RoaringBitmap [^RoaringBitmap a ^RoaringBitmap b]
     (RoaringBitmap/and a b))

   :cljs
   (defn entity-bitset-and
     [a b]
     (into (sorted-set) (filter b) a)))

#?(:clj
   (defn entity-bitset-or
     "Union two bitsets. Returns a new bitset."
     ^RoaringBitmap [^RoaringBitmap a ^RoaringBitmap b]
     (RoaringBitmap/or a b))

   :cljs
   (defn entity-bitset-or
     [a b]
     (into a b)))

#?(:clj
   (defn entity-bitset-andnot
     "Subtract b from a. Returns a new bitset (a AND NOT b)."
     ^RoaringBitmap [^RoaringBitmap a ^RoaringBitmap b]
     (RoaringBitmap/andNot a b))

   :cljs
   (defn entity-bitset-andnot
     [a b]
     (into (sorted-set) (remove b) a)))

#?(:clj
   (defn entity-bitset-cardinality
     "Return the number of entity IDs in the bitset."
     ^long [^RoaringBitmap bs]
     (.getLongCardinality bs))

   :cljs
   (defn entity-bitset-cardinality
     [bs]
     (count bs)))

#?(:clj
   (defn entity-bitset-from-longs
     "Create an EntityBitSet from a sequence of long entity IDs."
     ^RoaringBitmap [eids]
     (let [bs (RoaringBitmap.)]
       (doseq [^long eid eids]
         (.add bs (int eid)))
       (.runOptimize bs)
       bs))

   :cljs
   (defn entity-bitset-from-longs
     [eids]
     (into (sorted-set) eids)))

#?(:clj
   (defn entity-bitset-seq
     "Return a lazy seq of entity IDs from the bitset."
     [^RoaringBitmap bs]
     (iterator-seq (.iterator bs)))

   :cljs
   (defn entity-bitset-seq
     [bs]
     (seq bs)))
