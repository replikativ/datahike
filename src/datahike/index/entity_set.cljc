(ns datahike.index.entity-set
  "EntityBitSet — a set of entity IDs for cross-engine filtering.
   Backed by Roaring64NavigableMap on JVM for fast AND/OR/NOT and compression
   across Datahike's full signed-long entity-id domain.
   Falls back to a sorted long array on CLJS."
  #?(:clj (:import [org.roaringbitmap.longlong Roaring64NavigableMap])))

#?(:clj
   (defn entity-bitset?
     "Whether x is the JVM EntityBitSet representation."
     [x]
     (instance? Roaring64NavigableMap x))

   :cljs
   (defn entity-bitset?
     "Whether x is the CLJS EntityBitSet representation."
     [x]
     (instance? cljs.core/PersistentTreeSet x)))

#?(:clj
   (defn entity-bitset
     "Create an empty EntityBitSet (RoaringBitmap)."
     ^Roaring64NavigableMap []
     (Roaring64NavigableMap.))

   :cljs
   (defn entity-bitset
     "Create an empty EntityBitSet (sorted set on CLJS)."
     []
     (sorted-set)))

#?(:clj
   (defn entity-bitset-add!
     "Add an entity ID to the bitset. Mutates in place (JVM)."
     [^Roaring64NavigableMap bs ^long eid]
     (.addLong bs eid)
     bs)

   :cljs
   (defn entity-bitset-add!
     [bs eid]
     (conj bs eid)))

#?(:clj
   (defn entity-bitset-contains?
     "Check if an entity ID is in the bitset."
     [^Roaring64NavigableMap bs ^long eid]
     (.contains bs eid))

   :cljs
   (defn entity-bitset-contains?
     [bs eid]
     (contains? bs eid)))

#?(:clj
   (defn entity-bitset-and
     "Intersect two bitsets. Returns a new bitset."
     ^Roaring64NavigableMap [^Roaring64NavigableMap a
                             ^Roaring64NavigableMap b]
     (Roaring64NavigableMap/and a b))

   :cljs
   (defn entity-bitset-and
     [a b]
     (into (sorted-set) (filter b) a)))

#?(:clj
   (defn entity-bitset-or
     "Union two bitsets. Returns a new bitset."
     ^Roaring64NavigableMap [^Roaring64NavigableMap a
                             ^Roaring64NavigableMap b]
     (Roaring64NavigableMap/or a b))

   :cljs
   (defn entity-bitset-or
     [a b]
     (into a b)))

#?(:clj
   (defn entity-bitset-andnot
     "Subtract b from a. Returns a new bitset (a AND NOT b)."
     ^Roaring64NavigableMap [^Roaring64NavigableMap a
                             ^Roaring64NavigableMap b]
     (Roaring64NavigableMap/andNot a b))

   :cljs
   (defn entity-bitset-andnot
     [a b]
     (into (sorted-set) (remove b) a)))

#?(:clj
   (defn entity-bitset-cardinality
     "Return the number of entity IDs in the bitset."
     ^long [^Roaring64NavigableMap bs]
     (.getLongCardinality bs))

   :cljs
   (defn entity-bitset-cardinality
     [bs]
     (count bs)))

#?(:clj
   (defn entity-bitset-from-longs
     "Create an EntityBitSet from a sequence of long entity IDs."
     ^Roaring64NavigableMap [eids]
     (let [bs (Roaring64NavigableMap.)]
       (doseq [^long eid eids]
         (.addLong bs eid))
       (.runOptimize bs)
       bs))

   :cljs
   (defn entity-bitset-from-longs
     [eids]
     (into (sorted-set) eids)))

#?(:clj
   (defn entity-bitset-seq
     "Return a lazy seq of entity IDs from the bitset."
     [^Roaring64NavigableMap bs]
     (iterator-seq (.iterator bs)))

   :cljs
   (defn entity-bitset-seq
     [bs]
     (seq bs)))
