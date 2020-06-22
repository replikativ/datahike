(ns hitchhiker.tree
  "Default tree implementation

  The parts of the serialization system that seem like they're need hooks are:

  * Must provide a function that takes a node, serializes it, and returns an addr

  * Must be able to rollback writing an addr

  * Whatever the addr it returns, it should cache its resolve in-mem
  somehow

  * The serialize a node & rollback a node functions should accept a
  'stats' object as well

  * The 'stats' object must be convertible to a summary or whatever at the end"
  (:refer-clojure :exclude [subvec])
  (:require
   [hitchhiker.tree.utils.async :as ha :include-macros true]
   [hitchhiker.tree.node :as n]
   [hitchhiker.tree.backend :as b]
   [hitchhiker.tree.key-compare :as c]
   [clojure.core.rrb-vector :refer [catvec subvec]]
   #?(:clj [clojure.core.async :as async]
      :cljs [cljs.core.async :as async :include-macros true])))

(defrecord Config [index-b data-b op-buf-size])

(defrecord Split [left right median])

(declare data-node
         index-node
         flush-tree
         flush-tree-without-root

         index-node?
         data-node?
         resolved?)

(defn index-node-keys
  "Calculates the separating keys given the children of an index node"
  [children]
  (into []
        (map n/-last-key)
        (pop children)))

(defmacro <?-resolve
  [n]
  `(let [n# ~n]
     (if (resolved? n#)
       n#
       (ha/<? (n/-resolve-chan n#)))))

(defn <-cache
  [cache calc-fn]
  (let [c @cache]
    (if (#?(:clj identical?
            :cljs keyword-identical?)
         c ::nothing)
      (vreset! cache (calc-fn))
      c)))

(defn cache
  []
  (volatile! ::nothing))

(defrecord IndexNode [children
                      storage-addr
                      op-buf
                      cfg
                      *last-key-cache]
  n/IIndexNode
  n/IResolved

  n/IAddress
  (-dirty? [this]
    (not (async/poll! storage-addr)))

  (-dirty! [this]
    (assoc this
           :storage-addr (async/promise-chan)
           :*last-key-cache (cache)))

  n/INode
  (-last-key [this]
    (<-cache *last-key-cache
             #(n/-last-key (peek children))))

  (-overflow? [this]
    (>= (count children)
        (* 2 (:index-b cfg))))

  (-underflow? [this]
    (< (count children)
       (:index-b cfg)))

  (-split-node [this]
    (let [b (:index-b cfg)
          median (some-> (nth children (dec b)) n/-last-key)
          ;; TODO this should use msg/affects-key
          op-bufs (sort-by :key c/-compare op-buf)]
      (loop [op-bufs op-bufs
             left-buf (transient [])
             right-buf (transient [])]
        (if-let [op-buf (first op-bufs)]
          ;; check if we are still on  left side
          (if (not (pos? (c/-compare (:key op-buf) median)))
            (recur (next op-bufs)
                   (conj! left-buf op-buf)
                   right-buf)
            ;; otherwise just copy the rest on the right
            (recur nil
                   left-buf
                   (reduce conj! right-buf op-bufs)))
          (->Split (index-node (subvec children 0 b)
                               (persistent! left-buf)
                               cfg)
                   (index-node (subvec children b)
                               (persistent! right-buf)
                               cfg)
                   median)))))
  (-merge-node [this other]
    (index-node (catvec children (:children other))
                (catvec op-buf (:op-buf other))
                cfg))

  (-lookup [this k]
    ;; This is written like so because it's performance critical
    (let [l (dec (count children))
          a (object-array l)
          _ (dotimes [i l]
              (aset a i (n/-last-key (nth children i))))
          x #?(:clj (java.util.Arrays/binarySearch a 0 l k c/-compare)
               :cljs (goog.array/binarySearch a k c/-compare))]
      (if (neg? x)
        (- (inc x))
        x))))

(defn index-node
  [children op-buf cfg]
  (->IndexNode children
               (async/promise-chan)
               op-buf
               cfg
               (cache)))

(defn nth-of-set
  "Like nth, but for sorted sets. O(n) in worst case, 0(1) when idx out
  of bounds."
  [set index]
  ;; we can escape early for free since sorted-sets are ICounted
  (when (> (count set) index)
    (loop [i 0
           set set]
      (if (< i index)
        (recur (unchecked-inc i)
               (next set))
        (first set)))))

(def empty-sorted-map-by-compare (sorted-map-by c/-compare))

(defrecord DataNode [children storage-addr cfg *last-key-cache]

  n/IDataNode
  n/IResolved

  n/IAddress
  (-dirty? [this] (not (async/poll! storage-addr)))

  (-dirty! [this]
    (assoc this
           :storage-addr (async/promise-chan)
           :*last-key-cache (cache)))

  n/INode
  (-last-key [this]
    (<-cache *last-key-cache
             #(when (seq children)
                (-> children
                    (rseq)
                    (first)
                    (key)))))

  ;; Should have between b & 2b-1 children
  (-overflow? [this]
    (>= (count children) (* 2 (:data-b cfg))))

  (-underflow? [this]
    (< (count children) (:data-b cfg)))

  (-split-node [this]
    (let [data-b (:data-b cfg)]
      (loop [children children
             i 0
             left empty-sorted-map-by-compare
             right empty-sorted-map-by-compare]
        (if-let [child (first children)]
          (if (< i data-b)
            (recur (next children)
                   (inc i)
                   (conj left child)
                   right)
            (recur nil
                   (inc i)
                   left
                   (reduce conj right children)))
          (->Split (data-node left cfg)
                   (data-node right cfg)
                   (nth-of-set children (dec data-b)))))))

  (-merge-node [this other]
    (data-node (into children (:children other))
               cfg))

  (-lookup [root k]
    (let [x #?(:clj (java.util.Collections/binarySearch (vec (keys children))
                                                        k
                                                        c/-compare)
               :cljs (goog.array/binarySearch (into-array (keys children))
                                              k
                                              c/-compare))]
      (if (neg? x)
        (- (inc x))
        x))))

(defn data-node
  "Creates a new data node"
  [children cfg]
  (->DataNode children
              (async/promise-chan)
              cfg
              (cache)))

(defn data-node?
  [node]
  (instance? DataNode node))

(defn index-node?
  [node]
  (instance? IndexNode node))

(defn resolved?
  [node]
  (or (index-node? node)
      (data-node? node)))

(defn backtrack-up-path-until
  "Given a path (starting with root and ending with an index), searches
  backwards, passing each pair of parent & index we just came from to
  the predicate function.  When that function returns true, we return
  the path ending in the index for which it was true, or else we
  return the empty path"
  [path pred]
  (loop [path path]
    (when (pos? (count path))
      (let [from-index (peek path)
            tmp (pop path)
            parent (peek tmp)]
        (if (pred parent from-index)
          path
          (recur (pop tmp)))))))


(defn right-successor
  "Given a node on a path, find's that node's right successor node"
  [path]
  ;; FIXME this function would benefit from a prefetching hint to keep
  ;; the next several sibs in mem
  (ha/go-try
   (when-let [common-parent-path
              (backtrack-up-path-until path
                                       (fn [parent index]
                                         (< (inc index)
                                            (count (:children parent)))))]
     (let [next-index (-> common-parent-path peek inc)
           parent (-> common-parent-path pop peek)
           new-sibling (-> (nth (:children parent)
                                next-index)
                           <?-resolve)
           sibling-lineage (loop [res (transient [new-sibling])
                                  s new-sibling]
                             (let [first-child (-> s :children first)]
                               (if (n/address? first-child)
                                 (let [resolved-first-child (<?-resolve first-child)]
                                   (when (n/address? resolved-first-child)
                                     (recur (conj! res resolved-first-child)
                                            resolved-first-child)))
                                 (persistent! res))))
           path-suffix (-> (interleave sibling-lineage
                                       (repeat 0))
                           ;; butlast ensures we end w/ node
                           (butlast))]
       (-> (pop common-parent-path)
           (conj next-index)
           (into path-suffix))))))

(defn lookup-path
  "Given a B-tree and a key, gets a path into the tree"
  [tree key]
  (ha/go-try
   (loop [;; alternating node/index/node/index/node... of the search taken
          path (transient [tree])
          ;; current search node
          cur tree]
     (let [children (:children cur)]
       (when (> (count children) 0)
         (if (data-node? cur)
           (persistent! path)
           (let [index (n/-lookup cur key)
                 child (when-not (data-node? cur)
                         (-> children
                             ;; TODO what are the semantics for
                             ;; exceeding on the right? currently
                             ;; it's trunc to the last element
                             (nth index (peek children))
                             (<?-resolve)))]
             (recur (-> path
                        (conj! index)
                        (conj! child))
                    child))))))))

(defn lookup-key
  "Given a B-tree and a key, gets an iterator into the tree"
  ([tree key]
   (lookup-key tree key nil))
  ([tree key not-found]
   (ha/go-try
    (-> (ha/<? (lookup-path tree key))
        (peek)
        (<?-resolve)
        :children
        (get key not-found)))))

(defn insert
  [{:keys [cfg] :as tree} k v]
  (ha/go-try
   (let [path (ha/<? (lookup-path tree k))
         {:keys [children] :or {children empty-sorted-map-by-compare}} (peek path)
         updated-data-node (data-node (assoc children k v)
                                      cfg)]
     (loop [node updated-data-node
            path (pop path)]
       (if (empty? path)
         (if (n/-overflow? node)
           (let [{:keys [left right median]} (n/-split-node node)]
             (index-node [left right]
                         []
                         cfg))
           node)
         (let [index (peek path)
               init-path (pop path)
               {:keys [children keys] :as parent} (peek init-path)]
           ;; splice the split into the parent
           (if (n/-overflow? node)
             ;; TODO refactor paths to be node/index pairs or 2 vectors or something
             (let [{:keys [left right median]} (n/-split-node node)
                   new-children (catvec (conj (subvec children 0 index)
                                              left right)
                                        (subvec children (inc index)))]
               (recur (-> parent
                          (assoc :children new-children)
                          (n/-dirty!))
                      (pop init-path)))
             (recur (-> parent
                        ;;TODO this assoc seems to be a bottleneck
                        (assoc :children (assoc children index node))
                        (n/-dirty!))
                    (pop init-path)))))))))

;;TODO: cool optimization: when merging children, push as many operations as you can
;;into them to opportunistically minimize overall IO costs

(defn delete
  [{:keys [cfg] :as tree} key]
  (ha/go-try
   (let [path (ha/<? (lookup-path tree key)) ;; don't care about the found key or its index
         {:keys [children]
          :or {children empty-sorted-map-by-compare}} (peek path)
         updated-data-node (data-node (dissoc children key)
                                      cfg)]
     (loop [node updated-data-node
            path (pop path)]
       (if (empty? path)
         ;; Check for special root underflow case
         (if (and (index-node? node)
                  (= 1 (count (:children node))))
           (first (:children node))
           node)
         (let [index (peek path)
               init-path (pop path)
               {:keys [children keys op-buf] :as parent} (peek init-path)]
           (if (n/-underflow? node) ;; splice the split into the parent
             ;;TODO this needs to use a polymorphic sibling-count
             ;;to work on serialized nodes
             (let [bigger-sibling-idx
                   (cond
                     (= (dec (count children)) index) (dec index) ; only have left sib
                     (zero? index) 1  ;only have right sib
                     (> (count (:children (nth children (dec index))))
                        (count (:children (nth children (inc index)))))
                     (dec index)      ; right sib bigger
                     :else (inc index))
                   node-first? (> bigger-sibling-idx index)
                   ;; if true, `node` is left
                   merged (if node-first?
                            (n/-merge-node node (<?-resolve (nth children bigger-sibling-idx)))
                            (n/-merge-node (<?-resolve (nth children bigger-sibling-idx)) node))
                   old-left-children (subvec children 0 (min index bigger-sibling-idx))
                   old-right-children (subvec children (inc (max index bigger-sibling-idx)))]
               (if (n/-overflow? merged)
                 (let [{:keys [left right median]} (n/-split-node merged)]
                   (recur (index-node (catvec (conj old-left-children left right)
                                              old-right-children)
                                      op-buf
                                      cfg)
                          (pop init-path)))
                 (recur (index-node (catvec (conj old-left-children merged)

                                            old-right-children)
                                    op-buf
                                    cfg)
                        (pop init-path))))
             (recur (index-node (assoc children index node)
                                op-buf
                                cfg)
                    (pop init-path)))))))))

(defn b-tree
  [cfg & kvs]
  (ha/go-try
   (loop [[[k v] & r] (partition 2 kvs)
          t (data-node empty-sorted-map-by-compare
                       cfg)]
     (if k
       (recur r (ha/<? (insert t k v)))
       t))))

;;TODO make this a loop/recur instead of mutual recursion
(defn flush-children
  [children backend session]
  (ha/go-try
   (loop [[c & r] children
          res (transient [])]
     (if-not c
       (persistent! res)
       (recur r (conj! res (ha/<? (flush-tree c backend session))))))))

(defn flush-tree
  "Given the tree, finds all dirty nodes, delivering addrs into them.
   Every dirty node also gets replaced with its TestingAddr.
   These form a GC cycle, have fun with the unmanaged memory port :)"
  ([tree backend]
   (ha/go-try
    (let [session (b/-new-session backend)
          flushed (ha/<? (flush-tree tree backend session))
          root (b/-anchor-root backend flushed)]
      {:tree (<?-resolve root) ;; root should never be unresolved for API
       :stats session})))
  ([tree backend stats]
   (ha/go-try
    (if (n/-dirty? tree)
      (let [cleaned-children (if (data-node? tree)
                               (:children tree)
                               (->> (flush-children (:children tree) backend stats)
                                    ha/<?
                                    catvec))
            cleaned-node (assoc tree :children cleaned-children)
            new-addr (ha/<? (b/-write-node backend cleaned-node stats))]
        (async/>!! (:storage-addr tree)
                   new-addr)
        new-addr)
      tree))))

;; TODO merge this with the code above
(defn flush-children-without-root
  [children backend session]
  (ha/go-try
   (loop [[c & r] children
          res (transient [])]
     (if-not c
       (persistent! res)
       (recur r (conj! res (ha/<? (flush-tree-without-root c backend session false))))))))

(defn flush-tree-without-root
  "Given the tree, finds all dirty nodes, delivering addrs into them.
   Does not flush root node, but returns it."
  ([tree backend]
   (ha/go-try
    (let [session (b/-new-session backend)
          flushed (ha/<? (flush-tree-without-root tree backend session true))
          root (b/-anchor-root backend flushed)]
      {:tree (<?-resolve root) ; root should never be unresolved for API
       :stats session})))
  ([tree backend stats root-node?]
   (ha/go-try
    (if (n/-dirty? tree)
      (let [cleaned-children (if (data-node? tree)
                               (:children tree)
                               ;; TODO throw on nested errors
                               (->> (flush-children-without-root (:children tree) backend stats)
                                    ha/<?
                                    catvec))
            cleaned-node (assoc tree :children cleaned-children)]
        (if root-node?
          cleaned-node
          (let [new-addr (ha/<? (b/-write-node backend cleaned-node stats))]
            (async/>!! (:storage-addr tree)
                       new-addr)
            new-addr)))
      tree))))

(ha/if-async?
 (do
   (defn forward-iterator
     "Takes the result of a search and puts the iterated elements onto iter-ch
  going forward over the tree as needed. Does lg(n) backtracking sometimes."
     [iter-ch path start-key]
      (ha/go-try
          (loop [path path]
            (if path
              (let [start-node (peek path)
                    elements (subseq (:children start-node)
                                     >=
                                     start-key)]
                (ha/<? (async/onto-chan iter-ch
                                        elements false))
                (recur (ha/<? (right-successor (pop path)))))
              (async/close! iter-ch)))))

   #?(:clj
      (defn lookup-fwd-iter
        "Compatibility helper to clojure sequences. Please prefer the channel
  interface of forward-iterator, as this function blocks your thread, which
  disturbs async contexts and might lead to poor performance. It is mainly here
  to facilitate testing."
        [tree key]
        (let [path (ha/<?? (lookup-path tree key))
              iter-ch (async/chan)]
          (forward-iterator iter-ch path key)
          (ha/chan-seq iter-ch)))))
 ;; else
 (do
   (defn forward-iterator
     "Takes the result of a search and returns an iterator going
   forward over the tree. Does lg(n) backtracking sometimes."
     [path start-key]
     (let [start-node (peek path)]
       (assert (data-node? start-node))
       (let [first-elements (-> start-node
                                :children ; Get the indices of it
                                (subseq >= start-key)) ; skip to the start-index
             next-elements (lazy-seq
                            (when-let [succ (right-successor (pop path))]
                              (forward-iterator succ start-key)))]
         (concat first-elements next-elements))))

   (defn lookup-fwd-iter
     [tree key]
     (let [path (lookup-path tree key)]
       (when path
         (forward-iterator path key))))))
