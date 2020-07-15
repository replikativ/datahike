(ns hitchhiker.tree.messaging
  (:refer-clojure :exclude [subvec])
  (:require
   [hitchhiker.tree.utils.async :as ha]
   [hitchhiker.tree.op :as op]
   [hitchhiker.tree.node :as n]
   [hitchhiker.tree.key-compare :as c]
   [clojure.core.rrb-vector :as rrb]
   [hasch.core :as h]
   [hitchhiker.tree :as tree :include-macros true]
   #?@(:clj [[clojure.core.async :as async]]
       :cljs [[cljs.core.async :as async]])))

(defrecord InsertOp [key value]
  op/IOperation
  (-affects-key [_] key)
  (-apply-op-to-coll [_ map]
    (assoc map key value))
  (-apply-op-to-tree [_ tree]
    (tree/insert tree key value)))

(defrecord DeleteOp [key]
  op/IOperation
  (-affects-key [_] key)
  (-apply-op-to-coll [_ map]
    (dissoc map key))
  (-apply-op-to-tree [_ tree] (tree/delete tree key)))

(defn enqueue
  ([tree msgs]
   (ha/go-try
    (let [deferred-ops (atom [])]
      (loop [tree (ha/<? (enqueue tree msgs deferred-ops))
             [op & r] @deferred-ops]
        (if op
          (recur (ha/<? (op/-apply-op-to-tree op tree))
                 r)
          tree)))))

  ([tree msgs deferred-ops]
   (ha/go-try
    (let [tree (tree/<?-resolve tree)]
      (cond
        (tree/data-node? tree) ;; need to return ops to apply to the tree proper...
        (do (swap! deferred-ops into msgs)
            tree)
        (<= (+ (count msgs) (count (:op-buf tree)))
            (get-in tree [:cfg :op-buf-size])) ; will there be enough space?
        (-> tree
            (n/-dirty!)
            (update-in [:op-buf] into msgs))
        :else ;; overflow, should be IndexNode
        (do (assert (tree/index-node? tree))
            (loop [[child & children] (:children tree)
                   rebuilt-children (transient [])
                   msgs (vec (sort-by op/-affects-key ;must be a stable sort
                                      c/-compare
                                      (concat (:op-buf tree) msgs)))]
              (let [took-msgs (into []
                                    (take-while #(>= 0 (c/-compare
                                                        (op/-affects-key %)
                                                        (n/-last-key child))))
                                    msgs)
                    extra-msgs (into []
                                     (drop-while #(>= 0 (c/-compare
                                                         (op/-affects-key %)
                                                         (n/-last-key child))))
                                     msgs)
                    on-the-last-child? (empty? children)

                    ;; Any changes to the current child?
                    new-child
                    (cond
                      (and on-the-last-child? (seq extra-msgs))
                      (ha/<? (enqueue (tree/<?-resolve child)
                                      (rrb/catvec took-msgs extra-msgs)
                                      deferred-ops))
                      (seq took-msgs) ;; save a write
                      (ha/<? (enqueue (tree/<?-resolve child)
                                      (rrb/catvec took-msgs)
                                      deferred-ops))
                      :else
                      child)]

                (if on-the-last-child?
                  (-> tree
                      (assoc :children (-> rebuilt-children
                                           (conj! new-child)
                                           persistent!)
                             :op-buf [])
                      (n/-dirty!))
                  (recur children
                         (conj! rebuilt-children new-child)
                         extra-msgs))))))))))

(defn general-max [e & r]
  ;; fast track for number keys
  (if (number? e)
    (apply max e r)
    (reduce (fn [old elem]
              (if (pos? (c/-compare old elem))
                old
                elem))
            e r)))

(defn apply-ops-in-path
  [path]
  (if (>= 1 (count path))
    (:children (peek path))
    (let [ops (->> path
                   (into [] (comp (filter tree/index-node?)
                                  (map :op-buf)))
                   (rseq) ; highest node should be last in seq
                   (apply rrb/catvec)
                   (sort-by op/-affects-key c/-compare)) ;must be a stable sort
          init-path (pop path)
          this-node-index (peek init-path)
          parent (-> init-path pop peek)
          is-first? (zero? this-node-index)
          ;;We'll need to find the smallest last-key of the left siblings along the path
          [left-sibs-on-path is-last?]
          (loop [path path
                 is-last? true
                 left-sibs (transient [])]
            (if (= 1 (count path)) ; are we at the root?
              [(persistent! left-sibs) is-last?]
              (let [init-path (pop path)
                    this-node-index (peek init-path)
                    parent (-> init-path pop peek)
                    is-first? (zero? this-node-index)
                    local-last? (= (-> parent :children count dec)
                                   this-node-index)]
                (recur (pop init-path)
                       (and is-last? local-last?)
                       (if is-first?
                         left-sibs
                         (conj! left-sibs
                                (nth (:children parent)
                                     (dec this-node-index))))))))
          left-sibs-min-last (when (seq left-sibs-on-path)
                               (->> left-sibs-on-path
                                    (map n/-last-key)
                                    (apply general-max)))
          left-sib-filter (if left-sibs-min-last
                            (drop-while #(>= 0 (c/-compare (op/-affects-key %)
                                                           left-sibs-min-last)))
                            identity)
          data-node (peek path)
          my-last (n/-last-key data-node)
          right-side-filter (if is-last?
                              identity
                              (take-while #(>= 0 (c/-compare (op/-affects-key %) my-last))))
          ;; We include op if leq my left, and not if leq left's left
          ;; TODO we can't apply all ops, we should ensure to only
          ;; apply ops whose keys are in the defined range, unless
          ;; we're the last sibling
          correct-ops (into [] (comp left-sib-filter right-side-filter) ops)]
      (reduce (fn [coll op]
                (op/-apply-op-to-coll op coll))
              (:children data-node)
              correct-ops))))

(defn lookup
  ([tree key]
   (lookup tree key nil))
  ([tree key not-found]
   (ha/go-try
    (let [path (ha/<? (tree/lookup-path tree key))
          expanded (apply-ops-in-path path)]
      (get expanded key not-found)))))

(defn insert
  [tree key value]
  (enqueue tree [(assoc (->InsertOp key value)
                        :tag (h/uuid))]))

(defn upsert
  [tree upsertOp]
  (enqueue tree [(assoc upsertOp
                        :tag (h/uuid))]))

(defn delete
  [tree key]
  (enqueue tree [(assoc (->DeleteOp key)
                        :tag (h/uuid))]))


(ha/if-async?
 (do
   (defn forward-iterator
     "Takes the result of a search and puts the iterated elements onto iter-ch
  going forward over the tree as needed. Does lg(n) backtracking sometimes."
     [iter-ch path start-key]
     (ha/go-try
      (loop [path path]
        (if path
          (let [elements (drop-while (fn [[k v]]
                                       (neg? (c/-compare k start-key)))
                                     (apply-ops-in-path path))]
            (ha/<? (async/onto-chan iter-ch elements false))
            (recur (ha/<? (tree/right-successor (pop path)))))
          (async/close! iter-ch)))))

   #?(:clj
      (defn lookup-fwd-iter
        "Compatibility helper to clojure sequences. Please prefer the channel
  interface of forward-iterator, as this function blocks your thread, which
  disturbs async contexts and might lead to poor performance. It is mainly here
  to facilitate testing or for exploration on the REPL."
        [tree key]
        (let [path (ha/<?? (tree/lookup-path tree key))
              iter-ch (async/chan)]
          (forward-iterator iter-ch path key)
          (ha/chan-seq iter-ch)))))
 ;; else
 (do
  (defn forward-iterator
      "Takes the result of a search and returns an iterator going
   forward over the tree. Does lg(n) backtracking sometimes."
      [path]
      (assert (tree/data-node? (peek path)))
      (let [first-elements (apply-ops-in-path path)
            next-elements (lazy-seq
                           (when-let [succ (tree/right-successor (pop path))]
                             (forward-iterator succ)))]
        (concat first-elements next-elements)))


   (defn lookup-fwd-iter
     [tree key]
     (let [path (tree/lookup-path tree key)]
       (when path
         (drop-while (fn [[k v]]
                       (neg? (c/-compare k key)))
                     (forward-iterator path)))))))


