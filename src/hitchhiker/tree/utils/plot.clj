(ns hitchhiker.tree.utils.plot
  "This namespace provides functions to help visualizing hh-trees.

  It provides a visualization similar to those in https://youtu.be/jdn617M3-P4?t=1583
  "
  (:require
   [konserve.memory :refer [new-mem-store]]
   [hitchhiker.tree.bootstrap.konserve :as kons]
   [konserve.cache :as kc]
   [hitchhiker.tree :as tree]
   [hitchhiker.tree.node :as n]
   [hitchhiker.tree.utils.async :as ha]
   [hitchhiker.tree.messaging :as msg]
   [clojure.core.async  :as async]
   [clojure.string :as str]
   [loom.io :refer [view] :as lio]
   [loom.graph :refer [graph] :as lg]
   [loom.derived :as deriv]
   [loom.attr :as attr]
   [cheshire.core :as json]))

;; patch loom for compass rendering
(ns loom.io)

(defn dot-str
  "Renders graph g as a DOT-format string. Calls (node-label node) and
  (edge-label n1 n2) to determine what labels to use for nodes and edges,
  if any. Weights become edge labels unless a label is specified.
  Labels also include attributes when the graph satisfies AttrGraph."
  [g & {:keys [graph-name node-label edge-label]
        :or {graph-name "graph"} :as opts }]
  (let [d? (directed? g)
        w? (weighted? g)
        a? (attr? g)
        node-label (or node-label
                       (if a?
                         #(attr g % :label)
                         (constantly nil)))
        edge-label (or edge-label
                       (cond
                         a? #(if-let [a (attr g %1 %2 :label)]
                               a
                               (if w? (weight g %1 %2)))
                         w? #(weight g %1 %2)
                         :else (constantly nil)))
        sb (doto (StringBuilder.
                  (if d? "digraph \"" "graph \""))
             (.append (dot-esc graph-name))
             (.append "\" {\n"))]
    (doseq [k [:graph :node :edge]]
      (when (k opts)
        (doto sb
          (.append (str "  " (name k) " "))
          (.append (dot-attrs (k opts))))))
    (doseq [[n1 n2] (distinct-edges g)]
      (let [n1l (str (or (node-label n1) n1))
            n2l (str (or (node-label n2) n2))
            el (edge-label n1 n2)
            eattrs (assoc (if a?
                            (attrs g n1 n2) {})
                          :label el)]
        (doto sb
          (.append "  \"")
          (.append (dot-esc n1l))
          (.append
           (if d? (str "\"" (when (:compass eattrs)
                              (str ":" (:compass eattrs)))
                       " -> \"")
               "\" -- \""))
          (.append (dot-esc n2l))
          (.append \"))
        (let [eattrs (dissoc eattrs :compass)]
          (when (or (:label eattrs) (< 1 (count eattrs)))
            (.append sb \space)
            (.append sb (dot-attrs eattrs))))
        (.append sb "\n")))
    (doseq [n (nodes g)]
      (doto sb
        (.append "  \"")
        (.append (dot-esc (str (or (node-label n) n))))
        (.append \"))
      (when-let [nattrs (when a?
                          (dot-attrs (attrs g n)))]
        (.append sb \space)
        (.append sb nattrs))
      (.append sb "\n"))
    (str (doto sb (.append "}")))))


(ns hitchhiker.tree.utils.plot)

(def store
  (kons/add-hitchhiker-tree-handlers
   (kc/ensure-cache (async/<!! (new-mem-store)))))

;; put a tree in the store including merkle hashes
(def flushed
  (ha/<?? (tree/flush-tree
           (time (reduce (fn [t i]
                           (ha/<?? (msg/insert t i i)))
                         (ha/<?? (tree/b-tree (tree/->Config 3 3 2)))
                         (shuffle (range 1 30))))
           (kons/->KonserveBackend store))))


(def flushed
  (ha/<?? (tree/flush-tree
           (time (reduce (fn [t i]
                           (ha/<?? (msg/insert t i i)))
                         (:tree flushed)
                         (shuffle (range -4 -2))))
           (kons/->KonserveBackend store))))



(comment
  ;; TODO double root node?
  (do
    (def store (kons/add-hitchhiker-tree-handlers
                (kc/ensure-cache (ha/<?? (new-mem-store)))) )


    ;; insertion
    (def flushed (ha/<?? (tree/flush-tree
                          (time (reduce (fn [t i]
                                          (ha/<?? (msg/insert t i i)))
                                        (ha/<?? (tree/b-tree (tree/->Config 2 2 2)))
                                        (concat (range 1 12)
                                                #_[0 13 14 -1 15])))
                          (kons/->KonserveBackend store))))


    (def flushed (ha/<?? (tree/flush-tree
                          (time (reduce (fn [t i]
                                          (ha/<?? (msg/insert t i i)))
                                        (:tree flushed)
                                        (range 12 14)))
                          (kons/->KonserveBackend store))))

    (view (create-graph store))))

(defn init-graph [store]
  (apply lg/digraph
         (->> @(:state store)
              (filter (fn [[id {:keys [children]}]] children))
              (map (fn [[id {:keys [children] :as node}]]
                     (if (:op-buf node)
                       {id (mapv (fn [c] (:konserve-key c)) children)}
                       {id []}))))))


(defn use-record-nodes [g]
  (attr/add-attr-to-nodes g :shape "record" (lg/nodes g)))


(defn node-layout [g [id {:keys [children] :as node}]]
  (if (tree/index-node? node)
    (attr/add-attr
     g id
     :label (str
             ;; key space separators
             (str/join " | "
                       (map #(str "<" % "> " ;; compass point (invisible)
                                  "\\<" % "\\>")
                            (map n/-last-key (:children node))))
             ;; render op-log
             " | {"
             (str/join " | " (map (fn [{:keys [key value]}]
                                    key)
                                  (:op-buf node)))
             "}"))
    (attr/add-attr
     g id
     :label (str (str/join "|" (map key (:children node)))))))


(defn set-node-layouts [g store]
  (->> @(:state store)
       (filter (fn [[id {:keys [children]}]] children))
       (reduce node-layout g)))

(defn edge-hash [id]
  (-> id str (subs 0 4)))

(defn set-edge-layouts [g store]
  (let [node-map @(:state store)
        edges (lg/edges g)]
    (reduce
     (fn [g [n1 n2]]
       (let [h (:konserve-key (async/<!!
                               (:storage-addr (into {} (node-map n2)))))]
         (-> g
             (attr/add-attr-to-edges :compass (n/-last-key (node-map n2))
                                     [[n1 n2]])
             (attr/add-attr-to-edges :label (edge-hash h) [[n1 n2]]))))
     g
     edges)))


(defn create-graph [store]
  (-> (init-graph store)
      use-record-nodes
      (set-node-layouts store)
      (set-edge-layouts store)))



(comment
  (view (create-graph store))

  (println (lio/dot-str g)))



(defn remove-storage-addrs [[k v]]
  (if (tree/index-node? v)
    [k (-> v
           (dissoc :storage-addr :cfg)
           (update :children (fn [cs] (mapv #(dissoc % :storage-addr :store) cs)))
           (update :op-buf (fn [cs] (mapv #(into {} %) cs)))
           )]
    [k (dissoc v :storage-addr :cfg)]))


(comment
  (spit "/tmp/sample-tree.json"
        (json/generate-string
         (into {} (map remove-storage-addrs @(:state store)))))


  (prn (map remove-storage-addrs @(:state store))))
