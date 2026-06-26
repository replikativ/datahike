(ns benchmark.sip-bench
  "Sideways-information-passing (SIP) regression benchmark.

   Exercises the query shape where values are bound by an `:in` collection
   binding (or scalar) and then fed into a join — the GraphQL-batch shape that
   a major downstream user (jobtech-taxonomy-api) hits. The compiled planner is
   expected to DRIVE the scan from the bound values (point lookups / index seeks
   / a semi-join filter), cost O(#bound-values), rather than scan the whole
   attribute and hash-join afterward, cost O(attribute cardinality).

   Three shapes, each across a range of batch sizes (#bound ids), compiled
   engine vs the relational base engine:

     S1  standalone single pattern   [?e :node/id ?id]
     S2  entity-group (scan + merge) [?e :node/id ?id] [?e :node/data ?d]
     S3  rule + OR (fwd/rev edge)    (linked ?a ?b) over an edge relation

   Run:
     clj -M:bench-compare -m benchmark.sip-bench [n-nodes]

   Interpreting: for small/medium batches the compiled engine should be at or
   below the base engine. A compiled time that is flat across batch sizes (and
   scales with n-nodes) is the O(attribute) full-scan regression."
  (:require
   [datahike.api :as d]
   [datahike.query :as q]
   [benchmark.datascript-bench :as dsb]))

;; ---------------------------------------------------------------------------
;; Synthetic store: n nodes (unique :node/id, sparse :node/data) + n edges.

(def schema
  [{:db/ident :node/id   :db/valueType :db.type/long    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :node/data :db/valueType :db.type/long    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :edge/from :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one}
   {:db/ident :edge/to   :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one}])

(defn make-db [n]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? false
             :search-cache-size 0
             :index :datahike.index/persistent-set}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn {:tx-data schema})
      ;; nodes 1..n with :node/id; :node/data on ~10% of them (sparse merge)
      (doseq [batch (partition-all 5000 (range n))]
        (d/transact conn {:tx-data (mapv (fn [i]
                                           (cond-> {:node/id i}
                                             (zero? (mod i 10)) (assoc :node/data (* i 7))))
                                         batch)}))
      ;; ring of edges i -> (i+1)
      (doseq [batch (partition-all 5000 (range n))]
        (d/transact conn {:tx-data (mapv (fn [i] {:edge/from [:node/id i]
                                                  :edge/to   [:node/id (mod (inc i) n)]})
                                         batch)}))
      (d/db conn))))

(def edge-rules
  '[[(linked ?a ?b ?r)
     (or (and [?r :edge/from ?a] [?r :edge/to ?b])
         (and [?r :edge/to ?a]   [?r :edge/from ?b]))]])

(def q-single  '{:find [?id]     :in [$ [?id ...]] :where [[?e :node/id ?id]]})
(def q-join    '{:find [?id ?d]  :in [$ [?id ...]] :where [[?e :node/id ?id] [?e :node/data ?d]]})
(def q-rule    '{:find [?ida ?idb] :in [$ % [?ida ...]]
                 :where [[?a :node/id ?ida] (linked ?a ?b ?r) [?b :node/id ?idb]]})

(defn run-shape [label db run-fn batches]
  (println (format "%-26s %10s %10s %8s" label "compiled" "base" "ratio"))
  (doseq [k batches]
    (let [ids (vec (range k))
          compiled (binding [q/*disable-planner* false] (dsb/bench-10 (run-fn db ids)))
          base     (binding [q/*disable-planner* true]  (dsb/bench-10 (run-fn db ids)))]
      (println (format "  batch=%-6d            %10s %10s %7sx"
                       k (dsb/round compiled) (dsb/round base)
                       (dsb/round (/ compiled (max 1e-6 base))))))))

(defn -main [& args]
  (let [n (if-let [s (first args)] (Integer/parseInt s) 100000)
        batches [1 10 100 1000]]
    ;; result cache hides re-execution cost (and has a known staleness bug, #808)
    (alter-var-root #'q/*query-result-cache?* (constantly false))
    (println (format "\nSIP benchmark — %d nodes, %d edges, result-cache off\n" n n))
    (let [db (make-db n)]
      (run-shape "S1 single  [?e :node/id ?id]" db (fn [db ids] (d/q q-single db ids)) batches)
      (println)
      (run-shape "S2 join    +[?e :node/data ?d]" db (fn [db ids] (d/q q-join db ids)) batches)
      (println)
      (run-shape "S3 rule+OR (linked ?a ?b)" db (fn [db ids] (d/q q-rule db edge-rules ids)) batches))
    (println)))
