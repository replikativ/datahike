(ns datahike.ivm
  "EXPERIMENTAL incremental view maintenance (IVM).

   Maintains standing query results across transactions instead of
   re-running the query. For a conjunctive query Q = C1 ∧ ... ∧ Ck the
   change between db-before and db-after telescopes into k per-clause
   delta queries

     ΔQ_i = C1(after) ∧ ... ∧ C_{i-1}(after) ∧ δC_i ∧ C_{i+1}(before) ∧ ... ∧ Ck(before)

   where δC_i is the transaction's datom batch restricted to clause i,
   carrying Z-set weights (+1 add / -1 retract). Each ΔQ_i is an ordinary
   datahike query over the two snapshot values plus the consolidated delta
   bound as an input relation — so maintenance reuses the whole query
   engine unchanged, including the async+sync duality on cljs.

   Multiplicity discipline: delta queries :find ALL clause variables
   (wildcards rewritten to fresh vars), so each result row is exactly one
   valuation. The per-view state is the multiplicity map
   {find-tuple -> valuation-count}; set-semantics changes are emitted only
   on zero crossings, which makes the folded state equal to
   (q query db-after) at every commit — the invariant the tests assert.

   Queries outside the supported class (aggregates, pull, or/not/rules,
   db-reading functions such as get-else/missing?, non-scalar inputs,
   explicit sources, :attribute-refs? stores) degrade to re-evaluation +
   set diff: same change stream, no incrementality.

   Known prototype limits: purge is invisible in tx-data (views over
   purged data go stale until resubscribed); watermark mismatches from
   concurrent transact callers self-heal by re-initializing from db-after."
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [datalog.parser.impl :as dpi]
            [datahike.api :as api]
            [is.simm.partial-cps.async :as pca :refer [async+sync]]
            #?(:cljs [clojure.core.async :as async
                      :refer [<! put! chan promise-chan close!]
                      :refer-macros [go-loop]])))

;; ---------------------------------------------------------------------------
;; query analysis

(defn- qvar? [x] (and (symbol? x) (str/starts-with? (name x) "?")))
(defn- src-sym? [x] (and (symbol? x) (str/starts-with? (name x) "$")))

(defn- normalize-query [query]
  (if (map? query) query (dpi/query->map query)))

(defn- fresh-var [i pos] (symbol (str "?__b" i "-" pos)))

(defn- normalize-pattern
  "Pad a data pattern to at least [e a v], replacing _ with fresh vars.
   Returns {:pattern [...] :vars [...]} or nil if outside the delta class
   (5-element patterns, non-numeric ground entity, non-keyword ground attr)."
  [clause i]
  (let [p (vec clause)
        p (into [] (map-indexed (fn [pos x] (if (= '_ x) (fresh-var i pos) x))) p)
        p (loop [p p] (if (< (count p) 3) (recur (conj p (fresh-var i (count p)))) p))
        [e a _v] p]
    (when (and (<= (count p) 4)
               (or (qvar? e) (number? e))
               (or (qvar? a) (keyword? a)))
      {:pattern p :vars (into [] (comp (filter qvar?) (distinct)) p)})))

(defn- classify-clause
  "Classify one :where clause for the delta strategy. Returns
   {:type :pattern|:predicate|:function|:complex ...}."
  [clause i]
  (cond
    (seq? clause) {:type :complex :clause clause}          ; or/not/rule invocations
    (and (vector? clause) (seq? (first clause)))
    (let [[[f & fargs] & binding] clause]
      (if (or (some src-sym? (cons f fargs))               ; db-reading fns: get-else, missing?, nested q ...
              (and (seq binding)
                   (not (and (= 1 (count binding)) (qvar? (first binding))))))
        {:type :complex :clause clause}
        {:type (if (seq binding) :function :predicate)
         :clause clause
         :binding-var (first binding)}))
    (and (vector? clause) (src-sym? (first clause))) {:type :complex :clause clause}
    (vector? clause)
    (if-let [{:keys [pattern vars]} (normalize-pattern clause i)]
      {:type :pattern :clause clause :pattern pattern :vars vars}
      {:type :complex :clause clause})
    :else {:type :complex :clause clause}))

(defn- attr-deps
  "Attributes that can affect the view; :all when not statically derivable."
  [classified]
  (reduce (fn [deps {:keys [type pattern]}]
            (if (reduced? deps)
              deps
              (case type
                :pattern (let [a (nth pattern 1)]
                           (if (keyword? a) (conj deps a) (reduced :all)))
                (:predicate :function) deps
                (reduced :all))))
          #{} classified))

(defn- clause-matcher
  "Builds (fn [datom] -> binding vector over vars, or nil) for a normalized
   pattern; enforces ground positions and repeated-variable consistency."
  [pattern vars]
  (let [ps (cond-> [[:e (nth pattern 0)] [:a (nth pattern 1)] [:v (nth pattern 2)]]
             (> (count pattern) 3) (conj [:tx (nth pattern 3)]))]
    (fn [d]
      (loop [ps (seq ps) bind {}]
        (if (nil? ps)
          (mapv bind vars)
          (let [[k p] (first ps)
                dv (case k :e (:e d) :a (:a d) :v (:v d) :tx (:tx d))]
            (cond
              (qvar? p) (if-let [ex (find bind p)]
                          (when (= (val ex) dv) (recur (next ps) bind))
                          (recur (next ps) (assoc bind p dv)))
              (= p dv) (recur (next ps) bind)
              :else nil)))))))

(defn- delta-query
  "The i-th telescoped delta query: clauses before i on $__a (db-after),
   after i on $__b (db-before), clause i replaced by the delta input
   relation. nil :query means the delta rows are already the valuations."
  [classified i all-vars extra-ins]
  (let [{:keys [vars]} (nth classified i)
        where (into []
                    (keep-indexed
                     (fn [j {:keys [type pattern clause]}]
                       (when (not= i j)
                         (if (= :pattern type)
                           (into [(if (< j i) '$__a '$__b)] pattern)
                           clause))))
                    classified)]
    {:matcher (clause-matcher (:pattern (nth classified i)) vars)
     :query (when (seq where)
              {:find (conj all-vars '?__w)
               :in (into ['$__a '$__b (vector (conj vars '?__w))] extra-ins)
               :where where})}))

(defn parse-view
  "Analyze a standing query into a view description: strategy, attribute
   routing, and (for the delta class) the valuation query and per-clause
   delta queries."
  [query args]
  (let [qm (normalize-query query)
        find-spec (vec (:find qm))
        in-spec (or (seq (:in qm)) '[$])
        classified (into [] (map-indexed (fn [i c] (classify-clause c i))) (:where qm))
        deps (attr-deps classified)
        extra-ins (vec (rest in-spec))
        all-vars (into []
                       (comp (mapcat (fn [{:keys [type vars binding-var]}]
                                       (case type
                                         :pattern vars
                                         :function [binding-var]
                                         nil)))
                             (distinct))
                       classified)
        find-idx (mapv (fn [v] (first (keep-indexed (fn [i av] (when (= av v) i)) all-vars)))
                       find-spec)
        delta? (and (every? qvar? find-spec)
                    (every? some? find-idx)
                    (nil? (:with qm)) (nil? (:keys qm)) (nil? (:syms qm)) (nil? (:strs qm))
                    (= '$ (first in-spec))
                    (every? qvar? extra-ins)
                    (every? #(not= :complex (:type %)) classified)
                    (seq classified))
        base {:query qm :extra-args (vec args) :attr-deps deps}]
    (if-not delta?
      (assoc base :strategy :reeval)
      (assoc base
             :strategy :delta
             :all-vars all-vars
             :find-idx find-idx
             :init-query {:find all-vars
                          :in (vec in-spec)
                          :where (mapv (fn [{:keys [type pattern clause]}]
                                         (if (= :pattern type) pattern clause))
                                       classified)}
             :delta-queries (into []
                                  (keep (fn [i]
                                          (when (= :pattern (:type (nth classified i)))
                                            (delta-query classified i all-vars extra-ins))))
                                  (range (count classified)))))))

;; ---------------------------------------------------------------------------
;; maintenance

(defn- q-step [qm sync?]
  (async+sync sync?
              (pca/await (api/q (assoc qm :sync? sync?)))))

(defn- as-set
  "Subscriptions operate at set-of-tuples level; aggregate/collection find
   shapes come back from q as vectors (or scalars) and are normalized here."
  [res]
  (cond (set? res) res
        (nil? res) #{}
        (coll? res) (set res)
        :else #{res}))

(def ^:private probe-q
  '[:find ?v :in $ ?e ?a ?v :where [?e ?a ?v]])
(def ^:private values-q
  '[:find ?v :in $ ?e ?a :where [?e ?a ?v]])

(defn- effective-delta-step
  "Normalize tx-data into the true Z-set delta of the current index by
   probing both snapshots. This recovers what the report alone does not
   say: card-one upserts drop the old datom from the index WITHOUT a
   retraction in tx-data, and re-asserted or within-tx-superseded adds
   appear in tx-data without changing the index at all. Returns
   [[datom-like w] ...] where datom-like supports :e/:a/:v/:tx."
  [view db-before db-after tx-data sync?]
  (async+sync sync?
              (let [deps (:attr-deps view)
                    ds (into [] (filter (fn [d] (or (= :all deps) (contains? deps (:a d))))) tx-data)
                    n (count ds)
                    ;; retractions the report already carries (e.g. card-one
                    ;; upsert displacement) must not be synthesized again
                    reported (into #{}
                                   (comp (remove :added)
                                         (map (fn [d] [(:e d) (:a d) (:v d)])))
                                   ds)]
                (loop [i 0 out [] synthed reported]
                  (if (>= i n)
                    out
                    (let [d (nth ds i)
                          e (:e d) a (:a d) v (:v d)]
                      (if (:added d)
                        (let [in-after? (seq (pca/await (q-step {:query probe-q :args [db-after e a v]
                                                                 :disable-planner? true}
                                                                sync?)))
                              in-before? (when in-after?
                                           (seq (pca/await (q-step {:query probe-q :args [db-before e a v]
                                                                    :disable-planner? true}
                                                                   sync?))))]
                          (cond
                            (not in-after?) (recur (inc i) out synthed)   ; superseded within the tx
                            in-before? (recur (inc i) out synthed)        ; no-op re-assert
                            :else
                            ;; real add; synthesize retractions for old values the
                            ;; upsert displaced (present before, gone after)
                            (let [olds (pca/await (q-step {:query values-q :args [db-before e a]
                                                           :disable-planner? true}
                                                          sync?))
                                  olds (into [] (comp (map first) (remove #(contains? synthed [e a %]))) olds)
                                  m (count olds)
                                  [out' synthed']
                                  (loop [j 0 out (conj out [d 1]) synthed synthed]
                                    (if (>= j m)
                                      [out synthed]
                                      (let [vold (nth olds j)
                                            still? (seq (pca/await (q-step {:query probe-q
                                                                            :args [db-after e a vold]
                                                                            :disable-planner? true}
                                                                           sync?)))]
                                        (if still?
                                          (recur (inc j) out synthed)
                                          (recur (inc j)
                                                 (conj out [{:e e :a a :v vold :tx (:tx d)} -1])
                                                 (conj synthed [e a vold]))))))]
                              (recur (inc i) out' synthed'))))
                        ;; retraction: count it only if it actually left the index
                        (let [in-before? (seq (pca/await (q-step {:query probe-q :args [db-before e a v]
                                                                  :disable-planner? true}
                                                                 sync?)))
                              in-after? (when in-before?
                                          (seq (pca/await (q-step {:query probe-q :args [db-after e a v]
                                                                   :disable-planner? true}
                                                                  sync?))))]
                          (if (and in-before? (not in-after?))
                            (recur (inc i) (conj out [d -1]) synthed)
                            (recur (inc i) out synthed))))))))))

(defn- consolidate
  "Match effective-delta entries against one clause and sum weights per
   binding. Returns [[binding... w] ...] with non-zero weights only."
  [matcher wdatoms]
  (let [m (reduce (fn [acc [d w]]
                    (if-let [b (matcher d)]
                      (assoc acc b (+ w (get acc b 0)))
                      acc))
                  {} wdatoms)]
    (into [] (keep (fn [[b w]] (when-not (zero? w) (conj b w)))) m)))

(defn- fold-valuations
  "Fold weighted valuation rows into the multiplicity map. Returns
   {:weights m' :changes [[find-tuple ±1] ...]} — changes only on zero
   crossings — or {:resync? true} if a count would go negative."
  [find-idx weights rows]
  (let [deltas (reduce (fn [m row]
                         (let [row (vec row)
                               w (peek row)
                               t (mapv row find-idx)]
                           (assoc m t (+ w (get m t 0)))))
                       {} rows)]
    (reduce-kv (fn [{:keys [weights changes] :as acc} t dw]
                 (if (:resync? acc)
                   acc
                   (let [old (get weights t 0)
                         nw (+ old dw)]
                     (cond
                       (neg? nw) {:resync? true}
                       (zero? nw) {:weights (dissoc weights t)
                                   :changes (if (pos? old) (conj changes [t -1]) changes)}
                       :else {:weights (assoc weights t nw)
                              :changes (if (zero? old) (conj changes [t 1]) changes)}))))
               {:weights weights :changes []} deltas)))

(defn- init-step
  "Compute the full multiplicity map for the view on db."
  [view db sync?]
  (async+sync sync?
              (if (= :delta (:strategy view))
                (let [vals (pca/await (q-step {:query (:init-query view)
                                               :args (into [db] (:extra-args view))}
                                              sync?))
                      find-idx (:find-idx view)]
                  (reduce (fn [m row]
                            (let [t (mapv (vec row) find-idx)]
                              (assoc m t (inc (get m t 0)))))
                          {} vals))
                (let [res (as-set (pca/await (q-step {:query (:query view)
                                                      :args (into [db] (:extra-args view))}
                                                     sync?)))]
                  (reduce (fn [m t] (assoc m t 1)) {} res)))))

(defn- maintain-step
  "Apply one tx-report to the multiplicity map. Returns
   {:weights m' :changes [...]} or {:resync? true}."
  [view weights tx-report sync?]
  (async+sync sync?
              (if (= :reeval (:strategy view))
                (let [res (as-set (pca/await (q-step {:query (:query view)
                                                      :args (into [(:db-after tx-report)] (:extra-args view))}
                                                     sync?)))
                      old (set (keys weights))
                      changes (-> []
                                  (into (map (fn [t] [t 1])) (set/difference res old))
                                  (into (map (fn [t] [t -1])) (set/difference old res)))]
                  {:weights (into {} (map (fn [t] [t 1])) res)
                   :changes changes})
                (let [{:keys [db-before db-after tx-data]} tx-report
                      delta (pca/await (effective-delta-step view db-before db-after tx-data sync?))
                      dqs (:delta-queries view)
                      n (count dqs)
                      rows (loop [i 0 acc []]
                             (if (< i n)
                               (let [{:keys [matcher query]} (nth dqs i)
                                     rel (consolidate matcher delta)]
                                 (cond
                                   (empty? rel) (recur (inc i) acc)
                                   (nil? query) (recur (inc i) (into acc rel))
                                   :else
                                   ;; :disable-planner? routes the multi-source delta
                                   ;; query through the relational engine — the planner's
                                   ;; cartesian split does not see connectivity through
                                   ;; the input relation (NPE at query.cljc:4049)
                                   (let [res (pca/await (q-step {:query query
                                                                 :args (into [db-after db-before rel]
                                                                             (:extra-args view))
                                                                 :disable-planner? true}
                                                                sync?))]
                                     (recur (inc i) (into acc res)))))
                               acc))]
                  (fold-valuations (:find-idx view) weights rows)))))

;; ---------------------------------------------------------------------------
;; subscriptions

(defn- relevant? [view tx-data]
  (let [deps (:attr-deps view)]
    (or (= :all deps)
        (boolean (some (fn [d] (contains? deps (:a d))) tx-data)))))

(defn- resync-step
  "Re-initialize from db-after, emitting a :reset snapshot."
  [sub db-after sync?]
  (async+sync sync?
              (let [{:keys [view weights-atom state-atom callback]} sub
                    weights (pca/await (init-step view db-after sync?))]
                (reset! weights-atom weights)
                (swap! state-atom assoc :last-tx (:max-tx db-after))
                (callback {:reset true
                           :tx (:max-tx db-after)
                           :db-after db-after
                           :result (set (keys weights))})
                nil)))

(defn- process-report-step
  "Watermarked, self-healing application of one tx-report to a subscription.
   Must be called serially per subscription."
  [sub tx-report sync?]
  (async+sync sync?
              (let [{:keys [view weights-atom state-atom callback]} sub
                    {:keys [db-before db-after tx-data]} tx-report
                    last-tx (:last-tx @state-atom)]
                (cond
                  (and last-tx (<= (:max-tx db-after) last-tx))
                  nil                                             ; stale or duplicate delivery

                  (and last-tx (not= (:max-tx db-before) last-tx))
                  (pca/await (resync-step sub db-after sync?))    ; gap: heal from the snapshot

                  (not (relevant? view tx-data))
                  (swap! state-atom assoc :last-tx (:max-tx db-after))

                  :else
                  (let [{:keys [weights changes resync?]}
                        (pca/await (maintain-step view @weights-atom tx-report sync?))]
                    (if resync?
                      (pca/await (resync-step sub db-after sync?))
                      (do (reset! weights-atom weights)
                          (swap! state-atom assoc :last-tx (:max-tx db-after))
                          (when (seq changes)
                            (callback {:tx (:max-tx db-after)
                                       :db-after db-after
                                       :changes changes}))
                          nil)))))))

(defn subscribe!
  "Subscribe to a standing query on conn. Returns a subscription handle.

   opts: {:query    datalog query (vector or map form; :in must start with $)
          :args     extra :in arguments after the db (scalars for the delta class)
          :callback (fn [msg]) — receives, in commit order:
                      {:reset true :tx t :db-after db :result #{tuple ...}}   on (re)initialization
                      {:tx t :db-after db :changes [[tuple 1] [tuple -1] ...]} on set-level changes
          :sync?    cljs only: false runs maintenance through the async engine}

   The folded state (see `result`) equals (q query db) at every delivered
   :tx. Changes are emitted only when the result set changes."
  [conn {:keys [query args callback sync?] :or {args [] sync? true}}]
  #?(:clj (assert sync? "async subscriptions are cljs-only"))
  (let [view (parse-view query args)
        sub {:key (keyword "datahike.ivm" (str (gensym "sub")))
             :conn conn
             :view view
             :weights-atom (atom {})
             :state-atom (atom {:last-tx nil})
             :callback callback
             :sync? sync?
             :lock #?(:clj (Object.) :cljs nil)
             :ch #?(:clj nil :cljs (when-not sync? (chan 1024)))}]
    #?(:clj
       (do (api/listen conn (:key sub)
                       (fn [tx-report]
                         (locking (:lock sub)
                           (try
                             (process-report-step sub tx-report true)
                             (catch Exception _e
                               ;; self-heal: any maintenance failure falls back to a
                               ;; full re-initialization from the committed snapshot
                               (try
                                 (resync-step sub (:db-after tx-report) true)
                                 (catch Exception e2
                                   (callback {:error e2}))))))))
           (locking (:lock sub)
             (resync-step sub @conn true)))
       :cljs
       (if sync?
         (do (api/listen conn (:key sub)
                         (fn [tx-report]
                           (try
                             (process-report-step sub tx-report true)
                             (catch js/Error _e
                               (try
                                 (resync-step sub (:db-after tx-report) true)
                                 (catch js/Error e2
                                   (callback {:error e2})))))))
             (resync-step sub @conn true))
         (let [ch (:ch sub)]
           ;; the go-loop serializes init and maintenance in arrival order;
           ;; maintenance errors self-heal by re-initializing from db-after
           (go-loop []
             (when-let [job (<! ch)]
               (let [p (promise-chan)
                     step (if (= :init (first job))
                            (resync-step sub (second job) false)
                            (process-report-step sub (second job) false))]
                 (step (fn [v] (put! p [:ok v]))
                       (fn [e] (put! p [:err e])))
                 (let [[k e] (<! p)]
                   (when (= :err k)
                     (if (= :init (first job))
                       (callback {:error e})
                       (let [p2 (promise-chan)]
                         ((resync-step sub (:db-after (second job)) false)
                          (fn [v] (put! p2 [:ok v]))
                          (fn [e2] (put! p2 [:err e2])))
                         (let [[k2 e2] (<! p2)]
                           (when (= :err k2)
                             (callback {:error e2}))))))))
               (recur)))
           (api/listen conn (:key sub)
                       (fn [tx-report] (put! ch [:report tx-report])))
           (put! ch [:init @conn]))))
    sub))

(defn unsubscribe!
  "Remove the subscription's listener and stop maintenance."
  [sub]
  (api/unlisten (:conn sub) (:key sub))
  #?(:cljs (some-> (:ch sub) close!))
  nil)

(defn result
  "Current folded result set of the subscription — equals (q query db) at
   the subscription's last delivered :tx."
  [sub]
  (set (keys @(:weights-atom sub))))

(defn strategy
  "How this subscription is maintained: :delta (incremental) or :reeval
   (re-run + diff fallback)."
  [sub]
  (get-in sub [:view :strategy]))
