(ns datahike.oracle
  "A NAIVE REFERENCE EVALUATOR (\"oracle\") for datahike Datalog queries.

   Purpose: differential testing between the planner and the relational base
   engine has a systematic blind spot — when both share a wrong assumption they
   agree, and the test passes. This namespace is a THIRD implementation, written
   to be obviously correct by reading rather than fast:

     * no indexes    — every pattern scans the full datom sequence;
     * no planner    — clauses are taken in written order (with one deferral
                       loop so that a function clause whose inputs are not yet
                       bound simply waits);
     * no fusion, no caching, no specialization, no fast paths;
     * one uniform notion of a partial answer: an ENVIRONMENT, a map from
       query variable to value. Everything is `unify`.

   The one non-negotiable rule, and the source of its oracle value:

       BINDING A VARIABLE THAT IS ALREADY BOUND IS UNIFICATION, NOT ASSIGNMENT.

   Both engines violate this in at least one place (see `bind` and the notes in
   plan-oracle.md). Because there is exactly ONE binding function here, the rule
   cannot be violated in one clause type and honoured in another.

   Deliberately UNSUPPORTED (throws ex-info {:oracle/unsupported ...} so a
   differential harness can skip rather than mis-report):
     pull, :keys/:syms/:strs return-maps, :order-by/:limit/:offset,
     :attribute-refs? true databases, nested `q` calls, and any clause form
     not listed in `eval-clause`."
  (:require
   [datahike.array :refer [a=]]
   [datahike.api :as d]
   [datahike.db.interface :as dbi]
   [datahike.db.utils :as dbu]
   [datahike.query :as dq]))

;; ---------------------------------------------------------------------------
;; Syntax predicates

(defn- qvar? [x] (and (symbol? x) (= \? (first (name x)))))
(defn- src-var? [x] (and (symbol? x) (= \$ (first (name x)))))
(defn- rules-var? [x] (= '% x))
(defn- blank? [x] (= '_ x))

(defn- unsupported! [what form]
  (throw (ex-info (str "oracle does not support " what)
                  {:oracle/unsupported what :form form})))

(defn- fail! [msg data]
  (throw (ex-info msg (assoc data :oracle/error true))))

;; ---------------------------------------------------------------------------
;; Query parsing (deliberately independent of datahike.parser — a shared parser
;; would be a shared assumption)

(def ^:private query-sections #{:find :with :in :where :keys :syms :strs})

(defn- parse-query
  "[:find ... :in ... :where ...] or the equivalent map -> a map of sections."
  [q]
  (let [m (if (map? q)
            q
            (loop [acc {} section nil items q]
              (if-let [x (first items)]
                (if (query-sections x)
                  (recur (assoc acc x []) x (rest items))
                  (recur (update acc section (fnil conj []) x) section (rest items)))
                acc)))]
    (when (some m [:keys :syms :strs])
      (unsupported! ":keys/:syms/:strs return maps" q))
    m))

(defn- parse-find
  "-> {:kind :rel|:coll|:scalar|:tuple, :elements [...]}
   An element is a symbol, or an aggregate call (agg ?v) / (agg n ?v)."
  [find]
  (let [check (fn [els]
                (doseq [e els]
                  (cond
                    (symbol? e) nil
                    (and (seq? e) (= 'pull (first e))) (unsupported! "pull" e)
                    (seq? e) nil
                    :else (unsupported! (str "find element " (pr-str e)) e)))
                (vec els))]
    (cond
      (and (= 2 (count find)) (= '. (second find)))
      {:kind :scalar :elements (check [(first find)])}

      (and (= 1 (count find)) (vector? (first find)))
      (let [inner (first find)]
        (if (and (= 2 (count inner)) (= '... (second inner)))
          {:kind :coll :elements (check [(first inner)])}
          {:kind :tuple :elements (check inner)}))

      :else
      {:kind :rel :elements (check find)})))

(defn- aggregate-element? [e] (seq? e))

;; ---------------------------------------------------------------------------
;; Datom access. The oracle's ONLY view of a database is the flat sequence of
;; its datoms, in whatever order the index hands them over. Nothing downstream
;; may depend on that order.

(defn- all-datoms [db]
  (when (:attribute-refs? (dbi/-config db))
    (unsupported! ":attribute-refs? true database" nil))
  (mapv (fn [d] [(:e d) (:a d) (:v d)]) (d/datoms db :eavt)))

;; ---------------------------------------------------------------------------
;; Environments and unification. THE core of the oracle: three lines that every
;; clause type goes through, so no clause type can have its own binding rule.

(def ^:private no-match ::no-match)

(defn- bind
  "Unify pattern position `x` against value `v` in environment `env`.
   Returns the extended env, or nil if unification fails.
     _        matches anything, binds nothing
     ?var     binds if free, CHECKS FOR EQUALITY if already bound
     constant matches iff equal"
  [env x v]
  (cond
    (blank? x)  env
    ;; `a=`, not `=`: a byte/float/double array is a VALUE in datahike (the
    ;; index comparator says so), and Clojure's `=` compares them by identity.
    ;; The reference engine has to hold the same value semantics as the thing
    ;; it is a reference FOR, or every array case reports a divergence that is
    ;; the oracle's own.
    (qvar? x)   (if-some [[_ bound] (find env x)]
                  (when (a= bound v) env)
                  (assoc env x v))
    :else       (when (a= x v) env)))

(defn- bind-form
  "Unify a :in / function-output BINDING FORM against a value.
   Returns a seq of envs (a collection binding fans out).
     ?x        scalar        [?a ?b]  tuple
     [?x ...]  collection    [[?a ?b]] relation"
  [env form value]
  (cond
    (or (blank? form) (qvar? form))
    (when-let [e (bind env form value)] [e])

    (and (vector? form) (= 2 (count form)) (= '... (second form)))
    (let [inner (first form)]
      (when-not (or (nil? value) (seqable? value))
        (fail! "Cannot bind non-collection to a collection binding"
               {:form form :value value}))
      (mapcat #(bind-form env inner %) value))

    (and (vector? form) (= 1 (count form)) (vector? (first form)))
    (let [inner (first form)]
      (mapcat #(bind-form env inner %) value))

    (vector? form)
    (when (and (seqable? value) (>= (count value) (count form)))
      (reduce (fn [envs [f v]] (mapcat #(bind-form % f v) envs))
              [env]
              (map vector form value)))

    :else (unsupported! (str "binding form " (pr-str form)) form)))

;; ---------------------------------------------------------------------------
;; Constants in pattern positions

(defn- lookup-ref? [x] (and (vector? x) (= 2 (count x)) (keyword? (first x))))

(defn- resolve-e
  "Entity-position constant -> eid, or `no-match` if it names no entity."
  [db x]
  (cond
    (or (qvar? x) (blank? x)) x
    (number? x) x
    (or (keyword? x) (lookup-ref? x)) (or (dbu/entid db x) no-match)
    :else (fail! "Bad entity-position constant" {:value x})))

(defn- resolve-v
  "Value-position constant for a REF attribute may itself be a lookup ref or an
   ident keyword; for every other attribute it is the value itself."
  [db a x]
  (if (and (keyword? a) (dbu/ref? db a) (not (qvar? x)) (not (blank? x))
           (or (keyword? x) (lookup-ref? x)))
    (or (dbu/entid db x) no-match)
    x))

(defn- resolve-in-lookup-refs
  "An :in binding may deliver a LOOKUP REF where an entity id is meant. Rather
   than teach every clause type to resolve one, the oracle resolves it ONCE, at
   binding time, and remembers the original so that a projection of that
   variable reports the lookup ref back — the convention both engines use.
   Returns [value' {var {eid original}}]."
  [db form value]
  (cond
    (and (qvar? form) (lookup-ref? value))
    (let [eid (dbu/entid db value)]
      (if eid [eid {form {eid value}}] [value {}]))

    (and (vector? form) (= 2 (count form)) (= '... (second form)))
    (let [pairs (map #(resolve-in-lookup-refs db (first form) %) value)]
      [(mapv first pairs) (apply merge-with merge {} (map second pairs))])

    (and (vector? form) (= 1 (count form)) (vector? (first form)))
    (let [pairs (map #(resolve-in-lookup-refs db (first form) %) value)]
      [(mapv first pairs) (apply merge-with merge {} (map second pairs))])

    (and (vector? form) (seqable? value))
    (let [pairs (map #(resolve-in-lookup-refs db %1 %2) form value)]
      [(mapv first pairs) (apply merge-with merge {} (map second pairs))])

    :else [value {}]))

;; ---------------------------------------------------------------------------
;; Function / predicate resolution.
;;
;; SCOPE DECISION: the oracle re-implements every construct that carries QUERY
;; semantics (get-else, get-some, missing?, ground, binding, negation, rules,
;; find specs, aggregates). For ordinary value functions (arithmetic, string
;; ops, comparison) it reuses datahike's built-in table: those are plain
;; Clojure functions with no query semantics, and re-deriving e.g. `<`'s
;; cross-type ordering would produce mismatches that are about a library
;; convention rather than about the engine.

(def ^:private oracle-fns
  {'get-else ::get-else 'get-some ::get-some 'missing? ::missing?
   'ground ::ground})

(defn- resolve-fn [f]
  (or (get oracle-fns f)
      (get dq/built-ins f)
      (get dq/clj-core-built-ins f)
      (when (namespace f) (some-> (requiring-resolve f) deref))
      (fail! "Unknown function" {:fn f})))

(defn- arg-value
  "Resolve one argument of a function/predicate clause in `env`."
  [ctx env a]
  (cond
    (src-var? a)              (or (get (:sources ctx) a)
                                  (fail! "Unknown source" {:src a}))
    (qvar? a)                 (if-some [[_ v] (find env a)]
                                v
                                (fail! "Unbound variable in function args" {:var a}))
    (and (seq? a) (= 'quote (first a))) (second a)
    :else                     a))

(defn- args-bound? [ctx env args]
  (every? (fn [a] (or (not (qvar? a)) (contains? env a))) args))

;; --- the db-touching built-ins, re-implemented ---

(defn- attr-values
  "Every value of (e, a) in the db, in index order. The oracle never assumes
   an attribute is card-one."
  [datoms e a]
  (into [] (comp (filter (fn [[de da]] (and (= de e) (= da a))))
                 (map (fn [[_ _ v]] v)))
        datoms))

(defn- oracle-get-else [ctx db-sym e a default]
  (when (nil? default)
    (fail! "get-else: nil default value is not supported" {}))
  (let [db (get (:sources ctx) db-sym)
        e' (resolve-e db e)
        vs (if (= no-match e') [] (attr-values (get (:datoms-of ctx) db-sym) e' a))]
    (cond
      (empty? vs) default
      (= 1 (count vs)) (first vs)
      ;; OPEN QUESTION Q2: datahike returns one arbitrary value here; Datomic
      ;; refuses the query. The oracle refuses, so that a card-many get-else
      ;; can never silently produce an order-dependent answer.
      :else (unsupported! "get-else on a card-many attribute" [e a]))))

(defn- oracle-get-some [ctx db-sym e as]
  (let [db (get (:sources ctx) db-sym)
        e' (resolve-e db e)]
    (when-not (= no-match e')
      (some (fn [a]
              (let [vs (attr-values (get (:datoms-of ctx) db-sym) e' a)]
                (when (seq vs)
                  (when (> (count vs) 1)
                    (unsupported! "get-some on a card-many attribute" [e a]))
                  [a (first vs)])))
            as))))

(defn- oracle-missing? [ctx db-sym e a]
  (let [db (get (:sources ctx) db-sym)
        e' (resolve-e db e)]
    (or (= no-match e')
        (empty? (attr-values (get (:datoms-of ctx) db-sym) e' a)))))

(defn- apply-oracle-fn [ctx tag raw-args values]
  (case tag
    ::get-else  (oracle-get-else ctx (first raw-args)
                                 (nth values 1) (nth values 2) (nth values 3))
    ::get-some  (oracle-get-some ctx (first raw-args) (nth values 1) (drop 2 values))
    ::missing?  (oracle-missing? ctx (first raw-args) (nth values 1) (nth values 2))
    ::ground    (first values)))

(defn- call-fn
  "Apply a function/predicate clause head `(f a1 a2 ...)` in `env`."
  [ctx env [f & args]]
  (let [resolved (resolve-fn f)
        values (mapv #(arg-value ctx env %) args)]
    (if (keyword? resolved)
      (apply-oracle-fn ctx resolved (vec args) values)
      (apply resolved values))))

;; ---------------------------------------------------------------------------
;; Clause evaluation. Every eval-* takes a SEQ OF ENVS and returns a SEQ OF ENVS.

(declare eval-clauses eval-clause)

(defn- eval-pattern
  "[?e :attr ?v] — the nested loop. For each env, for each datom, unify."
  [ctx envs clause]
  (let [[src clause] (if (src-var? (first clause))
                       [(first clause) (vec (rest clause))]
                       ['$ (vec clause)])
        db (or (get (:sources ctx) src) (fail! "Unknown source" {:src src}))
        datoms (get (:datoms-of ctx) src)
        pat (into clause (repeat (- 3 (count clause)) '_))
        [e a v] pat
        e (resolve-e db e)
        v (resolve-v db a v)]
    (if (or (= no-match e) (= no-match v))
      []
      (for [env envs
            [de da dv] datoms
            :let [env (bind env e de)
                  env (when env (bind env a da))
                  env (when env (bind env v dv))]
            :when env]
        env))))

(defn- eval-predicate [ctx envs clause]
  (filter (fn [env]
            (try (boolean (call-fn ctx env (first clause)))
                 ;; mirrors the base engine: a predicate that cannot be applied
                 ;; to these values filters the row out rather than raising
                 (catch ClassCastException _ false)
                 (catch IllegalArgumentException _ false)))
          envs))

(defn- eval-function [ctx envs clause]
  (let [[head out] clause]
    (mapcat (fn [env]
              (let [v (call-fn ctx env head)]
                ;; a nil result drops the row (matches both engines)
                (when-not (nil? v)
                  (bind-form env out v))))
            envs)))

(defn- branch-clauses [b]
  (if (and (seq? b) (= 'and (first b))) (vec (rest b)) [b]))

(defn- free-vars [form]
  (into #{} (filter qvar?) (tree-seq coll? seq form)))

(defn- check-or-branch-vars!
  "A plain `or` requires every branch to bind the SAME variables — Datomic and
   DataScript both reject otherwise, and datahike raises
   \"Join variable not declared inside clauses\". Without the rule the union of
   the branches has no well-defined arity."
  [branches]
  (let [vs (map (comp free-vars vec branch-clauses) branches)]
    (when (apply not= vs)
      (fail! "Join variable not declared inside all or-branches"
             {:vars (vec vs)}))))

(defn- eval-not [ctx envs clause join-vars sub]
  (filter (fn [env]
            (empty? (eval-clauses ctx [(if join-vars (select-keys env join-vars) env)] sub)))
          envs))

(defn- eval-or
  "or / or-join. `join-vars` nil means plain `or`: the branch sees the whole
   env and every variable it binds survives. For or-join only the declared
   variables cross the boundary — in EITHER direction."
  [ctx envs branches join-vars]
  (distinct
   (mapcat (fn [env]
             (mapcat (fn [b]
                       (let [inner (if join-vars (select-keys env join-vars) env)]
                         (for [e2 (eval-clauses ctx [inner] (branch-clauses b))]
                           (if join-vars
                             (merge env (select-keys e2 join-vars))
                             e2))))
                     branches))
           envs)))

;; --- rules: bottom-up naive fixpoint -------------------------------------
;; The obvious implementation, and the one that TERMINATES where SLD-style
;; top-down resolution does not: compute each rule predicate's full extension
;; by iterating the rule bodies until nothing new appears, then answer a rule
;; call by unifying its arguments against that extension.

(defn- head-tuple [env args]
  (mapv (fn [a] (if (qvar? a) (get env a) a)) args))

(defn- rule-extensions [ctx rules]
  (loop [ext (zipmap (keys rules) (repeat #{}))
         guard 0]
    (when (> guard 100)
      (fail! "rule fixpoint did not converge in 100 rounds" {:rules (keys rules)}))
    (let [ctx' (assoc ctx :rule-ext ext)
          ext' (reduce
                (fn [acc [rname defs]]
                  (reduce (fn [acc rule]
                            (let [[head & body] rule
                                  args (vec (rest head))]
                              (update acc rname into
                                      (map #(head-tuple % args)
                                           (eval-clauses ctx' [{}] body)))))
                          acc defs))
                ext rules)]
      (if (= ext ext') ext (recur ext' (inc guard))))))

(defn- eval-rule-call [ctx envs clause]
  (let [[rname & args] clause
        ext (get (:rule-ext ctx) rname)
        _ (when (nil? ext) (fail! "Unknown rule" {:rule rname}))
        args (vec args)]
    (for [env envs
          tuple ext
          :let [env' (reduce (fn [e i]
                               (if e (bind e (nth args i) (nth tuple i)) (reduced nil)))
                             env (range (count args)))]
          :when env']
      env')))

;; --- dispatch ------------------------------------------------------------

(defn- rule-call? [ctx clause]
  (and (seq? clause) (contains? (:rule-ext ctx) (first clause))))

(defn- eval-clause [ctx envs clause]
  (cond
    (seq? clause)
    (case (first clause)
      not       (eval-not ctx envs clause nil (vec (rest clause)))
      not-join  (eval-not ctx envs clause (vec (second clause)) (vec (drop 2 clause)))
      or        (do (check-or-branch-vars! (rest clause))
                    (eval-or ctx envs (rest clause) nil))
      or-join   (let [jv (second clause)]
                  (when (some vector? jv)
                    (unsupported! "or-join required/optional variable syntax" clause))
                  (eval-or ctx envs (drop 2 clause) (vec jv)))
      and       (eval-clauses ctx envs (vec (rest clause)))
      (if (rule-call? ctx clause)
        (eval-rule-call ctx envs clause)
        (unsupported! (str "clause form " (pr-str clause)) clause)))

    (and (vector? clause) (seq? (first clause)))
    (if (= 1 (count clause))
      (eval-predicate ctx envs clause)
      (eval-function ctx envs clause))

    (vector? clause)
    (eval-pattern ctx envs clause)

    :else (unsupported! (str "clause form " (pr-str clause)) clause)))

(defn- ready?
  "SAFETY, the classical Datalog side condition: a clause that can only FILTER
   must not run before the variables it filters on are bound. This is the ONE
   concession to clause order — the oracle defers such a clause instead of
   demanding that the user write a good order.

     function / predicate   all variable arguments bound
     (not …)                at least one variable of the negation bound
     (not-join [vs] …)      every declared variable bound

   Everything else BINDS variables and is always ready. The thresholds match
   the base engine (`all-bound?` / `some-bound?` in datahike.query)."
  [_ctx envs clause]
  (let [env (first envs)
        bound? #(contains? env %)]
    (or (empty? envs)
        (cond
          (and (vector? clause) (seq? (first clause)))
          (every? (fn [a] (or (not (qvar? a)) (bound? a))) (rest (first clause)))

          (and (seq? clause) (= 'not (first clause)))
          (some bound? (free-vars (vec (rest clause))))

          (and (seq? clause) (= 'not-join (first clause)))
          (every? bound? (second clause))

          :else true))))

(defn- eval-clauses [ctx envs clauses]
  (loop [envs envs todo (vec clauses)]
    (if (empty? todo)
      envs
      (let [i (first (keep-indexed (fn [i c] (when (ready? ctx envs c) i)) todo))]
        (when (nil? i)
          (fail! "Cannot resolve any more clauses" {:clauses todo}))
        (recur (eval-clause ctx envs (nth todo i))
               (into (subvec todo 0 i) (subvec todo (inc i))))))))

;; ---------------------------------------------------------------------------
;; :in bindings

(defn- bind-in [ctx envs binding value]
  (cond
    (src-var? binding)   [(update ctx :sources assoc binding value) envs]
    (rules-var? binding) [(update ctx :rules merge (dq/parse-rules value)) envs]
    :else
    (let [db (get (:sources ctx) '$)
          [value revmap] (if db
                           (resolve-in-lookup-refs db binding value)
                           [value {}])]
      ;; OPEN QUESTION Q4: which source resolves a lookup-ref :in value when
      ;; the query has several? Both engines raise if ANY source lacks the
      ;; unique attribute; the oracle refuses to guess.
      (when (and (seq revmap) (> (count (:sources ctx)) 1))
        (unsupported! "lookup-ref :in binding in a multi-source query" binding))
      [(update ctx :lookup-ref-orig (fnil merge {}) revmap)
       (mapcat #(bind-form % binding value) envs)])))

;; ---------------------------------------------------------------------------
;; Aggregation. The contract is datahike's documented one (see the comment above
;; `built-in-aggregates` in datahike.query), re-derived here rather than reused:
;;   avg/variance/stddev -> double; variance/stddev POPULATION; median double.

(defn- agg-median [coll]
  (let [t (vec (sort coll)) n (count t) m (bit-shift-right n 1)]
    (if (even? n)
      (double (/ (+ (nth t (dec m)) (nth t m)) 2))
      (let [x (nth t m)] (if (number? x) (double x) x)))))

(defn- agg-variance [coll]
  (let [mean (double (/ (reduce + 0 coll) (count coll)))]
    (double (/ (reduce + 0 (map #(let [d (- % mean)] (* d d)) coll)) (count coll)))))

(defn- min-n [n coll] (vec (take n (sort compare coll))))
(defn- max-n [n coll] (vec (take n (sort #(compare %2 %1) coll))))

(def ^:private aggregates
  {'count          (fn [coll] (count coll))
   'count-distinct (fn [coll] (count (distinct coll)))
   'sum            (fn [coll] (reduce + 0 coll))
   'min            (fn ([coll] (reduce (fn [a x] (if (neg? (compare x a)) x a)) coll))
                     ([n coll] (min-n n coll)))
   'max            (fn ([coll] (reduce (fn [a x] (if (pos? (compare x a)) x a)) coll))
                     ([n coll] (max-n n coll)))
   'avg            (fn [coll] (double (/ (reduce + 0 coll) (count coll))))
   'median         agg-median
   'variance       agg-variance
   'stddev         (fn [coll] (Math/sqrt (agg-variance coll)))
   'distinct       (fn [coll] (set coll))})

(defn- agg-spec
  "(min 2 ?s) -> {:fn <f> :extra [2] :var ?s}"
  [e]
  (let [[f & args] e
        af (or (get aggregates f) (unsupported! (str "aggregate " f) e))]
    {:fn af :extra (vec (butlast args)) :var (last args)}))

(defn- apply-aggregates [elements rows]
  (let [agg-idx (into #{} (keep-indexed (fn [i e] (when (aggregate-element? e) i))) elements)
        group-idx (remove agg-idx (range (count elements)))
        specs (mapv (fn [e] (when (aggregate-element? e) (agg-spec e))) elements)]
    (->> (group-by (fn [row] (mapv #(nth row %) group-idx)) rows)
         (mapv (fn [[_ group]]
                 (mapv (fn [i]
                         (if-let [{:keys [fn extra var]} (nth specs i)]
                           (let [col (mapv #(nth % i) group)]
                             (apply fn (concat extra [col])))
                           (nth (first group) i)))
                       (range (count elements))))))))

;; ---------------------------------------------------------------------------
;; Entry point

(defn- element-var [e] (if (aggregate-element? e) (:var (agg-spec e)) e))

(defn q
  "Naive reference evaluation of `query`. Same calling convention as `d/q`
   (vector or map query form + positional inputs)."
  [query & inputs]
  (let [[query inputs] (if (map? query)
                         [(:query query) (or (:args query) inputs)]
                         [query inputs])
        {:keys [find with in where]} (parse-query query)
        find-spec (parse-find find)
        in (or in ['$])
        _ (when (not= (count in) (count inputs))
            (fail! ":in arity does not match the number of inputs"
                   {:in in :n (count inputs)}))
        [ctx envs] (reduce (fn [[ctx envs] [b v]] (bind-in ctx envs b v))
                           [{:sources {} :rules {}} [{}]]
                           (map vector in inputs))
        _ (doseq [[s db] (:sources ctx)]
            (when-not (dbu/db? db)
              (unsupported! "non-database source (collection source)" s)))
        ctx (assoc ctx :datoms-of
                   (into {} (map (fn [[s db]] [s (all-datoms db)])) (:sources ctx)))
        ctx (assoc ctx :rule-ext (if (seq (:rules ctx))
                                   (rule-extensions ctx (:rules ctx))
                                   {}))
        envs (eval-clauses ctx envs (vec where))
        proj (into (mapv element-var (:elements find-spec)) (or with []))
        _ (doseq [v proj] (when-not (qvar? v) (unsupported! (str "projection of " v) v)))
        ;; :with widens the dedup key; a variable already in :find is already
        ;; part of it, so the combination is meaningless rather than ambiguous.
        _ (when-let [dup (not-empty (filter (set (mapv element-var (:elements find-spec)))
                                            (or with [])))]
            (fail! ":find and :with should not use same variables" {:vars (vec dup)}))
        rows (mapv (fn [env]
                     (mapv (fn [v]
                             (if (contains? env v)
                               ;; a variable bound from a lookup ref reports the
                               ;; lookup ref back, not the entity id
                               (let [x (get env v)]
                                 (get-in (:lookup-ref-orig ctx) [v x] x))
                               (fail! "Variable in :find/:with is not bound by any clause"
                                      {:var v})))
                           proj))
                   envs)
        ;; the answer is a SET of tuples: duplicates are not answers.
        ;; :with widens the dedup key, and is then projected away — which is
        ;; exactly why a :with query may legitimately return DUPLICATE rows.
        rows (vec (distinct rows))
        n (count (:elements find-spec))
        rows (mapv #(vec (take n %)) rows)
        has-aggs? (boolean (some aggregate-element? (:elements find-spec)))
        rows (cond
               has-aggs?  (apply-aggregates (:elements find-spec) rows)
               (seq with) rows
               :else      (vec (distinct rows)))]
    (case (:kind find-spec)
      :rel    (if (or has-aggs? (seq with)) (vec rows) (set rows))
      :coll   (mapv first rows)
      :scalar (ffirst rows)
      :tuple  (first rows))))
