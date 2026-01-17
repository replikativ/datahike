(ns datahike.codegen.pod
  "Generate Babashka pod bindings from API specification.

   This namespace provides pod-specific configuration and logic for:
   - Mapping API operations to pod functions with ID-based references
   - Managing connection and database caching
   - Generating describe-map for pod protocol
   - Handling argument resolution and result transforms"
  (:require [clojure.string :as str]
            [datahike.api.specification :refer [api-specification]]))

;; =============================================================================
;; Pod-Specific Configuration (Overlay)
;; =============================================================================

(def pod-operations
  "Pod-specific configuration extending the universal API specification.

   Keys:
   - :resolve - Map of arg positions to resolver type (:conn, :db)
   - :returns - What the function returns (:conn-id, :db-id, :value)
   - :transform - Named transform to apply to result
   - :post-action - Action after main operation (:remove-conn)
   - :custom-fn - Custom function body (quoted) for operations needing special handling"

  '{;; Database lifecycle - direct pass-through
    database-exists? {:returns :value}
    create-database  {:returns :value}
    delete-database  {:returns :value}

    ;; Connection lifecycle
    connect {:returns :conn-id}
    release {:resolve {0 :conn}
             :returns :value
             :post-action :remove-conn}

    ;; DB access
    db {:resolve {0 :conn}
        :returns :db-id}

    ;; Temporal - derive from db
    as-of   {:resolve {0 :db} :returns :db-id}
    since   {:resolve {0 :db} :returns :db-id}
    history {:resolve {0 :db} :returns :db-id}
    db-with {:resolve {0 :db} :returns :db-id}

    ;; Query - has custom implementation due to multiple calling conventions
    q {:custom-fn
       (fn [& args]
         (let [resolve-db-arg (fn [arg]
                                (if-let [entry (get @dbs arg)]
                                  (:db entry)
                                  arg))]
           (if (map? (first args))
             ;; Map syntax: {:query ... :args [...]}
             (d/q (update (first args) :args (fn [a] (mapv resolve-db-arg a))))
             ;; Vector syntax: query db & args
             (apply d/q (first args) (mapv resolve-db-arg (rest args))))))}

    ;; DB consumers - resolve db, return value
    pull        {:resolve {0 :db} :returns :value}
    pull-many   {:resolve {0 :db} :returns :value}
    entity      {:resolve {0 :db} :returns :value :transform :entity->map}
    datoms      {:resolve {0 :db} :returns :value :transform :datoms->seqs}
    seek-datoms {:resolve {0 :db} :returns :value :transform :datoms->seqs}
    schema      {:resolve {0 :db} :returns :value}
    reverse-schema {:resolve {0 :db} :returns :value}
    metrics     {:resolve {0 :db} :returns :value}

    ;; Connection consumers
    transact {:resolve {0 :conn}
              :returns :value
              :transform :tx-report->summary}})

(def pod-excluded-operations
  "Operations excluded from pod with documented reasons."
  '{listen        "Requires persistent callbacks - not supported in pod protocol"
    unlisten      "Requires persistent callbacks - not supported in pod protocol"
    transact!     "Async variant - pods are synchronous"
    tempid        "Only useful within transaction context"
    entity-db     "Returns DB from entity - limited utility in pod context"
    filter        "Requires function argument - cannot serialize across pod boundary"
    is-filtered   "Limited utility in pod context"
    with          "Returns tx-report with db objects - use db-with instead"
    load-entities "Batch loading - use transact instead"
    query-stats   "Not yet exposed in pod"
    index-range   "Advanced index operation - can add later"
    gc-storage    "Maintenance operation - can add later"})

(def pod-additions
  "Pod-specific operations not in main API specification."
  '{release-db {:doc "Release a cached database snapshot from pod memory.

                      Takes the db-id that was returned from db, as-of, since, or history.
                      Returns an empty map."
                :args [:db-id]
                :returns :value}})

(def with-db-macro-code
  "Client-side macro for automatic db cleanup."
  "(defmacro with-db [bindings & body]
     (cond
       (= (count bindings) 0) `(do ~@body)
       (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                                 (try
                                   (with-db ~(subvec bindings 2) ~@body)
                                   (finally
                                     (release-db ~(bindings 0)))))
       :else (throw (IllegalArgumentException.
                      \"with-db only allows Symbols in bindings\"))))")

;; =============================================================================
;; Transform Registry
;; =============================================================================

(def transforms
  "Named transforms for pod result processing.
   Values are quoted forms that will be inlined in generated code."
  '{:entity->map
    (fn [e] (reduce-kv #(assoc %1 %2 %3) {} e))

    :datoms->seqs
    (fn [datoms] (map seq datoms))

    :tx-report->summary
    (fn [{:keys [db-before db-after tx-meta tx-data tempids]}]
      {:tempids tempids
       :db-before (select-keys db-before [:max-tx :max-eid])
       :db-after (select-keys db-after [:max-tx :max-eid])
       :tx-meta tx-meta
       :tx-data (map seq tx-data)})})

;; =============================================================================
;; Arity Extraction from Malli Schemas
;; =============================================================================

(defn extract-arities
  "Extract argument arities from malli function schema.
   Returns vector of arity vectors, e.g. [[] [config] [config opts]]"
  [schema]
  (cond
    ;; [:function [:=> [:cat ...] ret] [:=> [:cat ...] ret] ...]
    (and (vector? schema) (= :function (first schema)))
    (vec (for [arity-schema (rest schema)
               :when (and (vector? arity-schema) (= :=> (first arity-schema)))]
           (let [[_ input-schema _] arity-schema]
             (if (and (vector? input-schema) (= :cat (first input-schema)))
               (vec (rest input-schema))
               []))))

    ;; [:=> [:cat ...] ret]
    (and (vector? schema) (= :=> (first schema)))
    (let [[_ input-schema _] schema]
      (if (and (vector? input-schema) (= :cat (first input-schema)))
        [(vec (rest input-schema))]
        [[]]))

    :else
    [[]]))

(defn variadic-schema?
  "Check if a schema element is a variadic marker like [:* :any]"
  [schema-elem]
  (and (vector? schema-elem)
       (= :* (first schema-elem))))

(defn arity-arg-names
  "Generate argument names for an arity.
   Returns vector of symbols like [arg0 arg1 arg2] or [arg0 arg1 & args] for variadic.
   Also returns metadata {:variadic? bool}"
  [arity-schema]
  (let [;; Check if last element is variadic
        has-variadic? (and (seq arity-schema)
                           (variadic-schema? (last arity-schema)))
        ;; Fixed args are all but the variadic marker
        fixed-schema (if has-variadic?
                       (butlast arity-schema)
                       arity-schema)
        fixed-names (vec (map-indexed (fn [i _] (symbol (str "arg" i))) fixed-schema))]
    (if has-variadic?
      {:args (vec (concat fixed-names ['& 'rest-args]))
       :variadic? true
       :fixed-count (count fixed-names)}
      {:args fixed-names
       :variadic? false
       :fixed-count (count fixed-names)})))

;; =============================================================================
;; Code Generation - Simplified Approach
;; =============================================================================

(defn make-resolver-bindings
  "Generate let bindings for resolving arguments.
   Returns a vector of [original-sym resolved-sym resolver-call] tuples."
  [resolve-config arg-names]
  (when (and resolve-config (map? resolve-config))
    (for [[pos resolver] resolve-config
          :when (number? pos)
          :let [arg-sym (nth arg-names pos nil)]
          :when arg-sym]
      (let [resolved-sym (symbol (str (name arg-sym) "-resolved"))
            resolve-fn (case resolver
                         :conn 'resolve-conn
                         :db 'resolve-db
                         nil)]
        [arg-sym resolved-sym (when resolve-fn (list resolve-fn arg-sym))]))))

(defn wrap-with-transform
  "Wrap result expression with transform if specified."
  [transform result-expr]
  (if transform
    (list (get transforms transform) result-expr)
    result-expr))

;; =============================================================================
;; Function Generation
;; =============================================================================

(defn generate-api-call
  "Generate the API call expression, handling variadic args with apply."
  [api-impl fixed-args variadic?]
  (if variadic?
    `(apply ~api-impl ~@fixed-args ~'rest-args)
    `(~api-impl ~@fixed-args)))

(defn generate-arity-body
  "Generate the body for a single arity of a pod function.
   Uses explicit symbols instead of gensyms for clarity in generated code.
   arity-info is {:args [...] :variadic? bool :fixed-count n}"
  [op-name overlay arity-info api-impl]
  (let [{:keys [resolve returns transform post-action]} overlay
        {:keys [args variadic? fixed-count]} arity-info
        ;; Extract just the named args (excluding & and rest-args)
        named-args (if variadic?
                     (take fixed-count args)
                     args)
        ;; Symbols for generated code
        result-sym 'result
        db-id-sym 'db-id
        conn-id-sym 'conn-id
        ;; First arg is typically the one being resolved
        first-arg (first named-args)
        rest-named-args (rest named-args)
        ;; Check what kind of resolution we need
        resolves-conn? (= :conn (get resolve 0))
        resolves-db? (= :db (get resolve 0))]

    (cond
      ;; Case: resolve conn, return conn-id (connect)
      (and resolves-conn? (= returns :conn-id))
      `(let [~result-sym ~(generate-api-call api-impl named-args variadic?)
             ~conn-id-sym (~'generate-conn-id ~(or first-arg {}))]
         (swap! ~'conns assoc ~conn-id-sym ~result-sym)
         ~conn-id-sym)

      ;; Case: resolve conn, return db-id (db from conn)
      (and resolves-conn? (= returns :db-id))
      `(let [~'conn (~'resolve-conn ~first-arg)
             ~result-sym (~'d/db ~'conn)
             ~db-id-sym (~'generate-db-id ~result-sym)]
         (swap! ~'dbs assoc ~db-id-sym {:db ~result-sym :conn-id ~first-arg})
         ~db-id-sym)

      ;; Case: resolve conn, return value (transact, release)
      (and resolves-conn? (= returns :value))
      (let [call-expr (if variadic?
                        `(apply ~api-impl ~'conn ~@rest-named-args ~'rest-args)
                        `(~api-impl ~'conn ~@rest-named-args))
            call-with-resolved `(let [~'conn (~'resolve-conn ~first-arg)
                                      ~result-sym ~call-expr]
                                  ~(wrap-with-transform transform result-sym))]
        (if post-action
          `(let [~'ret ~call-with-resolved]
             ~(case post-action
                :remove-conn `(swap! ~'conns dissoc ~first-arg)
                nil)
             ~'ret)
          call-with-resolved))

      ;; Case: resolve db, return db-id (as-of, since, history, db-with)
      (and resolves-db? (= returns :db-id))
      (let [call-expr (if variadic?
                        `(apply ~api-impl ~'origin-db ~@rest-named-args ~'rest-args)
                        `(~api-impl ~'origin-db ~@rest-named-args))]
        `(let [~'origin-db (~'resolve-db ~first-arg)
               ~'parent-conn-id (get-in @~'dbs [~first-arg :conn-id])
               ~result-sym ~call-expr
               ~db-id-sym (~'generate-db-id ~result-sym)]
           (swap! ~'dbs assoc ~db-id-sym {:db ~result-sym :conn-id ~'parent-conn-id})
           ~db-id-sym))

      ;; Case: resolve db, return value (pull, entity, datoms, etc.)
      (and resolves-db? (= returns :value))
      (let [call-expr (if variadic?
                        `(apply ~api-impl ~'db ~@rest-named-args ~'rest-args)
                        `(~api-impl ~'db ~@rest-named-args))]
        `(let [~'db (~'resolve-db ~first-arg)
               ~result-sym ~call-expr]
           ~(wrap-with-transform transform result-sym)))

      ;; Case: no resolution, return conn-id
      (= returns :conn-id)
      `(let [~result-sym ~(generate-api-call api-impl named-args variadic?)
             ~conn-id-sym (~'generate-conn-id ~(or first-arg {}))]
         (swap! ~'conns assoc ~conn-id-sym ~result-sym)
         ~conn-id-sym)

      ;; Case: no resolution, return value (database-exists?, create-database, etc.)
      :else
      `(let [~result-sym ~(generate-api-call api-impl named-args variadic?)]
         ~(wrap-with-transform transform result-sym)))))

(defn generate-function
  "Generate a complete pod function definition."
  [op-name]
  (let [spec (get api-specification op-name)
        overlay (get pod-operations op-name)
        custom-fn (:custom-fn overlay)]

    (if custom-fn
      ;; Use custom function body from overlay
      `(def ~op-name ~custom-fn)

      ;; Generate function from specification
      (let [arities (extract-arities (:args spec))
            api-impl (symbol "d" (name op-name))]
        (if (= 1 (count arities))
          ;; Single arity
          (let [arity-info (arity-arg-names (first arities))]
            `(defn ~op-name ~(:args arity-info)
               ~(generate-arity-body op-name overlay arity-info api-impl)))

          ;; Multiple arities
          `(defn ~op-name
             ~@(for [arity arities
                     :let [arity-info (arity-arg-names arity)]]
                 `(~(:args arity-info)
                   ~(generate-arity-body op-name overlay arity-info api-impl)))))))))

;; =============================================================================
;; Describe Map Generation
;; =============================================================================

(defn first-sentence
  "Extract first sentence from docstring."
  [doc]
  (when doc
    (-> doc
        (str/split #"\.\s")
        first
        (str "."))))

(defn generate-var-entry
  "Generate a describe-map entry for an operation."
  [op-name]
  (let [spec (get api-specification op-name)
        overlay (get pod-operations op-name)]
    {"name" (name op-name)}))

(defn generate-describe-map
  "Generate the complete describe-map for pod protocol."
  []
  (let [;; Operations from overlay
        op-entries (for [op-name (keys pod-operations)]
                     (generate-var-entry op-name))
        ;; Pod additions
        addition-entries (for [[op-name config] pod-additions]
                           {"name" (name op-name)})
        ;; with-db macro
        macro-entry {"name" "with-db" "code" with-db-macro-code}]
    (vec (concat op-entries addition-entries [macro-entry]))))

;; =============================================================================
;; Publics Map Generation
;; =============================================================================

(defn generate-publics-map
  "Generate the publics lookup map."
  []
  (let [ops (for [op-name (keys pod-operations)]
              [(list 'quote op-name) op-name])
        additions (for [[op-name _] pod-additions]
                    [(list 'quote op-name) op-name])]
    `(def ~'publics
       ~(into {} (concat ops additions)))))

;; =============================================================================
;; Full Code Generation
;; =============================================================================

(defn generate-runtime-code
  "Generate the pod runtime infrastructure code."
  []
  '((def conns (atom {}))
    (def dbs (atom {}))

    (defn generate-conn-id [config]
      (str "conn:" (hash config)))

    (defn generate-db-id
      "Generate a unique ID for a database snapshot.
       Always creates a fresh ID to ensure speculative dbs (from db-with)
       don't collide with their source dbs."
      [db]
      (str (datahike.writing/create-commit-id db)))

    (defn resolve-conn [conn-id]
      (or (get @conns conn-id)
          (throw (ex-info "Connection not found" {:conn-id conn-id}))))

    (defn resolve-db [db-id]
      (or (get-in @dbs [db-id :db])
          (throw (ex-info "Database not found" {:db-id db-id}))))

    (defn resolve-db-in-args [args]
      (mapv (fn [arg]
              (if (and (string? arg) (get @dbs arg))
                (get-in @dbs [arg :db])
                arg))
            args))

    (defn release-db [db-id]
      (swap! dbs dissoc db-id)
      {})))

(defn generate-all-functions
  "Generate all pod function definitions."
  []
  (for [op-name (keys pod-operations)]
    (generate-function op-name)))

(defn generate-pod-namespace
  "Generate the complete pod namespace code."
  []
  (let [runtime (generate-runtime-code)
        functions (generate-all-functions)
        publics (generate-publics-map)
        describe-map `(def ~'describe-map ~(generate-describe-map))]
    {:runtime runtime
     :functions functions
     :publics publics
     :describe-map describe-map}))

;; =============================================================================
;; Validation
;; =============================================================================

(defn validate-coverage
  "Validate that all API operations are either implemented or excluded."
  []
  (let [all-ops (set (keys api-specification))
        implemented (set (keys pod-operations))
        excluded (set (keys pod-excluded-operations))
        covered (clojure.set/union implemented excluded)
        missing (clojure.set/difference all-ops covered)]
    (when (seq missing)
      (println "WARNING: Operations missing from pod overlay:")
      (doseq [op missing]
        (println "  -" op)))
    (empty? missing)))

;; =============================================================================
;; Debug / Development
;; =============================================================================

(defn print-generated-function
  "Print generated code for a single function (for debugging)."
  [op-name]
  (clojure.pprint/pprint (generate-function op-name)))

(defn print-all-generated
  "Print all generated code (for debugging)."
  []
  (let [{:keys [runtime functions publics describe-map]} (generate-pod-namespace)]
    (println ";; Runtime")
    (doseq [form runtime]
      (clojure.pprint/pprint form)
      (println))
    (println "\n;; Functions")
    (doseq [form functions]
      (clojure.pprint/pprint form)
      (println))
    (println "\n;; Publics")
    (clojure.pprint/pprint publics)
    (println "\n;; Describe Map")
    (clojure.pprint/pprint describe-map)))

;; =============================================================================
;; Compile-Time Code Generation Macros
;; =============================================================================

(defmacro defpod-runtime
  "Generate pod runtime infrastructure (atoms, helper functions)."
  []
  `(do
     ~@(generate-runtime-code)))

(defmacro defpod-functions
  "Generate all pod API functions from the overlay specification."
  []
  `(do
     ~@(generate-all-functions)))

(defmacro defpod-publics
  "Generate the publics map for pod protocol lookup."
  []
  (generate-publics-map))

(defmacro defpod-describe-map
  "Generate the describe-map for pod protocol."
  []
  `(def ~'describe-map ~(generate-describe-map)))
