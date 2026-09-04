(ns datahike.query.resolve
  "How a symbol in a query clause becomes a function.

   Queries reach a Datahike server from clients, over HTTP and over Kabel, so
   the symbols in `:where` function and predicate clauses, in aggregates, in
   `:db.attr/preds` and in `:db.entity/preds` are untrusted input. Resolving
   them through `clojure.core/resolve` or reflection would let any client run
   `load-string`, `slurp` or a shell command in the server process.

   Embedded in a Clojure process, Datahike keeps resolving symbols the
   permissive way (`permissive-symbol-resolver`: any loaded var, any method
   by reflection), since there the query author is the process itself. The
   server sets `*symbol-resolver*` to `safe-symbol-resolver` at start, which
   knows only `safe-fns`: a curated pure subset of `clojure.core` and all of
   `clojure.string`, plus whatever the process registered with `register-fn!`
   or `register-ns!`, which is how an application exposes its own functions
   to client queries. Set the resolver process-wide with `alter-var-root`
   rather than `binding` when transactions are involved: `:db.entity/preds`
   and `:db.attr/preds` run on the writer thread, which a binding does not
   reach. Compiled plans and results are cached per query, so choose the
   resolver before running queries rather than switching between runs.

   This namespace is a leaf so that the transaction path can use it without a
   cycle through `datahike.query`."
  #?(:clj (:require [clojure.string :as str]))
  #?(:clj (:import [clojure.lang Reflector]
                   [java.lang.reflect Method])))

;; Everything here is pure over its arguments: no I/O, no evaluation, no
;; namespace, var or thread access, no reflection. Infinite generators
;; (`iterate`, `cycle`, `repeatedly`) are left out on purpose.
#?(:clj
   (def ^:private safe-core-symbols
     '[;; arithmetic and comparison
       + - * / quot rem mod inc dec max min abs
       = == not= < > <= >= compare identical? zero? pos? neg? even? odd?
    ;; logic and type predicates
       not boolean true? false? nil? some? any?
       number? integer? int? nat-int? pos-int? neg-int? float? double? ratio? rational? decimal?
       string? char? keyword? symbol? ident? simple-keyword? qualified-keyword?
       simple-symbol? qualified-symbol? simple-ident? qualified-ident? uuid? inst? boolean? bytes?
       coll? map? vector? list? set? seq? sequential? associative? counted? sorted? indexed?
       empty? not-empty map-entry? distinct? fn? ifn?
    ;; conversion and construction of scalars
       str name namespace keyword symbol long int short byte double float bigint bigdec biginteger
       num char rationalize numerator denominator
       parse-long parse-double parse-boolean parse-uuid random-uuid
    ;; strings and regular expressions
       subs re-find re-matches re-seq re-pattern format pr-str prn-str print-str println-str
    ;; collections
       count get get-in contains? find nth first second last rest next butlast peek pop
       keys vals key val assoc assoc-in dissoc update update-in conj cons into vec vector
       set hash-set hash-map array-map sorted-map sorted-set list seq empty rseq subvec
       concat reverse sort sort-by distinct dedupe frequencies group-by
       partition partition-all partition-by take drop take-while drop-while take-last drop-last take-nth
       split-at split-with filter remove keep map mapv mapcat map-indexed keep-indexed
       reduce reductions apply some every? not-any? not-every? merge merge-with select-keys zipmap
       interleave interpose flatten range repeat min-key max-key shuffle rand rand-int rand-nth
    ;; function combinators (their function arguments come from the same resolution)
       identity constantly comp partial juxt fnil complement every-pred some-fn
       hash]))

(def safe-fns
  "The functions a query may name without any opt-in, keyed by bare symbol
   (`str`), by qualified core symbol (`clojure.core/str`) and by qualified
   string symbol (`clojure.string/upper-case`)."
  #?(:clj
     (let [core (ns-publics 'clojure.core)
           core-fns (into {} (keep (fn [sym]
                                     (when-some [v (get core sym)]
                                       [sym @v])))
                          safe-core-symbols)
           qualified (fn [ns-sym m] (into {} (map (fn [[k v]] [(symbol (name ns-sym) (name k)) v])) m))
           string-fns (into {} (map (fn [[k v]] [k @v])) (ns-publics 'clojure.string))]
       (merge core-fns
              (qualified 'clojure.core core-fns)
              (qualified 'clojure.string string-fns)))
     :cljs {}))

(defn- resolve-sym
  "The var named by a qualified symbol, loading its namespace on demand."
  [#?(:clj sym :cljs _)]
  #?(:cljs nil
     :clj (when (and (symbol? sym) (namespace sym))
            (try (some-> (requiring-resolve sym) deref)
                 (catch Throwable _ nil)))))

#?(:clj (def ^:private find-method
          (memoize
           (fn find-method-impl [^Class this-class method-name args-classes]
             (or (->> this-class
                      .getMethods
                      (some (fn [^Method method]
                              (when (and (= method-name (.getName method))
                                         (= (count args-classes)
                                            (.getParameterCount method))
                                         (every? true? (map #(Reflector/paramArgTypeMatch %1 %2)
                                                            (.getParameterTypes method)
                                                            args-classes)))
                                method))))
                 (throw (ex-info (str (.getName this-class) "."
                                      method-name "("
                                      (str/join "," (map #(.getName ^Class %) args-classes))
                                      ") not found")
                                 {:this-class this-class
                                  :method-name method-name
                                  :args-classes args-classes})))))))

(defn- resolve-method [#?(:clj method-sym :cljs _)]
  #?(:cljs nil
     :clj (when (symbol? method-sym)
            (let [method-str (name method-sym)]
              (when (= \. (.charAt method-str 0))
                (let [method-name (subs method-str 1)]
                  (fn [this & args]
                    (let [^Method method (find-method (class this) method-name (mapv class args))]
                      (Reflector/prepRet (.getReturnType method) (.invoke method this (into-array Object args)))))))))))

(defonce ^:private registry (atom {}))

(defn register-fn!
  "Make `f` callable from queries, aggregates, `:db.attr/preds` and
   `:db.entity/preds` as `sym`, a qualified symbol, under every resolver,
   the server's safe one included. Process-local: register on every process
   that runs the queries or the transactions. Returns `sym`."
  [sym f]
  (when-not (qualified-symbol? sym)
    (throw (ex-info "A query function is registered under a qualified symbol, e.g. 'app/valid-sku?"
                    {:error :query/register-fn :sym sym})))
  (when-not (ifn? f)
    (throw (ex-info (str "Not a function: " (pr-str f)) {:error :query/register-fn :sym sym})))
  (swap! registry assoc sym f)
  sym)

(defn unregister-fn!
  "Forget `sym`. Returns `sym`."
  [sym]
  (swap! registry dissoc sym)
  sym)

#?(:clj
   (defn register-ns!
     "Register every public function of namespace `ns-sym` under its qualified
      name, and under `alias/name` as well when `alias` is given, loading the
      namespace if needed. Returns the registered symbols."
     ([ns-sym] (register-ns! ns-sym nil))
     ([ns-sym alias]
      (require ns-sym)
      (let [entries (for [[n v] (ns-publics ns-sym)
                          :when (fn? @v)
                          sym (cond-> [(symbol (name ns-sym) (name n))]
                                alias (conj (symbol (name alias) (name n))))]
                      [sym @v])]
        (swap! registry into entries)
        (mapv first entries)))))

(defn registered
  "The registered functions, symbol to function."
  []
  @registry)

(defn safe-symbol-resolver
  "The resolver the server runs with: `safe-fns` and the registry, nothing
   from the runtime."
  [sym]
  (or (get safe-fns sym)
      (get @registry sym)))

(defn permissive-symbol-resolver
  "The default `*symbol-resolver*` on the JVM: `safe-fns`, then any qualified
   symbol through `requiring-resolve`, then a leading-dot symbol as a
   reflective method call. That is everything the JVM can reach, `load-string`
   included, so it is only for queries and schema the process wrote itself;
   a process that accepts them from clients uses `safe-symbol-resolver`."
  [sym]
  (or (get safe-fns sym)
      (get @registry sym)
      (resolve-sym sym)
      (resolve-method sym)))

(def ^:dynamic *symbol-resolver*
  "How a function or predicate symbol that is neither a query built-in nor
   bound in the query is turned into a function. Receives the symbol, returns
   a function or nil. `permissive-symbol-resolver` on the JVM; the server
   replaces it with `safe-symbol-resolver`."
  #?(:clj permissive-symbol-resolver :cljs safe-symbol-resolver))
