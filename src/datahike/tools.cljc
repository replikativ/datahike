(ns ^:no-doc datahike.tools
  (:require
   [superv.async :refer [throw-if-exception-]]
   [clojure.core.async.impl.protocols :as async-impl]
   [hasch.core :refer [uuid]]
   [clojure.core.async :as async]
   #?(:clj [clojure.java.io :as io])
   [replikativ.logging :as log])
  #?(:cljs (:require-macros [datahike.tools :refer [match-vector]]))
  #?(:clj (:import [java.util Properties UUID Date]
                   [java.util.concurrent CompletableFuture]
                   [java.net InetAddress])))

(defn combine-hashes [x y]
  #?(:clj  (clojure.lang.Util/hashCombine x y)
     :cljs (hash-combine x y)))

(defn -match-vector-class [x]
  (case x
    _ :negative
    * :any
    :positive))

(defn -match-vector [path pattern-pos pattern-size pattern-symbols pairs]
  (cond
    (< pattern-pos pattern-size)
    (let [groups (group-by (comp -match-vector-class #(nth % pattern-pos) first) pairs)
          sub (fn [p pairs] (-match-vector (conj path p)
                                           (inc pattern-pos)
                                           pattern-size
                                           pattern-symbols
                                           pairs))]
      (if (= [:any] (keys groups))
        (sub '* (:any groups))
        `(if ~(nth pattern-symbols pattern-pos)
           ~(sub 1 (mapcat groups [:positive :any]))
           ~(sub '_ (mapcat groups [:negative :any])))))

    (not= 1 (count pairs)) (throw (ex-info "There should be exactly one expression at leaf"
                                           {:path path}))
    :else (-> pairs first second)))

(defmacro match-vector [input-vector & pattern-expr-pairs]
  {:pre [(sequential? pattern-expr-pairs)
         (even? (count pattern-expr-pairs))]}
  (let [pairs (partition 2 pattern-expr-pairs)
        patterns (map first pairs)
        _ (assert (every? sequential? patterns))
        pattern-sizes (into #{} (map count) patterns)
        _ (assert (= 1 (count pattern-sizes)))
        pattern-size (first pattern-sizes)
        symbols (repeatedly pattern-size gensym)]
    `(let [[~@symbols] ~input-vector]
       ~(-match-vector [] 0 pattern-size symbols pairs))))

(defn ^:dynamic get-date []
  #?(:clj (Date.)
     :cljs (js/Date.)))

(defn ^:dynamic get-time-ms []
  #?(:clj (.getTime (Date.))
     :cljs (.getTime (js/Date.))))

(defn date->epoch-ms
  "Epoch milliseconds of a `Date`, cross-platform and native-image-safe.
   The `^Date` hint is load-bearing: an un-hinted `.getTime` on a Date compiles
   to a REFLECTIVE call (`clojure.lang.Reflector`), which works on the JVM but
   throws on GraalVM native-image (no reflect-config for java.util.Date) — it was
   exactly that, in `next-tx-instant`, that broke the native-image build. Route
   every Date→millis conversion through here."
  ^long [d]
  #?(:clj  (.getTime ^Date d)
     :cljs (.getTime d)))

;; Clock pinning for repeatable test runs / regulator replays:
;;
;; The writer runs transactions on a background thread, so per-call
;; dynamic bindings (`(binding [get-date ...] ...)`) don't propagate.
;; Two patterns work instead:
;;
;; 1. **Per-tx override via tx-meta** — pass `:db/txInstant <Date>` in
;;    your tx's `:tx-meta`; the transactor merges your value over the
;;    `get-date` default at `transact-tx-data` (db/transaction.cljc).
;;    Simplest for deterministic snapshots and the one we recommend
;;    for tests.
;;
;; 2. **Global override via `alter-var-root`** — for whole-suite test
;;    pinning, redefine `get-date` once at fixture setup. This is
;;    coarser than `binding` but survives the thread hop.

;; adapted from https://clojure.atlassian.net/browse/CLJ-2766
#?(:clj
   (defn- unwrap-execution
     "The error the caller actually threw, not the CompletableFuture's wrapper.

      `.completeExceptionally` + `.get` re-raises as `ExecutionException`, whose
      message is the cause's `toString` and whose `ex-data` is NOTHING. Rethrowing
      that hands every caller an error they can only string-match: `(ex-data e)`
      is nil even when the original carried a perfectly good `:type`. Unwrap once
      so a caller can dispatch on what was actually thrown."
     [t]
     (if (instance? java.util.concurrent.ExecutionException t)
       (or (.getCause ^java.util.concurrent.ExecutionException t) t)
       t)))

#?(:clj
   (defn throwable-promise
     "Returns a promise object that can be read with deref/@, and set, once only, with deliver. Calls to deref/@ prior to delivery will block, unless the variant of deref with timeout is used. All subsequent derefs will return the same delivered value without blocking. Exceptions delivered to the promise will throw on deref. 
   
      Also supports core.async take! to optionally consume values without blocking the reader thread."
     []
     (let [cf (CompletableFuture.)
           p (async/promise-chan)]
       (reify
         clojure.lang.IDeref
         (deref [_] (throw-if-exception- (try (.get cf) (catch Throwable t (unwrap-execution t)))))
         clojure.lang.IBlockingDeref
         (deref [_ timeout-ms timeout-val]
           (if-let [v (try (.get cf timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)
                           (catch Throwable t (unwrap-execution t)))]
             (throw-if-exception- v)
             timeout-val))
         clojure.lang.IPending
         (isRealized [_] (.isDone cf))
         clojure.lang.IFn
         (invoke [this x]
           (if (instance? Throwable x)
             (.completeExceptionally cf x)
             (.complete cf x))
           (if-not (nil? x) (async/put! p x) (async/close! p))
           this)
         async-impl/ReadPort
         (take! [_this handler] (async-impl/take! p handler))
         async-impl/WritePort
         (put! [_ val handler]
           (if (instance? Throwable val)
             (.completeExceptionally cf val)
             (.complete cf val))
           (async-impl/put! p val handler)))))
   :cljs (def throwable-promise async/promise-chan))

(defn delivered!
  "Guard a value taken from an async operation's channel. Returns `v`, or throws
   if the channel closed without delivering one.

   A `go-try-` block converts an escaping Exception into a channel VALUE, so
   `(<?- ch)` rethrows it and the caller sees the failure. That covers the
   ordinary case and is why the codebase reads as if it did. It does not cover
   the two ways a channel closes empty:

   * a **bare `go`** (or a `go-try-` whose throwable is not an Exception — a JVM
     `Error`, or on ClojureScript anything thrown that is not a `js/Error`)
     closes its channel instead of putting;
   * `put!`/`>!` **refuse nil**, so an operation that legitimately produces nil
     and forwards it to another channel throws inside the forwarding block and
     closes that channel too.

   In both cases `<?-` yields `nil` — not an error, a value. Every consumer then
   does something plausible and wrong: `(reduce rf acc nil)` is `acc`, so a chunk
   that failed to read becomes an EMPTY chunk and the import continues, reports
   success, and is short by exactly that chunk. That is the shape this exists to
   stop: the failure is not that an operation errored, it is that erroring became
   indistinguishable from returning nothing.

   `ctx` is merged into the ex-data so the site names itself — a bare
   \"unexpected nil\" is nearly as unhelpful as the silence it replaces."
  ([v ctx] (delivered! v ctx "an async operation delivered no result"))
  ([v ctx msg]
   (if (nil? v)
     (throw (ex-info (str msg
                          " — its channel closed without a value, which means it"
                          " failed in a way that could not be reported.")
                     (merge {:error :async/no-result} ctx)))
     v)))

(defn call-reporting-foreign-throws
  "Call `f`, converting a throw that the async machinery cannot report into one
   it can. Returns whatever `f` returns.

   ClojureScript can throw ANY value — `(throw #js {…})`, `(throw \"oops\")` —
   and `go-try-` expands to `(catch js/Error e e)` there, so such a value is not
   caught. What happens next is worse than the silent nil this codebase spends
   so much effort on: core.async's `run-state-machine-wrapped` catches
   `js/Object`, closes the go block's channel, and **rethrows**, onto the
   microtask queue where nothing is listening. Measured on Node — the whole
   process exits, mid-import, with the bare object printed and no stack:

       cljs.core.async.impl.ioc_helpers.js:99
       throw ex;
       { msg: 'a foreign object, not a js/Error' }

   Wrapping it in an `ex-info` puts it back inside what `go-try-` and `<?-`
   already handle correctly, with the original value preserved under
   `:thrown`.

   This can only cover a SYNCHRONOUS throw at the call site. A foreign value
   thrown inside the callee's own go block escapes into core.async before any
   caller can see it, and terminates the process just the same; guarding that
   would mean widening `go-try-`'s ClojureScript catch to `:default`, which is
   superv.async's decision, not ours. The synchronous case is the one that
   matters most here anyway: it is where a CALLER-SUPPLIED function runs — the
   `:read` of a record source — and a caller is exactly who might throw
   something that is not an `Error`.

   On the JVM this is a plain call: `Throwable`s that are not `Exception`s are
   deliberately left to propagate, and there is no analogous class of
   unreportable throw."
  [f ctx]
  #?(:clj (f)
     :cljs (try (f)
                (catch :default e
                  (if (instance? js/Error e)
                    (throw e)
                    (throw (ex-info (str "A non-Error value was thrown"
                                         (when-let [o (:op ctx)] (str " by " o))
                                         ". ClojureScript permits this, but the async"
                                         " machinery cannot report it — core.async would"
                                         " close the channel and rethrow onto the microtask"
                                         " queue, terminating the process. Throw an Error"
                                         " (or ex-info) instead.")
                                    (merge {:error :async/foreign-throw
                                            :thrown e}
                                           ctx))))))))

(defn cause-chain
  "`t` and every exception it wraps, outermost first."
  [t]
  (take-while some? (iterate ex-cause t)))

(defn ex-error
  "What an exception names about ITSELF — its `:error`, or failing that its
   `:type` — found by looking THROUGH the wrappers. Returns nil when nothing in
   the chain says.

   `(:error (ex-data e))` on the exception you were handed is usually nil even
   when the original said exactly what went wrong, because a failure crossing
   the writer boundary is wrapped twice: `throwable-promise`'s deref calls
   `.get` on a CompletableFuture, which raises `ExecutionException` (ex-data
   nil), and `superv.async/throw-if-exception-` then wraps THAT, reading its
   ex-data — so the original's keys are two levels down.

   Measured consequence, before this existed: a `:transact/unique` conflict and
   a dead writer (`:writer-shut-down`) reached the import's error handler
   looking identical, and both were filed under the handler's fallback label,
   `:import/corrupt-datom`. A store outage was reported as 74 corrupt datoms."
  [t]
  (some (fn [e] (let [d (ex-data e)] (or (:error d) (:type d))))
        (cause-chain t)))

#?(:clj
   (defn get-version
     "Retrieves the current version of a dependency. Thanks to https://stackoverflow.com/a/33070806/10978897"
     [dep]
     (let [path (str "META-INF/maven/" (or (namespace dep) (name dep))
                     "/" (name dep) "/pom.properties")
           props (io/resource path)]
       (when props
         (with-open [stream (io/input-stream props)]
           (let [props (doto (Properties.) (.load stream))]
             (.getProperty props "version")))))))

#?(:clj (def datahike-version (or (get-version 'org.replikativ/datahike) "DEVELOPMENT")))

#?(:clj (def hitchhiker-tree-version
          (try (get-version 'io.replikativ/hitchhiker-tree)
               (catch Exception _ nil))))

#?(:clj (def persistent-set-version (get-version 'org.replikativ/persistent-sorted-set)))

#?(:clj (def konserve-version (get-version 'org.replikativ/konserve)))

#?(:clj
   (defmacro meta-data []
     `{:datahike/version ~datahike-version
       :konserve/version ~konserve-version
       :hitchhiker.tree/version ~hitchhiker-tree-version
       :persistent.set/version ~persistent-set-version
       :datahike/id (uuid)
       :datahike/created-at (get-date)}))

(defn deep-merge
  "Recursively merges maps together. If all the maps supplied have nested maps
  under the same keys, these nested maps are merged. Otherwise the value is
  overwritten, as in `clojure.core/merge`.

  Copied from weavejester/medley 1.3.0"
  {:arglists '([& maps])
   :added    "1.1.0"}
  ([])
  ([a] a)
  ([a b]
   (when (or a b)
     (letfn [(merge-entry [m e]
               (let [k  (key e)
                     v' (val e)]
                 (if (contains? m k)
                   (assoc m k (let [v (get m k)]
                                (if (and (map? v) (map? v'))
                                  (deep-merge v v')
                                  v')))
                   (assoc m k v'))))]
       (reduce merge-entry (or a {}) (seq b)))))
  ([a b & more]
   (reduce deep-merge (or a {}) (cons b more))))

(defn timed [f]
  (let [now #?(:clj #(. System (nanoTime))
               :cljs #(* 1000 (. (js/Date.) (getTime))))
        start (now)
        result (f)
        end (now)
        t (/ (double (- end start))
             1000000.0)]
    {:res result
     :t t}))

(defn get-hostname []
  #?(:clj (.getHostAddress (InetAddress/getLocalHost))
     :cljs "" #_(raise "Not supported." {:type :hostname-not-supported})))

(defmacro with-destructured-vector [v & var-expr-pairs]
  {:pre [(even? (count var-expr-pairs))]}
  (let [pairs (partition 2 var-expr-pairs)
        vars (mapv first pairs)
        vsym (gensym)
        nsym (gensym)
        generate (fn generate [acc pairs]
                   (let [i (count acc)]
                     `(if (<= ~nsym ~i)
                        ~acc
                        ~(if (empty? pairs)
                           `(throw (ex-info "Pattern mismatch"
                                            {:input ~vsym
                                             :pattern (quote ~var-expr-pairs)}))
                           (let [[[_ expr] & pairs] pairs
                                 g (gensym)]
                             `(let [~g ~expr]
                                ~(generate (conj acc g) pairs)))))))]
    `(let [~vsym ~v
           ~nsym (count ~vsym)
           ~vars ~vsym]
       ~(generate [] pairs))))

(defn- reduce-clauses
  [resolver context clauses]
  (loop [context context
         clauses clauses
         failed-clauses []]
    (if (empty? clauses)
      [context failed-clauses]
      (let [[clause & clauses] clauses]
        (if-let [next-context (resolver context clause)]
          (recur next-context clauses failed-clauses)
          (recur context clauses (conj failed-clauses clause)))))))

(defn resolve-clauses [resolver context clauses]
  (if (empty? clauses)
    context
    (let [[context failed-clauses] (reduce-clauses resolver
                                                   context
                                                   clauses)]
      (if (= (count failed-clauses)
             (count clauses))
        (log/raise "Cannot resolve any more clauses"
                   {:clauses clauses})
        (recur resolver context failed-clauses)))))

(defn group-by-step
  "Create a step function to use with `transduce` for grouping values"
  [f]
  (fn
    ([] (transient {}))
    ([dst] (persistent! dst))
    ([dst x]
     (let [k (f x)]
       (assoc! dst k (conj (get dst k []) x))))))

(defn range-subset-tree
  "This function generates code for a decision tree that for an input expression `input` that has to represent a sequence of growing integers that is a subset of the integers in the sequence `(range length-length)`. Every leaf in the decision tree corresponds to one of the 2^range-length possible subsequences and the `branch-visitor-fn` is called at every leaf with the first argument being the subsequence and the second argument being a mask."
  ([range-length input branch-visitor-fn]
   (if (symbol? input)
     (range-subset-tree range-length
                        input
                        branch-visitor-fn
                        0
                        []
                        (vec (repeat range-length nil)))
     (let [sym (gensym)]
       `(let [~sym ~input]
          ~(range-subset-tree range-length sym branch-visitor-fn)))))
  ([range-length input-symbol branch-visitor-fn at acc-inds mask]
   {:pre [(number? range-length)
          (symbol? input-symbol)
          (ifn? branch-visitor-fn)
          (number? at)
          (vector? acc-inds)]}
   (if (= range-length at)
     (branch-visitor-fn acc-inds mask)
     `(if (empty? ~input-symbol)
        ~(branch-visitor-fn acc-inds mask)
        (if (= ~at (first ~input-symbol))
          (let [~input-symbol (rest ~input-symbol)]
            ~(range-subset-tree range-length
                                input-symbol
                                branch-visitor-fn
                                (inc at)
                                (conj acc-inds at)
                                (assoc mask at (count acc-inds))))
          ~(range-subset-tree range-length
                              input-symbol
                              branch-visitor-fn
                              (inc at)
                              acc-inds
                              mask))))))

(defn distinct-sorted-seq? [cmp s]
  (if (empty? s)
    true
    (loop [previous (first s)
           s (rest s)]
      (if (empty? s)
        true
        (let [x (first s)]
          (if (neg? (cmp previous x))
            (recur x (rest s))
            false))))))

(defn merge-distinct-sorted-seqs
  "Takes a comparator function `cmp` and two sequences `seq-a` and `seq-b` that are both distinct and sorted by `cmp`. Then combines the elements from both sequences to form a new sorted sequence that is distinct. The function distinct-sorted-seq? must return true for all input sequences and the result will also be a sequence for which this function returns true."
  [cmp seq-a seq-b]
  (cond
    (empty? seq-a) seq-b
    (empty? seq-b) seq-a
    :else
    (let [a (first seq-a)
          b (first seq-b)
          i (cmp a b)]
      (cond
        (< i 0) (cons
                 a (lazy-seq
                    (merge-distinct-sorted-seqs cmp (rest seq-a) seq-b)))
        (= i 0) (cons
                 a (lazy-seq
                    (merge-distinct-sorted-seqs cmp (rest seq-a) (rest seq-b))))
        :else (cons
               b (lazy-seq
                  (merge-distinct-sorted-seqs cmp seq-a (rest seq-b))))))))
