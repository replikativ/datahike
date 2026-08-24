(ns ^:no-doc datahike.index.persistent-set.warm
  "Budget-bounded breadth-first index warm for the persistent-set index — the
   adapter behind `datahike.api/warm-index`, `warm-datoms` and `warm-db`.

   THE WALK LIVES IN persistent-sorted-set NOW (`org.replikativ.persistent-
   sorted-set.warm`, PSS >= 0.5.142). It was first written here, entirely in
   PSS's own vocabulary — `Branch`, levels, child addresses,
   `IStorage.restore` — with its ClojureScript arm left as three documented
   holes; moving it to where the tree is made both platforms real and let
   every other consumer (stratum's per-attribute trees, proximum's id and
   metadata trees) share one implementation. What stays here is exactly what
   is datahike's: the option-map dialect (`:index-key`/`:siblings`/
   `:store-cache-size`), the clamp WARNING (PSS clamps and reports
   `:budget-clamped?`; whether that deserves a log line is the consumer's
   call, and datahike's answer is yes — but only when the caller asked for
   the budget explicitly), and the adaptation of PSS's async shapes to
   datahike's channel convention.

   For the rationale, the two bounds, budget sizing and selective warming,
   see the PSS namespace — the docstrings moved with the code.

   ## Platform shapes

   datahike's `-warm!` answers in datahike's convention: the report itself
   under `:sync? true`, a core.async channel carrying it (or the exception,
   as a value) otherwise.

     JVM  sync   -> PSS's synchronous walk, directly.
     JVM  async  -> the same walk on an `async/thread`; PSS deliberately
                    refuses `:sync? false` on the JVM rather than owning a
                    thread, so the thread is ours.
     cljs sync   -> PSS's sync arm (meaningful only when the storage supports
                    synchronous restore — a fully cached tree, or a sync
                    backing).
     cljs async  -> PSS returns a partial-cps expression; it is adapted onto
                    a promise-chan here, errors as values, which is the same
                    convention `chan->async-expr` serves in the opposite
                    direction in `datahike.index.persistent-set`."
  (:require
   #?(:clj [clojure.core.async :as async]
      :cljs [clojure.core.async :as async])
   [datahike.index.interface :as di]
   [org.replikativ.persistent-sorted-set.warm :as pss-warm]
   [replikativ.logging :as log]))

(def default-width
  "Concurrent in-flight restores — persistent-sorted-set's default (64 JVM,
   measured optimal against local MinIO; 6 cljs, the browser's per-origin
   connection budget)."
  pss-warm/default-width)

(def default-budget di/default-warm-budget)

(defn- entries
  "PSS `warm-trees!` entries from datahike's option dialect: this index under
   `:index-key`, plus `:siblings` — `[index-key index]` pairs sharing the same
   `:from`/`:to` bounds, which is correct because a db's indices are warmed for
   the same scan or none."
  [pset {:keys [index-key siblings from to] :or {index-key :index}}]
  (mapv (fn [[k s]] {:key k :set s :from from :to to})
        (cons [index-key pset] siblings)))

(defn- pss-opts
  [{:keys [depth budget width store-cache-size] :as opts}]
  (cond-> {:depth  (or depth :interior)
           :budget (or budget default-budget)
           :width  (or width default-width)}
    store-cache-size (assoc :cache-size store-cache-size)))

(defn- warn-on-clamp!
  "PSS clamps a budget past 0.8x the node cache and reports it; the WARNING is
   datahike's, and only when the caller asked for that budget explicitly — the
   default budget (2000) already exceeds the default cache (1000), so warning
   unconditionally would fire on every warm of an untuned database and say
   nothing about that database."
  [report {:keys [budget store-cache-size] :as opts}]
  (when (and (:budget-clamped? report) (contains? opts :budget))
    (log/warn :warm/budget-clamped
              {:requested budget
               :capped (long (* 0.8 store-cache-size))
               :store-cache-size store-cache-size
               :msg "budget exceeded 0.8x the entry-counted node cache; raise :store-cache-size to warm more"}))
  report)

(defn warm!
  "Breadth-first warm of a persistent-set index into its node cache — see the
   PSS walk for `:depth`/`:budget`/`:width`/`:from`/`:to` and the report;
   datahike adds `:index-key` (the label in `:by-index`), `:siblings` (other
   indices sharing this call's budget round-robin) and `:store-cache-size`
   (the clamp basis). Returns the report under `:sync?`, a channel carrying
   it otherwise."
  [pset opts]
  (let [es       (entries pset opts)
        popts    (pss-opts opts)
        sync?    (:sync? opts #?(:clj true :cljs false))]
    #?(:clj
       (if sync?
         (warn-on-clamp! (pss-warm/warm-trees! es (assoc popts :sync? true)) opts)
         ;; PSS refuses :sync? false on the JVM rather than owning a thread —
         ;; the thread is datahike's. Errors travel as VALUES on the channel,
         ;; the go-try-/<?- convention.
         (async/thread
           (try (warn-on-clamp! (pss-warm/warm-trees! es (assoc popts :sync? true)) opts)
                (catch Exception e e))))
       :cljs
       (if sync?
         (warn-on-clamp! (pss-warm/warm-trees! es (assoc popts :sync? true)) opts)
         ;; A partial-cps expression, adapted onto a promise-chan: exactly one
         ;; of resolve/raise fires, and either way the caller's take succeeds —
         ;; errors as values, like every other channel in this API.
         (let [ch (async/promise-chan)
               expr (pss-warm/warm-trees! es (assoc popts :sync? false))]
           (expr (fn [report] (async/put! ch (warn-on-clamp! report opts)))
                 (fn [err] (async/put! ch err)))
           ch)))))
