(ns datahike.experimental.diff
  "What changed between two database values — **experimental**.

   Two operations, one on top of the other:

     (diff db-before db-after)      ;=> {:added [datom ...] :removed [datom ...]}
     (tx-range db-before db-after)  ;=> [{:t 536870915 :data [datom ...]} ...]

   ## Why this can be cheap

   Datahike's indexes are persistent trees, so two database values that share
   history share every index node they have in common. `psset/diff` exploits
   that: a subtree whose address appears on both sides cannot contain a
   difference, so it is skipped WITHOUT BEING READ. The cost is proportional to
   the change rather than to the database — measured in persistent-sorted-set at
   3-4 node reads for a two-element delta whether the set holds a thousand
   elements or a hundred thousand.

   That is what makes catching a replica up practical: the reader pays for the
   delta, not for the database, and never has to hold a transaction listener
   open on the production machine.

   ## tx-range, and why it needs history

   `tx-range` is `diff` plus `(group-by :tx)`. A datom carries the transaction
   that asserted or retracted it, so grouping the delta by `:tx` reconstructs
   the transactions exactly — verified against the `:tx-data` datahike itself
   reported, over a window containing a card-one overwrite, a card-many
   retraction, a full `retractEntity`, a plain assertion, and an assert-then-
   retract within a SINGLE transaction.

   It reads the CURRENT and the TEMPORAL index and unions them. Both are needed
   and neither is sufficient: a datom asserted and retracted inside the window
   never appears in the current index at all, and a datom that is merely
   superseded leaves the current index without leaving any record there of WHEN
   it left. The temporal index holds both the superseded assertion and the
   retraction marker, so the union is complete.

   The union is then filtered to `(before's max-tx, after's max-tx]`. That
   filter is not cosmetic: the temporal index also receives NEW assertions from
   card-one upserts, so it is not merely \"the retracted things\", and without
   the filter a diff would report datoms belonging to transactions outside the
   window.

   **`tx-range` therefore requires `:keep-history? true`** and refuses without
   it. On a history-free database a retraction leaves no trace beyond the datom's
   absence, and the datom that vanished carries the tx that ASSERTED it, not the
   one that removed it — so the retraction cannot be attributed to a transaction
   at all. `diff` still works there and still reports it under `:removed`; only
   the per-transaction attribution is impossible.

   ## Garbage collection

   The nodes `diff` needs from the OLDER database are exactly the ones
   `d/gc-storage` reclaims — they are unreachable from the newer root, which is
   what makes them collectable. So a diff across a GC boundary fails, and it
   fails by way of a missing node deep inside the index rather than anywhere
   informative. Both entry points catch that and re-throw as
   `:diff/garbage-collected` naming both databases.

   Datomic's `tx-range` does not have this problem because it reads a dedicated
   T-major log tree that is not subject to the same reclamation. Datahike has no
   such index; `diff` reads the same trees queries read. Retaining what you
   intend to diff against — by not collecting past it — is the user's call, and
   the error says so rather than leaving a missing-node exception to be
   interpreted.

   ## Status

   Experimental and deliberately outside `datahike.api`: the shape of the return
   value, and whether `tx-range` should take timestamps rather than database
   values, are both unsettled. Persistent-set indexes only — the hitchhiker tree
   shares structure differently and is refused by name."
  (:require [clojure.core.async :as async]
            [datahike.db.interface :as dbi]
            [datahike.index.persistent-set :as dip]
            #?(:clj  [konserve.utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj  [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer-macros [go-try- <?-]])
            [replikativ.logging :as log]))

(defn- check-diffable!
  "Refuse a pair this cannot answer for, naming the reason, before any IO."
  [db-before db-after op]
  (let [idx-b (get-in db-before [:config :index])
        idx-a (get-in db-after [:config :index])]
    ;; A view — `as-of`, `since`, `history`, `filter` — wraps an origin db and
    ;; has NO indexes of its own, so `(:eavt view)` is nil. Diffing two of them
    ;; would find no trees to compare and answer "nothing changed", which is the
    ;; worst shape a wrong answer can take. Refuse by the property rather than by
    ;; listing the four wrapper types, so a fifth is refused too.
    (when-not (and (some? (:eavt db-before)) (some? (:eavt db-after)))
      (log/raise (str "diff needs plain database VALUES, not views. A database "
                      "returned by as-of / since / history / filter wraps another "
                      "database and holds no indexes of its own, so there is "
                      "nothing to compare — and comparing them would report no "
                      "changes rather than fail. Diff the underlying db values, "
                      "e.g. two snapshots of `@conn` or two roots from the commit "
                      "graph.")
                 {:error :diff/not-a-database :op op
                  :before-has-index? (some? (:eavt db-before))
                  :after-has-index? (some? (:eavt db-after))}))
    (when-not (= :datahike.index/persistent-set idx-a idx-b)
      (log/raise "diff needs :datahike.index/persistent-set on both databases"
                 {:error :diff/unsupported-index :op op
                  :index-before idx-b :index-after idx-a}))
    (when (> (long (:max-tx db-before)) (long (:max-tx db-after)))
      (log/raise "db-before is newer than db-after — the arguments are the wrong way round"
                 {:error :diff/reversed :op op
                  :max-tx-before (:max-tx db-before) :max-tx-after (:max-tx db-after)}))))

(defn- gc-error
  "The failure a missing index node almost always is, as a VALUE.

   Not thrown from inside the walk, and not caught there either: a `try/catch`
   nested inside `go-try-` breaks ClojureScript's `go` macroexpansion
   (\"Keyword cannot be cast to IObj\" — `go-try-` already contributes its own
   `catch`, and core.async does not survive the nesting). So the walk stays
   free of exception handling and the translation happens at the BOUNDARY, in
   `hint-failures`, which is a plain function on both runtimes."
  [db-before db-after e]
  (ex-info (str "Could not read an index node while diffing. The older database's "
                "nodes have most likely been garbage collected: `d/gc-storage` "
                "reclaims exactly the nodes that the newer database no longer "
                "references, which are the nodes a diff against it must read. "
                "Diff against a database you have not collected past.")
           {:error :diff/garbage-collected
            :max-tx-before (:max-tx db-before)
            :max-tx-after (:max-tx db-after)
            :cause e}))

(defn- hint-failures
  "Run `f`, translating a read failure into `gc-error`.

   Sync: catch and re-throw. Async: the result is a channel and konserve's
   convention is that errors travel as VALUES on it, so the translation is a
   `take!`/`put!` hop rather than a catch — no `go` block, hence no macro
   trouble on either runtime."
  [sync? db-before db-after f]
  (if sync?
    (try (f)
         (catch #?(:clj Exception :cljs js/Error) e
           (if (:error (ex-data e)) (throw e) (throw (gc-error db-before db-after e)))))
    (let [out (async/promise-chan)]
      (async/take! (f)
                   (fn [v]
                     (async/put! out
                                 (if (and (instance? #?(:clj Exception :cljs js/Error) v)
                                          (not (:error (ex-data v))))
                                   (gc-error db-before db-after v)
                                   v))))
      out)))

(defn- distinct-datoms
  "Dedupe on the FULL `[e a v tx added]` tuple, preserving order.

   `distinct` is wrong here and quietly so: Datom equality is `[e a v]` only —
   `equiv-datom` compares nothing else and `hash-datom` hashes nothing else — so
   `distinct` collapses an assertion together with its own retraction, and
   collapses an assertion in one transaction with its retraction in a later one.
   Measured: an entity asserted and retracted inside one transaction lost two of
   its three datoms, and the tx that asserted the superseded value lost its
   retraction to the tx that later removed it.

   The two trees genuinely do overlap — a card-one upsert writes the new
   assertion to both the current and the temporal index — so the dedupe is
   needed; it just has to be on the whole datom."
  [ds]
  (second (reduce (fn [[seen out] d]
                    (let [k [(:e d) (:a d) (:v d) (:tx d) (:added d)]]
                      (if (contains? seen k)
                        [seen out]
                        [(conj seen k) (conj out d)])))
                  [#{} []]
                  ds)))

(defn- index-pair
  "The `[before after]` trees for one index key, or nil when either side lacks
   it. A history-free database has no temporal trees at all — absent, not empty
   — so this must not invent them."
  [db-before db-after k]
  (let [a (get db-before k)
        b (get db-after k)]
    (when (and (some? a) (some? b)) [a b])))

(defn diff
  "Datoms the CURRENT index of `db-after` holds that `db-before` did not, and
   vice versa.

   Returns `{:added [datom ...] :removed [datom ...]}`, in index order, or a
   channel yielding it under `{:sync? false}`.

   `:removed` is what left the current index — retracted or superseded. Those
   datoms carry the transaction that ASSERTED them, so do not read a `:removed`
   datom's `:tx` as the time it went away; use `tx-range` for that.

   Both databases must come from the same lineage and use persistent-set
   indexes. See the namespace docstring on garbage collection."
  ([db-before db-after] (diff db-before db-after {}))
  ([db-before db-after {:keys [sync?] :or {sync? #?(:clj true :cljs false)} :as opts}]
   (check-diffable! db-before db-after :diff)
   (let [opts (assoc opts :sync? sync?)]
     (hint-failures
      sync? db-before db-after
      (fn []
        (async+sync
         sync? *default-sync-translation*
         (go-try-
          ;; The parking must be LEXICALLY inside the `go` block -- a `go` state
          ;; machine cannot park across a function call -- so the cljs channel
          ;; adaptation is spelled out here rather than hidden in a helper. On the
          ;; JVM `diff-index` returns a value and there is nothing to park on.
          (or (when-let [[a b] (index-pair db-before db-after :eavt)]
                #?(:clj  (dip/diff-index a b opts)
                   :cljs (if sync?
                           (dip/diff-index a b opts)
                           (<?- (dip/diff-index a b opts)))))
              {:added [] :removed []}))))))))

(defn tx-range
  "The transactions between `db-before` (exclusive) and `db-after` (inclusive),
   oldest first.

   Returns `[{:t <tx-eid> :data [datom ...]} ...]`, or a channel yielding it
   under `{:sync? false}`. `:data` is the transaction's datoms — assertions and
   retractions both, with `:added` distinguishing them — which is the same shape
   a `d/transact` report's `:tx-data` carries, and is verified equal to it.

   Requires `:keep-history? true`; see the namespace docstring for why a
   history-free database cannot attribute a retraction to a transaction.

   Not lazy: the window is materialized. It is bounded by the SIZE OF THE DELTA
   rather than by the database, which is the point, but a caller diffing across
   a year of writes should expect a year of writes in memory."
  ([db-before db-after] (tx-range db-before db-after {}))
  ([db-before db-after {:keys [sync?] :or {sync? #?(:clj true :cljs false)} :as opts}]
   (check-diffable! db-before db-after :tx-range)
   (when-not (and (dbi/-keep-history? db-before) (dbi/-keep-history? db-after))
     (log/raise (str "tx-range needs :keep-history? true. Without history a retraction "
                     "leaves no record of the transaction that made it — the datom that "
                     "vanished carries the tx that asserted it. Use `diff` for what "
                     "changed, without per-transaction attribution.")
                {:error :diff/history-required
                 :max-tx-before (:max-tx db-before) :max-tx-after (:max-tx db-after)}))
   (let [opts (assoc opts :sync? sync?)
         lo   (long (:max-tx db-before))
         hi   (long (:max-tx db-after))]
     (hint-failures
      sync? db-before db-after
      (fn []
        (async+sync
         sync? *default-sync-translation*
         (go-try-
          ;; BOTH trees. Neither alone is complete: a datom asserted and retracted
          ;; inside the window never reaches the current index, and one merely
          ;; superseded leaves it without recording when it left.
          (let [cur  (when-let [[a b] (index-pair db-before db-after :eavt)]
                       #?(:clj  (dip/diff-index a b opts)
                          :cljs (if sync?
                                  (dip/diff-index a b opts)
                                  (<?- (dip/diff-index a b opts)))))
                temp (when-let [[a b] (index-pair db-before db-after :temporal-eavt)]
                       #?(:clj  (dip/diff-index a b opts)
                          :cljs (if sync?
                                  (dip/diff-index a b opts)
                                  (<?- (dip/diff-index a b opts)))))]
            (->> (concat (:added cur) (:added temp))
                 ;; the temporal index also takes new assertions from card-one
                 ;; upserts, so it is not "the retracted things" and this window
                 ;; filter is load-bearing, not cosmetic
                 (filter (fn [d] (let [t (long (:tx d))] (and (> t lo) (<= t hi)))))
                 distinct-datoms
                 (group-by :tx)
                 (sort-by key)
                 (mapv (fn [[t ds]] {:t t :data (vec ds)})))))))))))
