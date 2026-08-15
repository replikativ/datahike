(ns datahike.migrate.datomic
  "Datomic Pro as a datahike migration SOURCE and SINK. **Experimental.**

   Nothing in datahike requires this namespace. It requires `datomic.api`, so it
   loads only when `com.datomic/peer` is on the classpath — which is the point:
   datahike takes no Datomic dependency, and a user who has one gets migration
   in both directions by requiring this.

   ## Scope: Pro, via the PEER api

     (require '[datahike.migrate.datomic :as dtm])
     (dtm/import-from-datomic! dh-conn datomic-conn)   ; Datomic -> datahike
     (dtm/export-to-datomic!   @dh-conn datomic-conn)  ; datahike -> Datomic

   The peer API (`datomic.api`) is **Pro-only**. Datomic Cloud and Datomic Local
   speak the CLIENT api (`datomic.client.api`), which has no `d/log` and
   therefore no way to read the transaction history this source is built on.
   Supporting them means a separate namespace and a different history strategy;
   it is not a matter of swapping the require.

   ## The seam

   Both directions go through `datahike.migrate`'s record seam — the same one
   dumps use — so a Datomic migration inherits the batcher, the id mapping, the
   verification and the error attribution rather than reimplementing them. The
   record is `[e a v t op]`.

   ## Identity: what survives and what cannot

   Measured, not assumed (`.internal/datomic-prototype/FINDINGS.md` has the
   probes). Two hard limits shape everything here.

   **Datomic's ids do not fit datahike's.** datahike's `emax`/`txmax` are
   `0x7FFFFFFF` = 2 147 483 647. A Datomic user eid is around 1.76e13 and a tx
   entity id around 1.3e13 — four orders of magnitude above the ceiling. So
   `{:eids :preserve}` is impossible against a Datomic source and is REFUSED
   here rather than silently downgraded. Worse than impossible, it is quiet:
   datahike does not range-check an incoming eid, it simply reallocates, so
   passing Datomic ids through would look like it worked.

   **datahike assigns its own `t`.** Measured through `import-source`: a stream
   stamped `tx0+1000, tx0+1001` lands as `536870913, 536870914` — transaction ids
   are assigned sequentially in source order. The source must still map `t` above
   `tx0`, because a `t` below it is refused as `:import/malformed-record`, but the
   value it chooses is not the value that lands. Hence `datomic-t->dh-t`, which
   exists to satisfy the contract and to order transactions, not to be preserved.

   So: entity and transaction ids are REMAPPED in both directions, transaction
   times and ordering are PRESERVED. A round trip is value-identical and
   history-identical, not id-identical.

   ## Provenance: which datahike transaction was which Datomic one

   Because the ids do not survive, the correspondence is recorded as DATA rather
   than inferred from a numbering convention. Each imported transaction carries
   two datoms ON THE TRANSACTION ENTITY — tx-meta, in the sense that a record
   whose `e` equals its `t` is a datom about the transaction itself:

     :datomic/t        the source `t`      (1004)
     :datomic/tx-eid   the source tx eid   (13194139534316)

   so the question is a query rather than arithmetic:

     (d/q '[:find ?tx :in $ ?t :where [?tx :datomic/t ?t]] @conn 1004)
     ;=> #{[536870917]}

   This needs nothing from datahike that a dump does not already need:
   `transact-entities-directly` builds `(dd/datom new-t a v new-t op)` for a
   record whose `e` is its `t`, so ANY attribute on a transaction entity is
   carried and rewritten to the new id. The source emits the schema for these two
   attributes itself, in the first transaction, exactly as it must for every other
   attribute it uses.

   The alternative — teaching datahike to accept caller-supplied transaction ids
   (a `:tids` option beside `:eids`) — was considered and rejected for this
   purpose. The lever exists (`(get-in migration-state [:tids t] max-tid)`), but
   it would still not reproduce Datomic's ids, since 1.3e13 does not fit in
   `txmax`; it would only choose a different mapping, which is what these two
   attributes already do, queryably and without changing the import contract.

   `{:provenance? false}` turns them off.

   ## Direction-specific limits

   Into Datomic (`export-to-datomic!`):

     * `:db/txInstant` is monotonic-only and must be >= the database basis. A
       FRESH Datomic database has basis instant 1970-01-01, so real historical
       times replay from the first transaction onward provided they ascend.
       Appending into a POPULATED database whose basis is newer than your oldest
       record cannot work, and Datomic says so with `:db.error/past-tx-instant`.
     * Datomic refuses datahike-only schema — `:db/maxLength`, `:db.valid/from`,
       `:db.valid/to`, `:db.secondary/*`, and the value types datahike adds — with
       `:db.error/not-an-entity`. Unknown APPLICATION attributes are fine. This
       namespace strips datahike-only schema by default (`:strip-datahike-schema?`)
       because the alternative is a failed transaction halfway through.
     * Boot attributes cannot be re-asserted: `:db/index` and `:db/txInstant`
       raise `:db.error/datom-cannot-be-altered`. (`:db/doc` is accepted and
       silently redefines, which is why the schema filter is explicit rather
       than trusting Datomic to reject what it should.)
     * Re-asserting an existing `:db.unique/identity` value UPSERTS onto the
       existing entity. Exporting into a non-empty Datomic database therefore
       merges rather than duplicates, which may or may not be what you want.

   From Datomic (`import-from-datomic!`):

     * `:db.type/uri` has no datahike equivalent. Values arrive as `java.net.URI`
       and are carried as their string form, losing the type. See #135.
     * Transaction functions (`:db/fn`, `:db.fn/cas`, …), `:db/fulltext`,
       `:db/lang` and the excision vocabulary have no datahike equivalent and are
       dropped with a warning rather than failing the import.
     * Excised data is, by definition, absent from the log and cannot be
       recovered by any reader."
  (:require [datomic.api :as dt]
            [datahike.migrate :as m]
            [datahike.constants :as const]
            [replikativ.logging :as log])
  (:import [datomic Datom]))

;; ---------------------------------------------------------------------------
;; vocabulary

(def ^:private datomic-only-attrs
  "Datomic vocabulary datahike has no equivalent for. Dropped from a record
   stream flowing INTO datahike, with a warning, rather than failing the import
   on a database that merely happens to use one."
  #{:db/fn :db/code :db/lang :db/fulltext :db/system-tx
    :db.excise/attrs :db.excise/before :db.excise/beforeT
    :db.install/function :db.install/partition :db.install/valueType
    :db.alter/attribute})

(def ^:private install-attrs
  "The `:db.install/*` bookkeeping Datomic writes when schema is transacted.
   datahike derives its schema from `:db/ident` + `:db/valueType` +
   `:db/cardinality` directly, so these carry no information for it and their
   ref values point at attribute entities, which would dangle."
  #{:db.install/attribute :db.install/partition :db.install/valueType
    :db.alter/attribute :db.excise/attrs})

(def ^:private datahike-only-attrs
  "datahike vocabulary Datomic REFUSES with `:db.error/not-an-entity`. Stripped
   from a stream flowing INTO Datomic unless `:strip-datahike-schema? false`."
  #{:db/maxLength :db/noCommit :db/retracted
    :db.valid/from :db.valid/to
    :db.secondary/type :db.secondary/config :db.secondary/attrs
    :db.secondary/only :db.secondary/status :db.secondary/status-type
    :db.secondary/building-since-tx
    :db.meta/attributes})

(def ^:private schema-defining-attrs
  "The attributes whose presence makes a datom part of an attribute's DEFINITION.
   Datomic installs an attribute in the transaction that carries these and refuses
   to use it until the NEXT one, so the sink splits a source transaction on this
   set. datahike has no such rule, which is why a dump can hold both in one."
  #{:db/ident :db/valueType :db/cardinality :db/unique :db/index
    :db/isComponent :db/noHistory :db/doc})

(def ^:private datomic-boot-attrs
  "Attributes whose definition Datomic ships and refuses to have altered
   (`:db.error/datom-cannot-be-altered`). A schema stream must not re-assert
   them. `:db/doc` is deliberately NOT here: Datomic accepts a redefinition of
   it silently, which is exactly why the filter is explicit."
  #{:db/ident :db/valueType :db/cardinality :db/unique :db/index
    :db/txInstant :db/doc :db/isComponent :db/noHistory})

;; ---------------------------------------------------------------------------
;; id mapping

(defn- datomic-t->dh-t
  "Datomic's `t` into datahike's transaction space.

   `t` is small and sequential (the first user transaction is 1000); datahike
   requires a transaction id above `tx0` and refuses anything below it as
   `:import/malformed-record`. Offsetting by `tx0` is order-preserving and
   keeps the gaps, which matters only for readability — datahike reassigns ids
   sequentially anyway (see the ns docstring).

   Refused rather than wrapped on overflow. The headroom is
   `txmax - tx0` = 1 610 612 735 transactions; a database past that cannot be
   imported at all and should learn so here rather than through a silently
   wrong id."
  [t]
  (let [t' (+ const/tx0 t)]
    (when (> t' const/txmax)
      (throw (ex-info (str "Datomic t " t " maps above datahike's txmax: this database has "
                           "more transactions than datahike can represent (" (- const/txmax const/tx0) ").")
                      {:error :datomic/t-overflow :t t :mapped t' :txmax const/txmax})))
    t'))

;; ---------------------------------------------------------------------------
;; Datomic -> records

(defn- ident-map [db]
  (into {} (dt/q '[:find ?e ?i :where [?e :db/ident ?i]] db)))

(defn- ref-attr-idents [db]
  (into #{} (map first)
        (dt/q '[:find ?i :where [?a :db/valueType :db.type/ref] [?a :db/ident ?i]] db)))

(defn- tx->records
  "One Datomic log entry -> datahike records, in `(t, txInstant-first, e, a)` order.

   `a` becomes a keyword ident.

   A ref value is turned into a keyword ONLY when the attribute is one of
   `schema-defining-attrs` — `:db/valueType` -> `:db.type/string`,
   `:db/cardinality` -> `:db.cardinality/one`. Those name Datomic's own
   vocabulary, datahike does not treat them as refs, and a raw eid there would
   dangle.

   Every OTHER ref value stays numeric and is remapped by datahike's importer
   along with `e`, INCLUDING one that points at an entity carrying a
   `:db/ident`. That is the enum case, and keying on \"the target has an ident\"
   instead of \"the attribute is schema\" broke it: `:p/color` -> `:color/red`
   became the keyword, `ids/apply-mapping` guards ref remapping with
   `(number? v)` so the keyword passed straight through, and
   `transact-entities-directly` then allocated a fresh eid for it. Measured:
   every enum reference pointed at a phantom entity with no datoms, a join
   through it returned `#{}`, and the import reported success. The same dump
   exported back to Datomic then aborted with `:db.error/tempid-not-an-entity`,
   because the sink minted a tempid for an eid that had no datoms to assert.

   The tx entity's own eid is rewritten to the mapped `t` so that the
   `:db/txInstant` datom names its own transaction, which is the shape datahike
   expects and `export-db` writes."
  [{:keys [t data]} idents refs dropped {:keys [provenance? extra]}]
  (let [dh-t   (datomic-t->dh-t t)
        tx-eid (dt/t->tx t)
        ;; tx-META: `e` equal to `t` makes these datoms ABOUT the transaction.
        ;; They are what survives the id remapping — see the ns docstring.
        prov   (when provenance?
                 [[dh-t :datomic/t t dh-t true]
                  [dh-t :datomic/tx-eid tx-eid dh-t true]])]
    (->> (concat data)
         (keep (fn [^Datom dm]
                 (let [a (idents (.a dm))]
                   (cond
                     (install-attrs a) nil
                     (datomic-only-attrs a) (do (vswap! dropped conj a) nil)
                     (nil? a) (do (vswap! dropped conj (.a dm)) nil)
                     :else
                     (let [v (.v dm)
                           v (cond
                               ;; a SCHEMA ref -> the ident it names. Keyed on
                               ;; the attribute, not on "the target happens to
                               ;; have an ident" — see the docstring.
                               (and (schema-defining-attrs a) (refs a) (idents v))
                               (idents v)
                               ;; :db.type/uri has no datahike type; carry the string
                               (instance? java.net.URI v) (str v)
                               :else v)
                           e (.e dm)]
                       [(if (= e tx-eid) dh-t e) a v dh-t (.added dm)])))))
         ;; `extra` is the provenance SCHEMA, emitted once with the first
         ;; transaction — a source owes its own schema before the data using it.
         (concat prov (map (fn [r] (assoc r 3 dh-t)) extra))
         ;; :db/txInstant first within the transaction, then by (e, a) — the
         ;; order `migrate.sort/sort-key` documents and the importer assumes.
         (sort-by (fn [[e a _ _ _]] [(if (= a :db/txInstant) 0 1) e (str a)]))
         vec)))

(defn- provenance-schema
  "Schema datoms for the two provenance attributes, at eids that cannot collide.

   NEGATIVE ids, because they only have to be DISTINCT — datahike reallocates
   them like every other source eid — and no Datomic eid is ever negative, so
   this is collision-free by construction rather than by assumption.

   It used to be `(inc (max ident-eid))`, on the reasoning that \"attribute
   entities live in the low hundreds and user entities start around 1.76e13, so
   one past the highest ident eid is free\". That premise is false for any
   database where a USER entity carries a `:db/ident` — a Datomic enum
   (`{:db/ident :color/red}`) is the common case, and the singleton/config
   idiom is another. `ident-map` queries `[?e :db/ident ?i]`, i.e. EVERY ident,
   so one such entity puts `base` inside the occupied user partition.

   Reproduced: with an enum at 17592186045418, `:datomic/tx-eid` was allocated
   17592186045420 — exactly the eid Datomic then handed the next user entity.
   The import reported `:verified? true` over a single entity that was
   simultaneously an attribute definition and a person, and `d/pull` on that
   person returned the attribute's schema. Silent, and it compounds: the merged
   entity now holds both a schema datom and a data datom, which is precisely the
   shape that splits into two entities on the way back out."
  []
  (into []
        (mapcat (fn [[i a vt]]
                  [[i :db/ident a 0 true]
                   [i :db/valueType vt 0 true]
                   [i :db/cardinality :db.cardinality/one 0 true]]))
        [[-1 :datomic/t      :db.type/long]
         [-2 :datomic/tx-eid :db.type/long]]))

(defn- log-t-range
  "The `[from to]` of the transaction log, as Datomic's half-open `tx-range` bounds.

   `to` comes from `dt/basis-t`, which is O(1). The obvious spelling —

       (let [txs (dt/tx-range (dt/log conn) nil nil)]
         [(:t (first txs)) (inc (:t (last txs)))])

   — is a two-fold mistake: it SCANS the whole log to learn its last `t`, and
   because `txs` is bound to a name while `last` walks it, the head is retained
   and the entire log is held in heap. That is the same defect the export path
   carries a `WeakReference` test against; it is easy to write by accident, which
   is why it is spelled out here.

   `first` is still a read, but a lazy seq's first chunk only — nothing is bound,
   so nothing is retained past the call."
  [conn]
  (let [to (dt/basis-t (dt/db conn))]
    (when-let [f (:t (first (dt/tx-range (dt/log conn) nil nil)))]
      [f (inc (long to))])))

(defn source
  "A `chunk-src` over a Datomic connection's transaction log, for `import-source`.

   Descriptors are `t` WINDOWS, not records — `{:from t :to t}` — so the whole
   log is never held. `:read` fetches one window with `dt/tx-range` and converts
   it, which is the IO the seam's `:read` exists to permit.

   Opts:

     :window        transactions per chunk (default 100). A chunk is whole
                    transactions by construction, so tx-alignment is free here.
     :provenance?   (default true) stamp each transaction with `:datomic/t` and
                    `:datomic/tx-eid`, and emit their schema in the first
                    transaction. See the ns docstring.

   The ident map and ref-attribute set are read ONCE, from the current db, so an
   attribute installed mid-log is still resolvable when its earlier datoms are
   read.

   `:read` is RE-ENTRANT, as the seam requires: the provenance schema is keyed to
   the first `t` of the whole log, not to \"the first chunk read\", so reading the
   same window twice yields the same records — `verify` and the index build both
   read chunks more than once."
  ([conn] (source conn {}))
  ([conn {:keys [window provenance?] :or {window 100 provenance? true}}]
   (when-not (pos? window)
     (throw (ex-info "Datomic source :window must be positive."
                     {:error :datomic/bad-window :window window})))
   (let [db      (dt/db conn)
         idents  (ident-map db)
         refs    (ref-attr-idents db)
         dropped (volatile! #{})
         [from to] (or (log-t-range conn) [0 0])
         schema  (when provenance? (provenance-schema))
         chunks  (vec (for [start (range from to window)]
                        {:from start :to (min to (+ start window))}))]
     {:chunks chunks
      :read   (fn [{f :from t :to} _opts]
                (let [rs (into []
                               (mapcat (fn [{:keys [t] :as tx}]
                                         (tx->records tx idents refs dropped
                                                      {:provenance? provenance?
                                                       ;; only with the log's FIRST
                                                       ;; transaction, whichever chunk
                                                       ;; that turns out to be in
                                                       :extra (when (= t from) schema)})))
                               (dt/tx-range (dt/log conn) f t))]
                  (when (seq @dropped)
                    (log/warn :datomic/dropped-attributes
                              {:msg "Datomic vocabulary with no datahike equivalent was dropped"
                               :attributes @dropped}))
                  rs))})))

(defn record-count
  "How many records `source` will yield. A FULL log scan — the cost is real on a
   large database, and it exists because `import-source` fails closed: without an
   `:expected-count` the import must be told `{:verify? false}` explicitly rather
   than be quietly unverified."
  ([conn] (record-count conn {}))
  ([conn opts]
   (let [{:keys [chunks read]} (source conn opts)]
     (transduce (map #(count (read % {}))) + 0 chunks))))

(defn import-from-datomic!
  "Import a Datomic database into datahike `conn`. **Experimental.**

   Entity and transaction ids are remapped; transaction times and ordering are
   preserved (see the ns docstring). `:eids :preserve` is refused — Datomic ids
   do not fit datahike's id space.

   Opts are `import-source`'s, plus `:window`. `:count?` (default true) does the
   extra log scan that makes verification possible; `{:count? false}` skips it and
   then requires `{:verify? false}`, so an unverified import is always something
   the caller asked for."
  ([dh-conn dtm-conn] (import-from-datomic! dh-conn dtm-conn {}))
  ([dh-conn dtm-conn {:keys [count?] :or {count? true} :as opts}]
   (when (= :preserve (:eids opts))
     (throw (ex-info (str "{:eids :preserve} cannot be honoured for a Datomic source: Datomic entity "
                          "ids (~1.76e13) exceed datahike's emax (" const/emax "). datahike does not "
                          "range-check them, it silently reallocates, so preserving them is not "
                          "merely unsupported but unobservable. Use the default :allocate.")
                     {:error :datomic/eids-preserve-unsupported :emax const/emax})))
   (let [src   (source dtm-conn opts)
         n     (when count? (record-count dtm-conn opts))
]
     (m/import-source dh-conn src
                      (merge (dissoc opts :count? :window :provenance?)
                             ;; NO `:max-tx`. It is a drift CHECK — "the restore
                             ;; should land on the same max-tx the source had" —
                             ;; and that is meaningful only where `t` is
                             ;; preserved, which for a Datomic source it never is
                             ;; (datahike compacts; see the ns docstring).
                             ;; Declaring it made every successful import print
                             ;; `max-tx drifted by -3998`, a warning about
                             ;; working as designed. Measured on 3000
                             ;; transactions. The `:datomic/t` provenance datoms
                             ;; carry the real correspondence.
                             {:source-meta (cond-> {:history? true}
                                             n (assoc :expected-count n))})))))

;; ---------------------------------------------------------------------------
;; records -> Datomic

(defn- sink-tx-data
  "One transaction's records -> Datomic tx-data.

   Source eids become tempids on first assertion and resolved ids thereafter, so
   a RETRACTION — which cannot name a tempid — resolves. The tx entity becomes
   \"datomic.tx\", which is how `:db/txInstant` is carried across.

   A retraction whose `[e a]` is also ASSERTED in the same transaction is
   dropped FOR A CARDINALITY-ONE ATTRIBUTE ONLY: Datomic derives that retraction
   itself, so replaying ours too records it twice. Measured against Datomic:
   `[[:db/add e :p/age 31]]` alone yields tx-data carrying both the assertion and
   `[e :p/age 30 false]`, and sending our own retraction as well records it
   twice.

   For CARDINALITY-MANY the same reasoning is false and the filter was silently
   wrong: a retraction of v1 and an assertion of v2 are independent facts, and
   Datomic derives nothing — measured, `[[:db/add e :p/tag \"c\"]]` yields only
   the assertion. Dropping ours left the retracted value ALIVE in the target.
   Measured end to end: source tags `#{y z}`, target `#{x y z}`.

   `many-idents` is learned from the stream, like `ref-idents`. An attribute of
   UNKNOWN cardinality keeps its retraction — the safe direction, since a
   duplicate retraction is a log-fidelity blemish while a dropped one is wrong
   data."
  [group t eids ref-idents many-idents strip?]
  (let [superseded (into #{} (keep (fn [[e a _ _ op]]
                                     (when (and op (not (many-idents a))) [e a])))
                         group)
        ref->      (fn [e] (if (= e t) "datomic.tx" (or (@eids e) (str "e" e))))]
    (into []
          (comp
           (remove (fn [[e a _ _ op]] (and (not op) (superseded [e a]))))
           ;; datahike-only vocabulary Datomic answers with
           ;; `:db.error/not-an-entity`, which would fail the whole transaction.
           (remove (fn [[_ a _ _ _]] (and strip? (datahike-only-attrs a))))
           ;; A datahike schema entity re-asserting a Datomic BOOT attribute's own
           ;; definition — `[:db/index :db/valueType …]` — raises
           ;; `:db.error/datom-cannot-be-altered`. Only the definition OF a boot
           ;; attribute is a problem; USING one (`[:p/name :db/index true]`) is
           ;; ordinary schema and must pass, so this keys on the entity being a
           ;; boot attribute, not on the attribute of the record.
           (remove (fn [[_ a v _ _]]
                     (and strip? (= a :db/ident) (datomic-boot-attrs v))))
           (map (fn [[e a v _ op]]
                  [(if op :db/add :db/retract)
                   (ref-> e)
                   a
                   (if (and (ref-idents a) (number? v)) (ref-> v) v)])))
          group)))

(defn sink
  "A `sink` over a Datomic connection, for `export-to-sink`.

   One Datomic transaction per source transaction, so `:db/txInstant` and the
   causal order carry across. `export-to-sink` hands whole transactions —
   chunks are transaction-aligned — which is exactly what this needs.

   Opts:

     :strip-datahike-schema?  (default true) drop datahike-only vocabulary that
                              Datomic refuses with `:db.error/not-an-entity`.

   Returns `{:transactions n :eids {source-eid datomic-eid}}` from `:close`."
  ([conn] (sink conn {}))
  ([conn {:keys [strip-datahike-schema?] :or {strip-datahike-schema? true}}]
   {:open  (fn [_opts] {:eids (atom {}) :refs (atom #{}) :idents (atom {})
                        :many (atom #{}) :n 0})
    :write (fn [{:keys [eids refs idents many] :as ctx} records]
             ;; learn the schema from the stream, as the dump's own importer does
             (doseq [[e a v _ op] records]
               (when (and op (= a :db/ident))     (swap! idents assoc e v))
               (when (and op (= a :db/valueType) (= v :db.type/ref)) (swap! refs conj e))
               ;; cardinality, for the superseded filter. Learned from the STREAM
               ;; rather than looked up in the target: `sink-tx-data` runs before
               ;; either half is committed, so a target lookup misses on exactly
               ;; the transaction that installs a card-many attribute and writes
               ;; to it — the one where it matters most.
               (when (and op (= a :db/cardinality) (= v :db.cardinality/many))
                 (swap! many conj e)))
             (let [ref-idents  (into #{} (keep @idents) @refs)
                   many-idents (into #{} (keep @idents) @many)]
               (reduce
                (fn [c group]
                  (let [t  (nth (first group) 3)
                        td (sink-tx-data group t eids ref-idents many-idents
                                         strip-datahike-schema?)
                        ;; DATOMIC REQUIRES SCHEMA IN AN EARLIER TRANSACTION than
                        ;; its first use; datahike accepts an attribute and the
                        ;; datom using it in ONE transaction, and `export-db`
                        ;; therefore emits them together. Measured: replaying such
                        ;; a transaction verbatim raises
                        ;; `:db.error/not-an-entity Unable to resolve entity: :datomic/t`.
                        ;;
                        ;; So a source transaction that installs schema becomes TWO
                        ;; Datomic transactions, schema first. The extra one carries
                        ;; no `:db/txInstant` of its own, so it is stamped by
                        ;; Datomic's clock rather than the source's — the schema's
                        ;; installation time is not preserved, its content is.
                        schema? (fn [[_ _ a]] (contains? schema-defining-attrs a))
                        [sch dat] [(filterv schema? td) (filterv (complement schema?) td)]
                        ;; Split only when this transaction actually USES an
                        ;; attribute it INSTALLS. Splitting on the mere presence
                        ;; of schema is stricter than the rule above, and the
                        ;; difference is not free: a Datomic SOURCE cannot
                        ;; produce a transaction that carries both, so every one
                        ;; of its schema transactions was being split for
                        ;; nothing. Measured on a source with two schema
                        ;; transactions: 4 came back as 6, of which one extra
                        ;; was real and one was this.
                        ;;
                        ;; The real one is datahike's own doing, not the source's
                        ;; — `source` emits the provenance schema with the log's
                        ;; FIRST transaction (`:extra (when (= t from) schema)`)
                        ;; and stamps `:datomic/t` on that same transaction.
                        ;; datahike accepts that; Datomic does not, which is the
                        ;; `:db.error/not-an-entity :datomic/t` the split was
                        ;; added for. That case still splits, because `:datomic/t`
                        ;; is installed and used here.
                        installed (into #{} (keep (fn [[_ _ a v]]
                                                    (when (= a :db/ident) v))
                                                  sch))
                        needs-split? (and (seq sch)
                                          (boolean (some (fn [[_ _ a]]
                                                           (contains? installed a))
                                                         dat)))
                        [sch dat] (if needs-split? [sch dat] [[] td])
                        ;; The split transaction needs the SOURCE's time too.
                        ;; Without it Datomic stamps the schema transaction with
                        ;; the wall clock, which advances the basis past every
                        ;; historical instant that follows — measured:
                        ;; `:db.error/past-tx-instant … 2021-01-01 is older than
                        ;; database basis`, on a FRESH database, caused entirely by
                        ;; the split.
                        ;;
                        ;; The SAME instant, not one millisecond earlier. Datomic
                        ;; accepts an equal `:db/txInstant` and rejects an earlier
                        ;; one (measured both ways), and ordering between the two
                        ;; halves is carried by `t`, not by the instant. The `dec`
                        ;; this replaces made the schema half's time approximate
                        ;; for no benefit, and was reachable as a FAILURE: two
                        ;; source transactions sharing a millisecond put the
                        ;; synthetic instant before the previous transaction, and
                        ;; the export aborted mid-way with `:db.error/past-tx-instant`
                        ;; leaving the target half-migrated. datahike's own
                        ;; allocator is strictly monotonic, but caller-supplied
                        ;; `:db/txInstant` in `:tx-meta` overrides it, and an
                        ;; imported database inherits whatever ties its source had.
                        inst (some (fn [[_ _ a v]] (when (= a :db/txInstant) v)) dat)
                        sch  (cond-> sch
                               (and (seq sch) inst)
                               (conj [:db/add "datomic.tx" :db/txInstant inst]))
                        commit! (fn [c' d]
                                  (if (empty? d)
                                    c'
                                    (let [{:keys [tempids]} @(dt/transact conn d)]
                                      (doseq [[k id] tempids]
                                        (when (string? k)
                                          (if (= k "datomic.tx")
                                            (swap! eids assoc t id)
                                            (swap! eids assoc (parse-long (subs k 1)) id))))
                                      (update c' :n inc))))
                        ;; TEMPIDS DO NOT SPAN TRANSACTIONS. `td` was built once,
                        ;; before the split, so both halves carry the same tempid
                        ;; STRING for a source eid — and Datomic resolves the same
                        ;; string in two transactions to two DIFFERENT entities.
                        ;; An entity with a schema datom and a data datom in one
                        ;; source transaction therefore became two entities, with
                        ;; the ident on one and the data on the other, silently.
                        ;; Measured: `{:db/ident :color/red :p/name "Red"}` landed
                        ;; as two entities and `(:p/name (d/entity db :color/red))`
                        ;; was nil.
                        ;;
                        ;; `commit! sch` fills `eids` for everything the schema
                        ;; half created, so the data half is re-resolved against it
                        ;; here — after that commit, not before.
                        resolve-tempid (fn [x]
                                         (if (and (string? x) (not= x "datomic.tx"))
                                           (or (@eids (parse-long (subs x 1))) x)
                                           x))
                        rebuild (fn [d]
                                  (mapv (fn [[op e a v]]
                                          [op (resolve-tempid e) a (resolve-tempid v)])
                                        d))]
                    (let [c (commit! c sch)]
                      ;; rebuilt only when a schema half actually ran; otherwise
                      ;; `dat` is `td` untouched and there is nothing to resolve.
                      (commit! c (if (seq sch) (rebuild dat) dat)))))
                ctx
                (partition-by #(nth % 3) records))))
    :close (fn [{:keys [n eids]}] {:transactions n :eids @eids})}))

(defn export-to-datomic!
  "Export a datahike database into a Datomic connection. **Experimental.**

   `db-or-conn` is a datahike db or connection; `dtm-conn` a Datomic connection,
   which should be FRESH — see the ns docstring on `:db/txInstant` monotonicity
   and on unique-identity upsert into a populated database.

   Opts are `export-to-sink`'s (`:history?` `:chunk-size` `:xform` …) plus
   `:strip-datahike-schema?`."
  ([db-or-conn dtm-conn] (export-to-datomic! db-or-conn dtm-conn {}))
  ([db-or-conn dtm-conn opts]
   (m/export-to-sink db-or-conn
                     (sink dtm-conn (select-keys opts [:strip-datahike-schema?]))
                     (dissoc opts :strip-datahike-schema?))))
