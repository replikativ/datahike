(ns ^:no-doc datahike.migrate.ids
  "Entity- and transaction-id remapping for import, as a PRE-PASS.

   ## Why a pre-pass

   `db.transaction/transact-entities-directly` builds the mapping incrementally:
   an id is allocated the first time it is seen, while datoms are being written.
   That is a fold — the mapping is not known until the import is over — and three
   things we want are blocked by it:

   1. **Bulk index build.** Building a PSS tree from sorted input needs the FINAL
      ids before sorting, because the sort order is over those ids. A fold cannot
      provide them.
   2. **Resumable import.** `migrate/import-db` refuses a non-empty target with
      \"import is not resumable — recreate and restart\", because a partial import
      leaves ids allocated and re-running would allocate different ones. A mapping
      computed up front is deterministic, so progress can be recorded against it.
   3. **Import into a populated database.** Same reason as (2).

   This namespace computes the whole mapping in one pass over the dump, before a
   single datom is written. The second pass then becomes `apply-mapping`, a PURE
   function of one record — which is what makes it sortable, parallelisable, and
   suitable as bulk-builder input.

   ## What it does NOT change

   Memory. `migrate/estimate-import-memory` already calls the O(entities) id map
   \"the dominant, unavoidable term\"; this relocates that cost rather than removing
   it. What it buys is that the map is complete before the writing starts.

   ## Portability

   Deliberately `.cljc` and free of IO: the caller supplies a reducing function
   over the dump's records, so this works over a file, a konserve store, or a
   channel. It is the piece of the import path with no platform coupling at all."
  (:require [datahike.schema :as ds]))

(defn- ref-attr?
  "Is `a` a `:db.type/ref` in `schema`? The dump carries attributes as idents, so
   this is a schema lookup rather than anything db-dependent."
  [schema a]
  (= :db.type/ref (:db/valueType (get schema a))))

(defn build-mapping
  "Scan every record once and return the COMPLETE id mapping.

     {:eids {old new} :tids {old new} :next-eid n :next-tx n}

   `system-entities` are seeded to THEMSELVES: a ref to a system entity must be
   translated to the target's own id, never reallocated (#508/#531). `next-eid`
   and `next-tx` start above the target's current maxima, which is what makes a
   populated target safe — every allocated id is above anything already there.

   `reduce-records` is `(fn [rf init] -> acc)`, the same shape
   `migrate/reduce-dump-records` and `migrate.store/reduce-records` already have.

   Allocation is FIRST-SEEN-WINS in record order. Since a dump is written in a
   fixed order, the mapping is a deterministic function of (dump, target maxima)
   — which is the property resumability rests on.

   Three id positions matter, and missing any one silently corrupts a restore:

     e  entity, except on a tx-entity datom where e IS the transaction
     t  transaction, always
     v  when the attribute is a ref — a ref value may point FORWARD to an entity
        whose own datoms appear later, so it must allocate on sight rather than
        waiting to encounter it as an `e`"
  [{:keys [schema system-entities max-eid max-tx]} reduce-records]
  (let [sys (set system-entities)]
    (reduce-records
     (fn [acc record]
       (let [[e a v t _op] record
             meta? (ds/meta-attr? a)
             alloc-e (fn [acc id]
                       (if (or (contains? (:eids acc) id) (contains? sys id))
                         acc
                         (-> acc
                             (assoc-in [:eids id] (:next-eid acc))
                             (update :next-eid inc))))
             acc (if (contains? (:tids acc) t)
                   acc
                   (-> acc
                       (assoc-in [:tids t] (:next-tx acc))
                       (update :next-tx inc)))
             ;; a tx-entity datom's `e` is its transaction, already handled above
             acc (if meta? acc (alloc-e acc e))
             acc (if (and (ref-attr? schema a) (number? v)) (alloc-e acc v) acc)]
         acc))
     {:eids (into {} (map (fn [e] [e e])) sys)
      :tids {}
      :next-eid (inc (long (or max-eid 0)))
      :next-tx (inc (long (or max-tx 0)))})))

(defn apply-mapping
  "Rewrite one dump record's ids. PURE — no db, no allocation, no order
   dependence, which is the whole point of the pre-pass.

   Unmapped ids pass through unchanged rather than throwing: a system entity is
   seeded to itself, and a value that merely looks like an id (a `:db.type/long`
   that happens to be 4) must not be touched. The guard is the schema, not the
   shape of the number."
  [{:keys [eids tids]} schema record]
  (let [[e a v t op] record
        e' (if (ds/meta-attr? a) (get tids e e) (get eids e e))
        v' (if (and (ref-attr? schema a) (number? v)) (get eids v v) v)
        t' (get tids t t)]
    [e' a v' t' op]))

(defn identity-mapping?
  "True when the mapping leaves every id alone — the ordinary case for an import
   into a freshly created database, where allocation happens to reproduce the
   source's own ids. Worth knowing because it means a bulk build can skip the
   rewrite pass entirely."
  [{:keys [eids tids]}]
  (and (every? (fn [[k v]] (= k v)) eids)
       (every? (fn [[k v]] (= k v)) tids)))
