(ns datahike.reference
  "Cross-database references over datahike's distributed index space.

   Datahike deployments legitimately span MANY databases — per-domain,
   per-tenant, per-permission-boundary — synchronized across peers by
   their store `:id` (a UUID; mandatory in every store config). A
   reference that crosses a database boundary can never be an entity id
   (eids are internal to one index) — it must be VALUE-level:

     (db-id, selector, temporal)

   - db-id     the target store's `:id` — the same logical database on
               any peer that replicates it.
   - selector  a LOOKUP REF `[attr value]` on a `:db/unique` attribute —
               datahike's native value-level entity addressing.
   - temporal  which version of that database:
                 nil               the live head (a LIVING reference —
                                   follows edits; navigation, mentions)
                 {:tx 536871113}   `as-of` a transaction id
                 {:date #inst …}   `as-of` a point in time
                 {:branch \"exp\"}   a branch head
               A pinned temporal is a RECORD reference — immutable,
               resolvable forever on stores with `:keep-history? true`;
               use it for provenance, citation, audit.

   The URI serialization (for text, hyperlinks, logs, export):

     dh://<db-id>/<attr>/<value>[@<temporal>]

     dh://5d7…c1/entity%2Fuuid/0830…9e                      ; living
     dh://5d7…c1/S.Page%2Ftitle/str:Roadmap@tx:536871113    ; record
     dh://5d7…c1/entity%2Fuuid/0830…9e@branch:exploration-2 ; branch head

   Attr keywords are URL-encoded whole (`entity%2Fuuid` ≙ :entity/uuid).
   Values carry an optional type tag — `uuid:` `str:` `long:` `kw:` —
   defaulting to uuid-if-it-parses-else-string.

   Related-work correspondence (deliberately rhymed with, not adopted):
   RDF named graph ≙ db-id, resource IRI ≙ selector, predicate ≙ the
   `:dh.ref/type` of a reified reference (below). Export is mechanical.

   ## Reified references

   Inside a database, model an OUTGOING cross-db link as a small entity
   (`ref-schema`) rather than a URI string, so links stay queryable in
   datalog by type/target without string parsing:

     {:dh.ref/db #uuid \"…\" :dh.ref/attr :entity/uuid
      :dh.ref/value \"0830…9e\" :dh.ref/type :derived-from}

   `reference->tx-map` / `tx-map->reference` convert. Within-database
   links should remain plain `:db.type/ref` attributes — this namespace
   is only for crossing stores.

   ## Resolution

   Connection acquisition is deployment-specific (which peer, which
   grants), so `resolve-reference` takes a `connect-fn`:
   `(fn [db-id {:keys [branch]}] conn-or-db-or-nil)`. The temporal is
   applied here via `as-of`. A nil connect result (unknown or ungranted
   store) resolves to nil — a dangling-but-typed reference, which is the
   correct behaviour at a permission boundary."
  (:refer-clojure :exclude [resolve])
  (:require [clojure.string :as str]
            [datahike.api :as d])
  #?(:clj (:import [java.net URLEncoder URLDecoder])))

;; ============================================================================
;; Reference value
;; ============================================================================

(defrecord Reference [db-id attr value temporal])

(defn reference
  "Construct a Reference. `lookup-ref` is `[unique-attr value]`;
   `temporal` one of nil, {:tx long}, {:date inst}, {:branch string}."
  ([db-id lookup-ref] (reference db-id lookup-ref nil))
  ([db-id [attr value] temporal]
   {:pre [(uuid? db-id) (keyword? attr)]}
   (->Reference db-id attr value temporal)))

(defn lookup-ref
  "The datahike lookup ref `[attr value]` of a Reference."
  [{:keys [attr value]}]
  [attr value])

;; ============================================================================
;; URI encoding
;; ============================================================================

(defn- url-encode [s]
  #?(:clj  (URLEncoder/encode (str s) "UTF-8")
     :cljs (js/encodeURIComponent (str s))))

(defn- url-decode [s]
  #?(:clj  (URLDecoder/decode (str s) "UTF-8")
     :cljs (js/decodeURIComponent (str s))))

(defn- encode-attr [attr]
  (url-encode (subs (str attr) 1)))          ; :entity/uuid → "entity%2Fuuid"

(defn- decode-attr [s]
  (keyword (url-decode s)))

(defn- encode-value [v]
  (cond
    (uuid? v)    (str v)                     ; uuids are the untagged default
    (string? v)  (str "str:" (url-encode v))
    (int? v)     (str "long:" v)
    (keyword? v) (str "kw:" (url-encode (subs (str v) 1)))
    :else (throw (ex-info "Unsupported reference value type"
                          {:value v :type (type v)}))))

(defn- parse-uuid* [s]
  #?(:clj  (try (java.util.UUID/fromString s) (catch Exception _ nil))
     :cljs (when (re-matches #"[0-9a-fA-F-]{36}" s) (uuid s))))

(defn- decode-value [s]
  (cond
    (str/starts-with? s "str:")  (url-decode (subs s 4))
    (str/starts-with? s "long:") #?(:clj (Long/parseLong (subs s 5))
                                    :cljs (js/parseInt (subs s 5) 10))
    (str/starts-with? s "kw:")   (keyword (url-decode (subs s 3)))
    :else (or (parse-uuid* s) (url-decode s))))

(defn- inst-str [d]
  #?(:clj (str (.toInstant ^java.util.Date d)) :cljs (.toISOString d)))

(defn- encode-temporal [{:keys [tx date valid branch]}]
  (let [parts (cond-> []
                tx     (conj (str "tx:" tx))
                date   (conj (str "date:" (inst-str date)))
                valid  (conj (str "valid:" (inst-str valid)))
                branch (conj (str "branch:" (url-encode branch))))]
    (when (seq parts) (str/join "," parts))))

(defn- parse-inst [s]
  #?(:clj (java.util.Date/from (java.time.Instant/parse s)) :cljs (js/Date. s)))

(defn- decode-temporal [s]
  (when (seq s)
    (reduce
     (fn [m part]
       (cond
         (str/starts-with? part "tx:")     (assoc m :tx #?(:clj (Long/parseLong (subs part 3))
                                                           :cljs (js/parseInt (subs part 3) 10)))
         (str/starts-with? part "date:")   (assoc m :date (parse-inst (subs part 5)))
         (str/starts-with? part "valid:")  (assoc m :valid (parse-inst (subs part 6)))
         (str/starts-with? part "branch:") (assoc m :branch (url-decode (subs part 7)))
         :else (throw (ex-info "Unknown temporal qualifier" {:temporal part}))))
     {}
     (str/split s #","))))

(defn render
  "Reference → `dh://…` URI string."
  [{:keys [db-id attr value temporal] :as _ref}]
  (str "dh://" db-id "/" (encode-attr attr) "/" (encode-value value)
       (when-let [t (encode-temporal temporal)] (str "@" t))))

(defn parse
  "`dh://…` URI string → Reference. Throws ex-info on malformed input."
  [s]
  (let [[_ db-id attr-s rest-s]
        (or (re-matches #"dh://([0-9a-fA-F-]{36})/([^/]+)/(.+)" (str s))
            (throw (ex-info "Malformed dh:// reference"
                            {:uri s :expected "dh://<db-id>/<attr>/<value>[@<temporal>]"})))
        [value-s temporal-s] (str/split rest-s #"@" 2)]
    (->Reference (parse-uuid* db-id)
                 (decode-attr attr-s)
                 (decode-value value-s)
                 (decode-temporal temporal-s))))

;; ============================================================================
;; Reified reference entities
;; ============================================================================

(def ref-schema
  "Schema for storing OUTGOING cross-database references as first-class,
   datalog-queryable entities. `:dh.ref/type` is the link predicate —
   application-defined (`:mentions`, `:summarizes`, `:derived-from`, …).

   NEW databases have these pre-installed in the system schema (they
   graduated like `:db.valid/*` / `:db.secondary/*`) — transact `:dh.ref/*`
   directly, no schema declaration needed. Keep this def for stores
   created before the graduation: transact it once there."
  [{:db/ident :dh.ref/db
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/doc "Target database (store :id)"}
   {:db/ident :dh.ref/attr
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Unique attribute of the target lookup ref"}
   {:db/ident :dh.ref/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Lookup-ref value, canonically encoded (encode-value)"}
   {:db/ident :dh.ref/temporal
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Temporal qualifier (encode-temporal); absent = live head"}
   {:db/ident :dh.ref/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Link predicate (application vocabulary)"}])

(defn reference->tx-map
  "Reference → entity map for transacting (merge into an entity of your
   own or transact standalone and point at it with a :db.type/ref)."
  ([ref] (reference->tx-map ref nil))
  ([{:keys [db-id attr value temporal]} type]
   (cond-> {:dh.ref/db db-id
            :dh.ref/attr attr
            :dh.ref/value (encode-value value)}
     temporal (assoc :dh.ref/temporal (encode-temporal temporal))
     type     (assoc :dh.ref/type type))))

(defn tx-map->reference
  "Pulled `:dh.ref/*` entity map → Reference."
  [{:dh.ref/keys [db attr value temporal]}]
  (->Reference db attr (decode-value value) (decode-temporal temporal)))

;; ============================================================================
;; Resolution
;; ============================================================================

(defn- apply-temporal
  ;; Composition order per the valid-time API doc: wrap as-of (tx-time
  ;; horizon) FIRST, valid-at (valid-time point) OUTERMOST.
  [db {:keys [tx date valid]}]
  (cond-> db
    tx    (d/as-of tx)
    date  (d/as-of date)
    valid (d/valid-at valid)))

(defn- ->db
  "connect-fn result (conn or db value) + temporal → the db to read."
  [conn-or-db temporal]
  (let [db* (if #?(:clj (instance? clojure.lang.IDeref conn-or-db)
                   :cljs (satisfies? cljs.core/IDeref conn-or-db))
              @conn-or-db
              conn-or-db)]
    (apply-temporal db* temporal)))

(defn- unique-attr?
  [db attr]
  (boolean (get-in (d/schema db) [attr :db/unique])))

(defn- indexed-attr?
  [db attr]
  (let [a (get (d/schema db) attr)]
    (boolean (or (:db/unique a) (:db/index a)))))

(defn- selector-eids
  "Entity ids carrying `value` on `attr`. Datahike's AVET index is
   SELECTIVE (only :db/unique / :db/index attrs live in it), so:
   indexed attrs -> a direct AVET slice (cheap, no query engine);
   everything else -> an AEVT attribute scan filtered on value — linear
   in the attribute's datom count, which is exactly why non-unique
   resolution is opt-in rather than the default."
  [db attr value]
  (if (indexed-attr? db attr)
    (mapv :e (d/datoms db :avet attr value))
    (into [] (comp (filter #(= (:v %) value)) (map :e))
          (d/datoms db :aevt attr))))

(defn resolve-all
  "The full resolution fiber of `ref`: `{:db <db> :eids [eid …]}`, or nil
   when the target database is unavailable/ungranted. Any selector attr,
   unique or not — this is the honest API for non-unique selectors."
  [{:keys [db-id temporal] :as ref} connect-fn]
  (when-let [conn-or-db (connect-fn db-id (select-keys temporal [:branch]))]
    (let [db (->db conn-or-db temporal)]
      {:db db :eids (selector-eids db (:attr ref) (:value ref))})))

(defn resolve-reference
  "Resolve `ref` to `{:db <db-value> :eid <eid>}`, or nil when the target
   database is unavailable/ungranted or the entity absent (a dangling
   reference — display as such, never an error).

   STRICT by default: the selector attr must be declared `:db/unique` in
   the target schema, so the database — not luck — guarantees the
   reference is single-valued; otherwise throws ex-info. To resolve on a
   non-unique attr, opt in EXPLICITLY with `{:ambiguous :first}` (takes
   the first AVET match) or use `resolve-all` for the whole fiber. The
   explicit opt-in keeps the relaxed convenience out of the default
   contract, so it can never silently under-deliver.

   `connect-fn`: `(fn [db-id {:keys [branch]}] conn-or-db-or-nil)` —
   deployment-supplied (peer registry, grant checks, branch selection).
   The `:tx`/`:date` temporal is applied via `as-of`. Lookups are direct
   AVET index slices (no query engine)."
  ([ref connect-fn] (resolve-reference ref connect-fn {}))
  ([{:keys [db-id temporal] :as ref} connect-fn {:keys [ambiguous]}]
   (when-let [conn-or-db (connect-fn db-id (select-keys temporal [:branch]))]
     (let [db (->db conn-or-db temporal)]
       (when-not (or (unique-attr? db (:attr ref)) (= ambiguous :first))
         (throw (ex-info "Selector attr is not :db/unique — a reference must be single-valued"
                         {:attr (:attr ref) :db-id db-id
                          :hint "declare :db/unique on the attr, pass {:ambiguous :first}, or use resolve-all"})))
       (when-let [eid (first (selector-eids db (:attr ref) (:value ref)))]
         {:db db :eid eid})))))
