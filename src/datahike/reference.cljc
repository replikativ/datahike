(ns datahike.reference
  "Cross-database references over datahike's distributed index space.

   Datahike deployments legitimately span MANY databases — per-domain,
   per-tenant, per-permission-boundary — synchronized across peers by
   their store `:id` (a UUID; mandatory in every store config). A
   reference that crosses a database boundary is a triple:

     (db-id, selector, temporal)

   - db-id     the target store's `:id` — the same logical database on
               any peer that replicates it.
   - selector  a LOOKUP REF `[attr value]` on a `:db/unique` attribute —
               datahike's native value-level entity addressing — or a bare
               ENTITY ID (represented as `[:db/id eid]`). An eid is the
               cheapest and most general pointer: a direct EAVT seek, no
               AVET, no schema, works for entities with no unique attr.
               Eids are stable within a logical database (replicas share
               the index, branches share it copy-on-write, datahike never
               reuses eids) but do NOT survive re-materialization
               (export/import, migration-by-retransaction) — value
               selectors do. Physically-bound vs semantically-robust:
               pick per use.
   - temporal  which version of that database:
                 nil               the live head (a LIVING reference —
                                   follows edits; navigation, mentions)
                 {:tx 536871113}   `as-of` a transaction id
                 {:date #inst …}   `as-of` a point in time
                 {:commit #uuid …} an exact content-addressed commit
                                   (via `commit-as-db`) — the most precise
                                   record reference; supersedes :tx/:date
                 {:branch \"exp\"}   a branch head
               A pinned temporal is a RECORD reference — immutable,
               resolvable forever on stores with `:keep-history? true`;
               use it for provenance, citation, audit.

   The URI serialization (for text, hyperlinks, logs, export). The
   temporal is a standard URL query string:

     dh://<db-id>/<attr>/<value>[?<temporal>]

     dh://5d7…c1/entity%2Fuuid/0830…9e                       ; living
     dh://5d7…c1/S.Page%2Ftitle/str:Roadmap?tx=536871113     ; record (as-of tx)
     dh://5d7…c1/entity%2Fuuid/0830…9e?commit=1a2b…          ; record (exact commit)
     dh://5d7…c1/entity%2Fuuid/0830…9e?branch=exploration-2  ; branch head
     dh://5d7…c1/387                                         ; bare entity id

   Attr keywords are URL-encoded whole (`entity%2Fuuid` ≙ :entity/uuid).
   Values carry an optional type tag — `uuid:` `str:` `long:` `kw:` —
   defaulting to uuid-if-it-parses-else-string.

   Related-work correspondence (deliberately rhymed with, not adopted):
   RDF named graph ≙ db-id, resource IRI ≙ selector, predicate ≙ the
   `:dh.ref/type` of a reified reference (below). Export is mechanical.

   ## Reified references

   Inside a database, model an OUTGOING cross-db link as a small entity
   rather than a URI string, so links stay queryable in datalog by
   type/target without string parsing:

     {:dh.ref/db #uuid \"…\" :dh.ref/attr :entity/uuid
      :dh.ref/value \"0830…9e\" :dh.ref/type :derived-from}

   The `:dh.ref/*` attributes live in datahike's system schema (graduated
   like `:db.valid/*` / `:db.secondary/*`), so every store accepts them
   directly — no schema declaration needed. `reference->tx-map` /
   `tx-map->reference` convert. Within-database links should remain plain
   `:db.type/ref` attributes — this namespace is only for crossing stores.

   ## Resolution

   Connection acquisition is deployment-specific (which peer, which
   grants), so `resolve-reference` takes a `connect-fn`:
   `(fn [db-id {:keys [branch]}] conn-or-db-or-nil)`. The temporal is
   applied here via `as-of`. A nil connect result (unknown or ungranted
   store) resolves to nil — a dangling-but-typed reference, which is the
   correct behaviour at a permission boundary."
  (:refer-clojure :exclude [resolve])
  (:require [clojure.string :as str]
            [datahike.api :as d]
            #?(:clj [clojure.edn :as edn] :cljs [cljs.reader :as reader])
            #?(:cljs [goog.crypt.base64 :as gb64]))
  #?(:clj (:import [java.net URLEncoder URLDecoder])))

;; ============================================================================
;; Reference value
;; ============================================================================

(defrecord Reference [db-id attr value temporal])

(defn reference
  "Construct a Reference. `selector` is `[unique-attr value]`, a bare
   entity id (long — becomes `[:db/id eid]`), or `[:db/id eid]`;
   `temporal` a map combining any of {:tx long} {:date inst}
   {:valid inst} {:branch string} {:commit uuid} (nil = live head).
   `:commit` pins an exact content-addressed db value (via `commit-as-db`)
   and supersedes `:tx`/`:date`; `:valid` composes with any of them."
  ([db-id selector] (reference db-id selector nil))
  ([db-id selector temporal]
   {:pre [(uuid? db-id)]}
   (if (number? selector)
     (->Reference db-id :db/id (long selector) temporal)
     (let [[attr value] selector]
       (assert (keyword? attr) "selector attr must be a keyword")
       (->Reference db-id attr value temporal)))))

(defn lookup-ref
  "The datahike lookup ref `[attr value]` of a Reference."
  [{:keys [attr value]}]
  [attr value])

;; ============================================================================
;; URI encoding
;; ============================================================================

(defn- url-encode [s]
  ;; Java's URLEncoder emits `+` for space (the form-urlencoded convention),
  ;; but a dh:// value/attr lives in a URL *path* segment where `+` is a
  ;; literal plus — and CLJS `decodeURIComponent` would not turn it back into a
  ;; space. Normalize to `%20` so encoding is RFC-3986-correct for paths and
  ;; byte-identical across CLJ and CLJS (references are cross-platform).
  #?(:clj  (str/replace (URLEncoder/encode (str s) "UTF-8") "+" "%20")
     :cljs (js/encodeURIComponent (str s))))

(defn- url-decode [s]
  #?(:clj  (URLDecoder/decode (str s) "UTF-8")
     :cljs (js/decodeURIComponent (str s))))

;; A qualified keyword's `/` is kept as a literal path separator so the attr
;; reads hierarchically (`:S.Page/title` → `S.Page/title`, not `S.Page%2Ftitle`).
;; Each `/`-delimited part is still url-encoded, so any *other* special char in
;; the namespace or name is escaped. `parse` recovers the attr as everything
;; between the db-id and the (always `/`-free) value segment.
(defn- encode-attr [attr]
  (str/join "/" (map url-encode (str/split (subs (str attr) 1) #"/"))))

(defn- decode-attr [s]
  (keyword (str/join "/" (map url-decode (str/split s #"/")))))

(defn- parse-uuid* [s]
  #?(:clj  (try (java.util.UUID/fromString s) (catch Exception _ nil))
     :cljs (when (re-matches #"[0-9a-fA-F-]{36}" s) (uuid s))))

(defn- inst-str [d]
  #?(:clj (str (.toInstant ^java.util.Date d)) :cljs (.toISOString d)))

(defn- parse-inst [s]
  #?(:clj (java.util.Date/from (java.time.Instant/parse s)) :cljs (js/Date. s)))

(defn- read-edn [s]
  #?(:clj (edn/read-string s) :cljs (reader/read-string s)))

;; base64url (url-safe alphabet, no padding) so a bytes value drops straight
;; into a URL path segment. CLJ: java.util.Base64; CLJS: goog.crypt.base64 over
;; the Uint8Array that is datahike's bytes representation (schema.cljc).
(defn- b64url-encode [b]
  #?(:clj  (.encodeToString (.withoutPadding (java.util.Base64/getUrlEncoder)) ^bytes b)
     :cljs (gb64/encodeByteArray b (.-WEBSAFE_NO_PADDING gb64/Alphabet))))

(defn- b64url-decode [s]
  #?(:clj  (.decode (java.util.Base64/getUrlDecoder) ^String s)
     :cljs (gb64/decodeStringToUint8Array s)))

(defn- bytes-val? [v]
  #?(:clj  (bytes? v)
     :cljs (and (some? (.-buffer v)) (instance? js/ArrayBuffer (.-buffer v)))))

;; Value ⇄ URL-segment codec. Readable tags for the identity-friendly scalars
;; (uuid untagged; str:/long:/kw:/bool:/inst:). Bytes get b64: (base64url, both
;; platforms) and Float gets a clj-only flt: (CLJS has no Float distinct from
;; double). Everything else — bigint, bigdec, double, symbol, tuple — rides an
;; `edn:` fallback whose pr-str / read-string preserves the exact type,
;; *including bigdec scale*. The payloads of str:/kw:/edn: are url-encoded and
;; the fixed tags use only URL-safe chars, so no encoded value ever contains a
;; raw `/` or `?` to confuse `parse`.
(defn- encode-value [v]
  (cond
    (uuid? v)      (str v)                   ; uuids are the untagged default
    (string? v)    (str "str:" (url-encode v))
    (keyword? v)   (str "kw:" (url-encode (subs (str v) 1)))
    (boolean? v)   (str "bool:" v)
    (inst? v)      (str "inst:" (inst-str v))
    (bytes-val? v) (str "b64:" (b64url-encode v))
    #?@(:clj [(instance? Long v)  (str "long:" v)   ; also carries :db/id eids
              (instance? Float v) (str "flt:" v)]
        :cljs [(and (number? v) (js/Number.isSafeInteger v)) (str "long:" v)])
    :else (str "edn:" (url-encode (pr-str v)))))

(defn- decode-value [s]
  (cond
    (str/starts-with? s "str:")  (url-decode (subs s 4))
    (str/starts-with? s "long:") #?(:clj (Long/parseLong (subs s 5))
                                    :cljs (js/parseInt (subs s 5) 10))
    (str/starts-with? s "kw:")   (keyword (url-decode (subs s 3)))
    (str/starts-with? s "bool:") (= "true" (subs s 5))
    (str/starts-with? s "inst:") (parse-inst (subs s 5))
    (str/starts-with? s "b64:")  (b64url-decode (subs s 4))
    ;; flt: is only emitted on CLJ (CLJS has no distinct Float); a CLJ-produced
    ;; float reference still decodes on CLJS, best-effort, to a plain number.
    (str/starts-with? s "flt:")  #?(:clj (Float/parseFloat (subs s 4))
                                    :cljs (js/parseFloat (subs s 4)))
    (str/starts-with? s "edn:")  (read-edn (url-decode (subs s 4)))
    :else (or (parse-uuid* s) (url-decode s))))

(defn- encode-temporal
  "Temporal map → URL query string (`tx=…&branch=…`), or nil when empty.
   Insts are left as readable ISO (`:` and `-` are query-legal per RFC 3986,
   and `.toInstant` yields a `Z` UTC form with no `+` to mangle); branch is
   url-encoded; commit is a bare UUID."
  [{:keys [tx date valid branch commit]}]
  (let [pairs (cond-> []
                tx     (conj (str "tx=" tx))
                date   (conj (str "date=" (inst-str date)))
                valid  (conj (str "valid=" (inst-str valid)))
                branch (conj (str "branch=" (url-encode branch)))
                commit (conj (str "commit=" commit)))]
    (when (seq pairs) (str/join "&" pairs))))

(defn- decode-temporal
  "URL query string (`tx=…&branch=…`) → temporal map. Unknown keys and a
   non-UUID `commit` throw ex-info."
  [s]
  (when (seq s)
    (reduce
     (fn [m pair]
       (let [[k v] (str/split pair #"=" 2)]
         (case k
           "tx"     (assoc m :tx #?(:clj (Long/parseLong v) :cljs (js/parseInt v 10)))
           "date"   (assoc m :date (parse-inst v))
           "valid"  (assoc m :valid (parse-inst v))
           "branch" (assoc m :branch (url-decode v))
           "commit" (assoc m :commit (or (parse-uuid* v)
                                         (throw (ex-info "commit qualifier must be a UUID"
                                                         {:commit v}))))
           (throw (ex-info "Unknown temporal qualifier" {:temporal pair})))))
     {}
     (str/split s #"&"))))

(defn render
  "Reference → `dh://…` URI string. Eid selectors (`:db/id`) render as a
   single numeric path segment; attr selectors as `<attr>/<value>`."
  [{:keys [db-id attr value temporal] :as _ref}]
  (str "dh://" db-id "/"
       (if (= :db/id attr)
         (str value)
         (str (encode-attr attr) "/" (encode-value value)))
       (when-let [t (encode-temporal temporal)] (str "?" t))))

(defn parse
  "`dh://…` URI string → Reference. A single all-digit path segment is an
   entity-id selector; `<attr>/<value>` is a lookup-ref selector. The
   temporal is a URL query string (`?tx=…&branch=…`). Throws ex-info on
   malformed input."
  [s]
  (if-let [[_ db-id eid-s temporal-s]
           (re-matches #"dh://([0-9a-fA-F-]{36})/([0-9]+)(?:\?(.+))?" (str s))]
    (->Reference (parse-uuid* db-id) :db/id
                 #?(:clj (Long/parseLong eid-s) :cljs (js/parseInt eid-s 10))
                 (decode-temporal temporal-s))
    ;; The value is always the last, `/`-free path segment; the attr is
    ;; whatever lies between the db-id and it (one or more segments, so a
    ;; namespaced keyword keeps its `/`). Temporal follows the first `?`.
    (let [[_ db-id body temporal-s]
          (or (re-matches #"dh://([0-9a-fA-F-]{36})/([^?]+)(?:\?(.+))?" (str s))
              (throw (ex-info "Malformed dh:// reference"
                              {:uri s :expected "dh://<db-id>/(<eid>|<attr>/<value>)[?<temporal>]"})))
          segs    (str/split body #"/")
          value-s (last segs)
          attr-s  (str/join "/" (butlast segs))]
      (when (str/blank? attr-s)
        (throw (ex-info "Malformed dh:// reference — expected <attr>/<value>"
                        {:uri s :expected "dh://<db-id>/(<eid>|<attr>/<value>)[?<temporal>]"})))
      (->Reference (parse-uuid* db-id)
                   (decode-attr attr-s)
                   (decode-value value-s)
                   (decode-temporal temporal-s)))))

;; ============================================================================
;; Reified reference entities
;;
;; The `:dh.ref/*` attributes are installed in datahike's system schema
;; (`datahike.schema/implicit-schema-spec`), so every store accepts them
;; without a schema declaration. `:dh.ref/db` and `:dh.ref/value` are
;; AVET-indexed there for reverse lookups ("all references into database X").
;; ============================================================================

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

(defn- deref-conn
  [conn-or-db]
  (if #?(:clj (instance? clojure.lang.IDeref conn-or-db)
         :cljs (satisfies? cljs.core/IDeref conn-or-db))
    @conn-or-db
    conn-or-db))

(defn- apply-temporal
  ;; Composition order per the valid-time API doc: wrap as-of (tx-time
  ;; horizon) FIRST, valid-at (valid-time point) OUTERMOST.
  [db {:keys [tx date valid]}]
  (cond-> db
    tx    (d/as-of tx)
    date  (d/as-of date)
    valid (d/valid-at valid)))

(defn- ->db
  "connect-fn result (conn, db value, or store) + temporal → the db to
   read, or nil when a `:commit` pin names a commit the store doesn't hold.

   `:commit` is a content-addressed exact db value (loaded via
   `commit-as-db`) and is self-sufficient — it takes precedence over the
   `:tx`/`:date` tx-time pins, which it makes redundant. `:valid` is
   orthogonal (valid-time) and still composes on top.

   `:commit` resolution loads a *different* db value from the store, which is
   synchronous only on CLJ. On CLJS `commit-as-db` is async, so a `:commit`
   pin cannot be resolved through this synchronous API — load the commit db
   yourself (`(commit-as-db conn cid)`) and hand it to `connect-fn`. The
   `:tx`/`:date`/`:branch`/`:valid` temporals resolve synchronously on both
   platforms (they transform an already-connected db)."
  [conn-or-db {:keys [commit valid] :as temporal}]
  (if commit
    #?(:clj  (when-let [base (d/commit-as-db conn-or-db commit)]  ; commit-as-db extracts the store
               (cond-> base valid (d/valid-at valid)))
       :cljs (throw (ex-info "Commit-pinned reference resolution is synchronous-only (CLJ). On CLJS, load the commit db via (commit-as-db conn cid) and pass it through connect-fn."
                             {:error :reference/commit-resolution-unsupported-cljs :commit commit})))
    (apply-temporal (deref-conn conn-or-db) temporal)))

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
  (cond
    ;; bare entity id — direct EAVT seek; no AVET, no schema involved
    (= :db/id attr)
    (if (seq (d/datoms db :eavt value)) [value] [])

    (indexed-attr? db attr)
    (mapv :e (d/datoms db :avet attr value))

    :else
    (into [] (comp (filter #(= (:v %) value)) (map :e))
          (d/datoms db :aevt attr))))

(defn resolve-all
  "The full resolution fiber of `ref`: `{:db <db> :eids [eid …]}`, or nil
   when the target database is unavailable/ungranted. Any selector attr,
   unique or not — this is the honest API for non-unique selectors."
  [{:keys [db-id temporal] :as ref} connect-fn]
  (when-let [conn-or-db (connect-fn db-id (select-keys temporal [:branch]))]
    (when-let [db (->db conn-or-db temporal)]     ; nil = commit pin the store lacks
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
   The `:tx`/`:date` temporal is applied via `as-of`, `:commit` via
   `commit-as-db`. Lookups are direct AVET index slices (no query engine)."
  ([ref connect-fn] (resolve-reference ref connect-fn {}))
  ([{:keys [db-id temporal] :as ref} connect-fn {:keys [ambiguous]}]
   (when-let [conn-or-db (connect-fn db-id (select-keys temporal [:branch]))]
     (when-let [db (->db conn-or-db temporal)]    ; nil = commit pin the store lacks
       (when-not (or (= :db/id (:attr ref))     ; eids unique by construction
                     (unique-attr? db (:attr ref))
                     (= ambiguous :first))
         (throw (ex-info "Selector attr is not :db/unique — a reference must be single-valued"
                         {:attr (:attr ref) :db-id db-id
                          :hint "declare :db/unique on the attr, pass {:ambiguous :first}, or use resolve-all"})))
       (when-let [eid (first (selector-eids db (:attr ref) (:value ref)))]
         {:db db :eid eid})))))
