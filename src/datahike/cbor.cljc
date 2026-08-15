(ns datahike.cbor
  "Datahike's CBOR codec: how a Datom, a DB and a TxReport go on the wire and
  into a store.

  Named for the FORMAT. These tag-27 shapes are exactly what a Rust, Python or
  JavaScript reader needs in order to read a datahike dump, so naming them after
  our Clojure implementation (org.replikativ/boring) would be parochial for the
  one thing here that is meant to cross languages.

  This lives in `src/`, not `src-kabel/`, deliberately. The same encoding has to
  serve two consumers — the kabel wire middleware and konserve's
  `BoringSerializer` (byte 3) — and if each defined it separately they would
  drift, which for a persisted format means a store written by one is misread by
  the other. There is one definition, here.

  The wire CONTENT is what the fressian handlers this replaced carried, value
  for value: a Datom carries `[e a v tx added]`, a DB carries `db->stored`, a
  TxReport carries its map with both DBs projected to stored form. Only the
  framing changed, which is what made the switch provably content-preserving
  rather than merely working.

  ## Framing: CBOR tag 27 throughout

  Every datahike type rides **tag 27** — IANA's registered \"serialised
  language-independent object with type name and constructor arguments\" — with
  the same type-name strings the fressian handlers used, so a reader of either
  is looking at the same type identity:

      27([\"datahike.datom.Datom\", [e, a, v, tx, added]])
      27([\"datahike.db.DB\", {...stored...}])

  No private tag numbers are claimed. This matters because boring already
  carries one unregistered provisional tag (39649, shaped arrays) and every
  additional FCFS number is another thing to register and another thing a
  foreign reader cannot interpret. A reader with no datahike handlers still
  decodes these to `boring.data/UnknownRecord`, which keeps the name and the
  values and re-encodes to identical bytes.

  ## Why a Datom is positional and a DB is not

  Datoms dominate: an index leaf holds hundreds, and the tx-data of a single
  transaction can hold thousands. Measured over 512 Datoms, carrying the five
  values as a field map costs **54.5 bytes each against 29.5** for a vector,
  1.85x — the five repeated keys are more than half the payload. So a Datom is
  `[e a v tx added]`, positional, matching `dd/datom-from-reader`. The ratio is
  asserted in `datahike.test.boring-test`, not merely recorded here.

  A stored DB is one value per message with a dozen genuinely-named fields, so
  a map costs nothing measurable and stays readable to a foreign consumer. It
  keeps its map.

  The cost of positional is on the fallback path and is worth stating: an
  UNREGISTERED reader gets a `clojure.lang.TaggedLiteral` carrying the type
  name and the five values, and cannot ask for `:e` by name — `(:e frame)` is
  nil, and map operations fail as ordinary \"not a map\" errors because a
  TaggedLiteral never claims to be one. Read the values with
  `boring.data/frame-payload`, which works for either fallback shape.

  Until boring split the fallback by payload shape this was worse than
  documented: a positional frame decoded to an `UnknownRecord`, which claims
  IPersistentMap and then threw raw ClassCastException from `keys` and
  IllegalArgumentException from `assoc`/`into`. A DB keeps its map, so it
  degrades to an UnknownRecord and every field stays reachable by name.

  That is why only the one dominant type pays it."
  (:require [boring.core :as boring]
            [datahike.datom :as dd]
            [datahike.db.utils :as dbu]
            [datahike.writing :as dw]
            [org.replikativ.persistent-sorted-set.cbor :as pss-cbor]
            [org.replikativ.persistent-sorted-set.impl.nodes :as pss-nodes]
            #?(:cljs [datahike.db :refer [DB TxReport]]))
  #?(:clj (:import [datahike.datom Datom]
                   [datahike.db DB TxReport])))

(def ^:const DEFAULT_BRANCHING_FACTOR 512)

;; The wire names.
;;
;; `namespace/Record`, which is what boring derives for itself:
;; `boring.records/wire-name` is `(str ns-sym "/" record-sym)` and
;; `boring.data/record-type-name` must match it. Registering under any other
;; shape means a record reached through boring's own `defrecords` macro would
;; carry a DIFFERENT name than the same type registered here — two names for one
;; type, found the hard way.
;;
;; It also says more to a stranger. These names are not private: they go in
;; tag-27 frames, and — since `dcbor/install` is what konserve-lmdb's boring
;; registry gets — into stored blobs. A foreign CBOR reader seeing
;; `datahike.datom.Datom` cannot tell where the namespace ends;
;; `datahike.datom/Datom` is self-describing, which is the whole reason the
;; frames are an IETF format rather than a private one.
(def ^:const datom-name     "datahike.datom/Datom")
(def ^:const db-name        "datahike.db/DB")
(def ^:const tx-report-name "datahike.db/TxReport")

;; NO compatibility alias for the dotted, class-shaped names these briefly had
;; (`datahike.datom.Datom`, inherited from the fressian handlers, which keyed on
;; the Java class). The CBOR codec is introduced by this change set — nothing
;; has ever written a tag-27 frame under the old spelling, so there is nothing
;; to read back. An alias would be dead code claiming to protect data that does
;; not exist.
;;
;; It buys nothing elsewhere either: an old peer speaks FRESSIAN frames, which a
;; CBOR peer cannot read at all, so the names inside are never reached; and
;; konserve-lmdb routes legacy vs boring by LEADING BYTE, not by record name.

;; ---------------------------------------------------------------------------
;; Store registry
;;
;; NOT format-specific, despite living here: this delegates to
;; persistent-sorted-set's impl.nodes registry, keyed by the store-config :id,
;; which is the :pss/storage-id a flushed root stamps into its meta. A peer
;; registers its FULL datahike store (stored->db needs it); the root resolver
;; pulls (:storage store) out of it. Any codec that reads PSS roots shares it.
;; ---------------------------------------------------------------------------

(defn register-store!   [store-config store] (pss-nodes/register-storage! (:id store-config) store))
(defn unregister-store! [store-config] (pss-nodes/unregister-storage! (:id store-config)))
(defn get-store         [store-config] (pss-nodes/registered-storage (:id store-config)))

(defn- reconstruct-db
  "A stored DB's index roots are already EAGER — each resolved its storage by
  `:pss/storage-id` while being read. Fetch the full store for `stored->db`; if
  it is not registered, hand back the raw stored map rather than throwing. That
  is what lets a peer inspect a DB whose store it does not hold, and it is easy
  to turn into a raise during a refactor, so the tests pin it."
  [stored]
  (if-let [store (pss-nodes/registered-storage (get-in stored [:config :store :id]))]
    (dw/stored->db stored store)
    stored))

(defn- tx-report->wire
  "A TxReport with both DBs projected to stored form.

  Guarded with `dbu/db?` rather than assuming: a TxReport built by a test stub
  can carry a plain map where a DB belongs, and `db->stored` raises on that,
  turning a serialisation concern into a failure somewhere unrelated."
  [tx-report]
  (let [->stored (fn [db] (if (dbu/db? db) (second (dw/db->stored db false)) db))]
    (-> (into {} tx-report)
        (update :db-before ->stored)
        (update :db-after ->stored))))

;; ---------------------------------------------------------------------------
;; Handlers
;; ---------------------------------------------------------------------------

(defn install-element-handlers
  "Datom write + read. Returns a NEW registry.

  `register-tag 27` supplies the WRITE side (a Datom is a deftype, so boring
  does not emit it natively the way it does a defrecord); `register-record`
  supplies the READ side, keyed by the same name. The `nil` read-fn on
  `register-tag` is deliberate — passing one would replace boring's built-in
  tag-27 dispatch wholesale, and that dispatch is what looks the name up."
  [reg]
  (-> reg
      (boring/register-tag 27 #?(:clj Datom :cljs dd/Datom)
                           (fn [d] [datom-name (vec (seq d))])
                           nil)
      (boring/register-record datom-name dd/datom-from-reader)))

(defn install-db-handlers
  "DB and TxReport write + read. Returns a NEW registry.

  A DB goes on the wire as `db->stored`, and comes back through `stored->db`
  with its roots already live. A TxReport stays a plain map on read — the
  KabelWriter reconstructs it once sync has completed, so materialising it here
  would be premature and would need a store that may not be registered yet."
  [reg]
  (-> reg
      (boring/register-tag 27 DB
                           (fn [db] [db-name (second (dw/db->stored db false))])
                           nil)
      (boring/register-record db-name reconstruct-db)
      (boring/register-tag 27 TxReport
                           (fn [r] [tx-report-name (tx-report->wire r)])
                           nil)
      (boring/register-record tx-report-name identity)))

(defn install
  "The full datahike CBOR registry: PSS nodes and roots, plus datahike's own
  element and record handlers. Returns a NEW registry.

  `opts` are `pss-cbor/install`'s. The wire default resolves storage and
  comparator through the shared registry, which is what a peer wants; a
  single-store serializer passes lexical closures instead."
  ([reg] (install reg {}))
  ([reg opts]
   (-> reg
       (pss-cbor/install
        (merge {:default-bf      DEFAULT_BRANCHING_FACTOR
                :resolve-storage (fn [m] (some-> (pss-nodes/registered-storage
                                                  (get m pss-nodes/storage-id-key))
                                                 :storage))
                :resolve-cmp     (fn [m] (dd/index-type->cmp-quick (:index-type m) false))}
               opts))
       install-element-handlers
       install-db-handlers)))

(defn registry
  "Convenience: `install` onto a fresh empty boring registry."
  ([] (registry {}))
  ([opts] (install (boring/tag-registry) opts)))
