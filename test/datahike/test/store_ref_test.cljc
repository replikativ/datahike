(ns datahike.test.store-ref-test
  "`:db.type/store-ref` — a datom value that NAMES an object in the database's own
   konserve store (a blob, an out-of-line document), and the GC contract that keeps
   it alive.

   The rule under test throughout: THE DATABASE IS THE ROOT SET. An object in the
   store lives iff a datom points at it; anything else is garbage by definition.

   Portable (`deftest-async`): every test runs on both JVM and Node cljs, since the
   feature — and its headline case, a browser referencing an out-of-line object — is
   portable. The platform seams are small and isolated in the helpers below:
   create/connect/delete (sync on JVM, channel-returning on cljs), byte construction
   (`byte[]` vs `js/Buffer`), and the binary read-back (`:input-stream` vs `:blob`)."
  #?(:clj  (:require [clojure.test :refer [deftest is testing]]
                     [clojure.spec.alpha :as s]
                     [datahike.schema :as ds]
                     [datahike.api :as d]
                     [datahike.gc :as gc]
                     [datahike.blob :as blob]
                     [datahike.gc-guard :as guard :refer [with-unreferenced-writes]]
                     [datahike.test.async :refer [deftest-async]]
                     [konserve.core :as k]
                     [clojure.core.async :as a :refer [<! go]])
     :cljs (:require [cljs.test :refer [deftest is testing] :include-macros true]
                     [cljs.spec.alpha :as s]
                     [datahike.schema :as ds]
                     [datahike.api :as d]
                     [datahike.gc :as gc]
                     [datahike.blob :as blob]
                     [datahike.gc-guard :as guard :refer [with-unreferenced-writes]]
                     [datahike.test.async :refer-macros [deftest-async]]
                     ;; register the :file backend on Node — GC's mark walk needs a
                     ;; flushing store (a store-ref names an object that lives there).
                     [konserve.node-filestore]
                     [konserve.core :as k]
                     [cljs.nodejs :as nodejs]
                     [clojure.core.async :as a :refer [<!] :refer-macros [go]])))

(def ^:private schema
  [{:db/ident :issue/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :issue/attachment
    :db/valueType :db.type/store-ref            ;; <- the object it names is GC-marked
    :db/cardinality :db.cardinality/one}])

;; ---------------------------------------------------------------------------
;; Platform seams — the only places JVM and cljs differ.
;; ---------------------------------------------------------------------------

(defn- rand-uuid [] #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

(defn- tmp-path [id]
  #?(:clj  (str (System/getProperty "java.io.tmpdir") "/dh-store-ref-" id)
     :cljs (let [os (nodejs/require "os") p (nodejs/require "path")]
             (.join p (.tmpdir os) (str "dh-store-ref-" id)))))

(defn- str->bytes [s]
  #?(:clj  (.getBytes ^String s "UTF-8")
     :cljs (js/Buffer.from s "utf-8")))

#?(:clj
   (defn- blob->str
     "Decode the JVM file backend's `:input-stream` to a UTF-8 string. Used only by
      the JVM read-back assertion (konserve's Node sync binary read is broken)."
     [{:keys [input-stream]}]
     (String. (.readAllBytes ^java.io.InputStream input-stream) "UTF-8")))

(defn- base-cfg [id extra]
  (merge {:store {:backend :file :path (tmp-path id) :id id}
          :schema-flexibility :write
          :keep-history? false
          ;; LOAD-BEARING: with the commit graph on and the default remove-before,
          ;; every commit record stays reachable and the sweep collects nothing —
          ;; every assertion below would then hold vacuously.
          :commit-graph? false}
         extra))

(defn- fresh-conn
  "Delete any prior db, create fresh, connect. Channel-returning on both platforms
   (create/connect are sync on the JVM, async on cljs)."
  [cfg]
  (go #?(:clj  (do (when (d/database-exists? cfg) (d/delete-database cfg))
                   (d/create-database cfg)
                   (d/connect cfg))
         :cljs (do (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
                   (<! (d/create-database cfg))
                   (<! (d/connect cfg {:sync? false}))))))

(defn- reopen
  "Connect to an existing db (no delete/create) — for the cold-read-back checks."
  [cfg]
  (go #?(:clj  (d/connect cfg)
         :cljs (<! (d/connect cfg {:sync? false})))))

(defn- cleanup [conn cfg]
  (go (d/release conn)
      #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg)))))

(defn- keys-of [store]
  ;; The sweep's whitelist is over stored keys; k/keys is available on both.
  (go (set (map :key (<! (k/keys store {:sync? false}))))))

(defn- put-blob!
  "What a user does — and exactly what doc/store-refs.md tells them to write: put the
   bytes under their content hash, then reference that hash from a datom, INSIDE the
   guard so a collection cannot run in the window where the object exists and nothing
   names it yet. Channel-returning; yields the blob id."
  [conn bytes attrs]
  (go (let [store (:store @conn)
            sid   (:id (:store (:config @conn)))
            key   (blob/blob-id bytes)]
        (with-unreferenced-writes sid
          (<! (k/bassoc store key bytes {:sync? false}))
          (<! (d/transact! conn [(assoc attrs :issue/attachment key)])))
        key)))

;; ---------------------------------------------------------------------------

(deftest store-ref-is-a-builtin-value-type
  ;; Defined exactly like every other builtin: an s/def, membership in the enum, a
  ;; system-schema entity (so :attribute-refs? can point at it). No registry, no
  ;; extension seam — the ONE thing it adds over :db.type/uuid is that GC marks it.
  (testing "it is a builtin, and the collector knows it names storage"
    (is (contains? ds/builtin-value-types :db.type/store-ref))
    (is (contains? ds/key-bearing-value-types :db.type/store-ref)
        "GC must know this type names an object")
    (is (s/valid? :db.type/store-ref (rand-uuid)))
    (is (not (s/valid? :db.type/store-ref "a-string"))
        "uuid only — an attribute mixing uuids and strings would have no AVET order")))

(deftest-async referenced-blob-survives-gc
  (let [cfg   (base-cfg (rand-uuid) nil)
        conn  (<! (fresh-conn cfg))
        store (:store @conn)]
    (<! (d/transact! conn schema))
    ;; churn, so the sweep has real garbage to take (not a vacuous pass)
    (loop [i 0]
      (when (< i 40)
        (<! (d/transact! conn [{:issue/title (str "noise-" i)}]))
        (recur (inc i))))
    (let [bytes (str->bytes "the attachment payload")
          blob  (<! (put-blob! conn bytes {:issue/title "crash report"}))]
      (is (contains? (<! (keys-of store)) blob) "precondition: the blob is in the store")
      (let [swept (set (<! (gc/gc-storage! @conn)))]
        (is (pos? (count swept))
            "precondition: the cycle swept real garbage")
        (is (not (contains? swept blob))
            "a blob a datom points at must survive collection"))
      (testing "and it is still readable, cold, after the sweep"
        (d/release conn)
        (let [c2    (<! (reopen cfg))
              store (:store @c2)
              found (d/q '[:find ?b . :where [?e :issue/title "crash report"] [?e :issue/attachment ?b]]
                         @c2)]
          (is (= blob found) "the reference round-trips through the store")
          ;; Reading the raw bytes back is konserve's contract, not datahike's — and
          ;; konserve's Node sync binary read miscomputes the value length (an upstream
          ;; quirk). Assert the byte content on the JVM; the datahike-level guarantee
          ;; (the reference survives GC and round-trips cold) is checked portably above.
          #?(:clj (is (= "the attachment payload"
                         (k/bget store found blob->str {:sync? true}))
                      "and the bytes are still there"))
          (d/release c2))))
    #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg)))))

(deftest-async retracted-blob-is-collected
  (testing "retract the datom and the blob becomes garbage — reclamation IS the GC's job"
    (let [cfg   (base-cfg (rand-uuid) nil)
          conn  (<! (fresh-conn cfg))
          store (:store @conn)]
      (<! (d/transact! conn schema))
      (let [bytes (str->bytes "soon to be orphaned")
            blob  (<! (put-blob! conn bytes {:issue/title "temp"}))]
        ;; still referenced => spared
        (is (not (contains? (set (<! (gc/gc-storage! @conn))) blob)))
        ;; retract the entity; nothing names the blob any more
        (let [eid (d/q '[:find ?e . :where [?e :issue/title "temp"]] @conn)]
          (<! (d/transact! conn [[:db/retractEntity eid]])))
        (let [swept (set (<! (gc/gc-storage! @conn)))]
          (is (contains? swept blob)
              "an unreferenced blob is ordinary garbage")
          (is (not (contains? (<! (keys-of store)) blob))
              "and it is gone from the store")))
      (<! (cleanup conn cfg)))))

(deftest-async attribute-refs-are-translated
  (testing "with :attribute-refs? the datoms hold the attribute's EID, not its ident"
    ;; The mark slices AEVT by attribute. Slicing by the ident would find NOTHING
    ;; here, mark no blobs, and sweep every one of them — silently.
    (let [cfg  (base-cfg (rand-uuid) {:attribute-refs? true})
          conn (<! (fresh-conn cfg))]
      (<! (d/transact! conn schema))
      (let [blob (<! (put-blob! conn (str->bytes "refs payload")
                                {:issue/title "with attribute refs"}))]
        (loop [i 0]
          (when (< i 20)
            (<! (d/transact! conn [{:issue/title (str "noise-" i)}]))
            (recur (inc i))))
        (let [swept (set (<! (gc/gc-storage! @conn)))]
          (is (pos? (count swept)) "precondition: swept real garbage")
          (is (not (contains? swept blob))
              "the blob survives — the slice used the attribute's eid")))
      (<! (cleanup conn cfg)))))

(defn- tx-error
  "The error a rejected transaction surfaces, as a string — portably. The async
   writer delivers a schema rejection as an error value rather than throwing
   synchronously, so take it and, if it did throw inside the go, catch it."
  [conn tx]
  (go (str (try (<! (d/transact! conn tx))
                (catch #?(:clj Throwable :cljs :default) e e)))))

(deftest-async schema-shapes-that-would-lose-blobs-are-rejected
  ;; Both of these defeat the mark SILENTLY, so they are refused at transact time
  ;; rather than documented.
  (let [cfg  (base-cfg (rand-uuid) nil)
        conn (<! (fresh-conn cfg))]
    (testing "a tuple cannot hold a store-ref — its valueType is :db.type/tuple, so
              the keys nested in the vector are invisible to the mark"
      (is (re-find #"tuple cannot hold"
                   (<! (tx-error conn [{:db/ident :issue/attachments
                                        :db/valueType :db.type/tuple
                                        :db/tupleType :db.type/store-ref
                                        :db/cardinality :db.cardinality/one}])))))
    (testing ":db/noHistory cannot hold a store-ref — a retracted value is not kept
              in the temporal indices, so history would name a collected object"
      (is (re-find #"noHistory cannot be combined"
                   (<! (tx-error conn [{:db/ident :issue/scratch
                                        :db/valueType :db.type/store-ref
                                        :db/noHistory true
                                        :db/cardinality :db.cardinality/one}])))))
    (<! (cleanup conn cfg))))

(deftest-async external-blobs-mark-without-sweep
  ;; The EXTERNAL deployment: the bytes never enter the konserve store at all. A
  ;; browser PUTs them to s3://bucket/tenant/{id}/blobs/{uuid} with a presigned URL
  ;; and a content-type — they never transit the JVM, which is the entire point.
  ;;
  ;; Datahike cannot delete from there and does not pretend to. It owns the HARD
  ;; half — which ids are still named, including from retained history and other
  ;; branches — and hands that set to the application, which owns the easy half.
  (let [cfg   (base-cfg (rand-uuid) nil)
        conn  (<! (fresh-conn cfg))
        store (:store @conn)]
    (<! (d/transact! conn schema))
    ;; a blob id for bytes that live SOMEWHERE ELSE — nothing is written here
    (let [external (blob/blob-id (str->bytes "lives in s3://bucket/tenant/x/blobs/…"))]
      (<! (d/transact! conn [{:issue/title "has an external attachment"
                              :issue/attachment external}]))
      (testing "the mark reports it, though its bytes are not in this store"
        (is (contains? (<! (gc/reachable-store-refs @conn)) external))
        (is (not (contains? (<! (keys-of store)) external))
            "precondition: nothing was written to the konserve store"))
      (testing "and a collection is a harmless no-op for it"
        ;; whitelisting a key the store does not have does nothing
        (is (not (contains? (set (<! (gc/gc-storage! @conn))) external))))
      (testing "retracting the datom drops it from the live set — that is the sweep signal"
        (let [eid (d/q '[:find ?e . :where [?e :issue/title "has an external attachment"]] @conn)]
          (<! (d/transact! conn [[:db/retractEntity eid]])))
        (is (not (contains? (<! (gc/reachable-store-refs @conn)) external))
            "the application now knows it may delete the object from its own store")))
    (<! (cleanup conn cfg))))

(deftest-async retained-history-keeps-blobs-live
  ;; Retention is not a special case — a store-ref named only by history is still
  ;; live, exactly as an index node would be. This matters because the point of
  ;; keeping history is being able to read it, and a retracted attachment you can
  ;; no longer fetch is not history.
  (let [cfg  (base-cfg (rand-uuid) {:keep-history? true})
        conn (<! (fresh-conn cfg))]
    (<! (d/transact! conn schema))
    (let [blob (<! (put-blob! conn (str->bytes "still readable as-of")
                              {:issue/title "historic"}))
          eid  (d/q '[:find ?e . :where [?e :issue/title "historic"]] @conn)]
      (<! (d/transact! conn [[:db/retractEntity eid]]))
      (is (contains? (<! (gc/reachable-store-refs @conn)) blob)
          "retracted, but :keep-history? keeps the datom — so the blob is still live")
      (is (not (contains? (set (<! (gc/gc-storage! @conn))) blob))
          "and the sweep leaves it alone"))
    (<! (cleanup conn cfg))))

(deftest-async content-addressing-dedups-and-survives-rename
  ;; The two things a content id buys, and the reason a PATH must never be the id.
  ;;
  ;; The git model: blobs are addressed by CONTENT; trees map NAMES to hashes. So the
  ;; name lives in its own datom, where it also sorts and seeks — you lose nothing.
  (let [cfg  (base-cfg (rand-uuid) nil)
        conn (<! (fresh-conn cfg))]
    (<! (d/transact! conn [{:db/ident :blob/id :db/valueType :db.type/store-ref
                            :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                           {:db/ident :blob/path :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one :db/index true}]))
    (let [bytes-1 (str->bytes "the invoice")
          bytes-2 (str->bytes "the invoice")]  ;; distinct arrays, same content
      (testing "the same bytes are the same id — two uploads, one object"
        (is (not (identical? bytes-1 bytes-2)))
        (is (= (blob/blob-id bytes-1) (blob/blob-id bytes-2))
            "content-addressed: re-uploading is idempotent and identical content dedups"))

      (let [id (blob/blob-id bytes-1)]
        (<! (k/bassoc (:store @conn) id bytes-1 {:sync? false}))
        (<! (d/transact! conn [{:blob/id id :blob/path "tenant/acme/2026/invoice.pdf"}]))

        (testing "the path sorts and seeks — that is what you wanted a string for"
          (is (= ["tenant/acme/2026/invoice.pdf"]
                 (mapv :v (d/datoms @conn :avet :blob/path)))
              "indexed, so a prefix like \"tenant/acme/2026/\" is a range scan"))

        (testing "a rename touches ONE datom — the object does not move"
          (let [e (d/q '[:find ?e . :where [?e :blob/id]] @conn)]
            (<! (d/transact! conn [{:db/id e :blob/path "archive/2026/invoice.pdf"}])))
          (is (= id (d/q '[:find ?b . :where [?e :blob/id ?b]] @conn))
              "the id is unchanged — every historical reference still resolves")
          (is (contains? (<! (keys-of (:store @conn))) id)
              "and the bytes are still at the same key"))))
    (<! (cleanup conn cfg))))

(deftest-async unreferenced-blob-is-not-kept
  (testing "writing bytes into the store does NOT make them live — only a datom does"
    (let [cfg   (base-cfg (rand-uuid) nil)
          conn  (<! (fresh-conn cfg))
          store (:store @conn)]
      (<! (d/transact! conn schema))
      (let [bytes  (str->bytes "nobody points at me")
            orphan (blob/blob-id bytes)]
        (<! (k/bassoc store orphan bytes {:sync? false}))
        (let [swept (set (<! (gc/gc-storage! @conn)))]
          (is (contains? swept orphan)
              "the database is the root set: an object nothing references is garbage")))
      (<! (cleanup conn cfg)))))
