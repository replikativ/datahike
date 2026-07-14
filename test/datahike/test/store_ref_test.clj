(ns datahike.test.store-ref-test
  "`:db.type/store-ref` — a datom value that NAMES an object in the database's own
   konserve store (a blob, an out-of-line document), and the GC contract that keeps
   it alive.

   The rule under test throughout: THE DATABASE IS THE ROOT SET. An object in the
   store lives iff a datom points at it; anything else is garbage by definition."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.blob :as blob]
            [datahike.gc-guard :as guard]
            [datahike.value-types :as vt]
            [hasch.core :as h]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]))

(def ^:private schema
  [{:db/ident :issue/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :issue/attachment
    :db/valueType :db.type/store-ref            ;; <- the object it names is GC-marked
    :db/cardinality :db.cardinality/one}])

(defn- cfg [nm]
  {:store {:backend :file
           :path (str (System/getProperty "java.io.tmpdir") "/dh-store-ref-" nm)
           :id (java.util.UUID/randomUUID)}
   :schema-flexibility :write
   :keep-history? false
   ;; LOAD-BEARING: with the commit graph on and the default remove-before, every
   ;; commit record stays reachable and the sweep collects nothing — every
   ;; assertion below would then hold vacuously.
   :commit-graph? false})

(defn- keys-of [store] (set (map :key (k/keys store {:sync? true}))))

(defn- put-blob!
  "What a user does: write the bytes under their hasch content hash, and reference
   that hash from a datom — inside the guard, so a collection cannot run in the
   window where the object exists and nothing names it yet."
  [conn bytes attrs]
  (let [store (:store @conn)
        sid   (:id (:store (:config @conn)))
        key   (blob/blob-id bytes)]
    (guard/with-unreferenced-writes sid
      (<?? S (k/bassoc store key bytes {:sync? false}))
      (d/transact conn [(assoc attrs :issue/attachment key)]))
    key))

(deftest store-ref-is-a-registered-value-type
  (testing "the type is registered through the value-types seam, and declares its GC contract"
    (is (vt/registered? :db.type/store-ref))
    (is (contains? (vt/key-bearing-types) :db.type/store-ref)
        "GC must know this type can name storage")
    (is (= #{:a} ((vt/reachable-keys-fn :db.type/store-ref) :a nil)))))

(deftest referenced-blob-survives-gc
  (let [c (cfg "keep")]
    (d/delete-database c)
    (d/create-database c)
    (let [conn  (d/connect c)
          _     (d/transact conn schema)
          store (:store @conn)]
      ;; churn, so the sweep has real garbage to take (not a vacuous pass)
      (doseq [i (range 40)]
        (d/transact conn [{:issue/title (str "noise-" i)}]))
      (let [bytes (.getBytes "the attachment payload" "UTF-8")
            blob  (put-blob! conn bytes {:issue/title "crash report"})]
        (is (contains? (keys-of store) blob) "precondition: the blob is in the store")
        (let [swept (set (<?? S (gc/gc-storage! @conn)))]
          (is (pos? (count swept))
              "precondition: the cycle swept real garbage")
          (is (not (contains? swept blob))
              "a blob a datom points at must survive collection"))
        (testing "and it is still readable, cold, after the sweep"
          (d/release conn)
          (let [c2    (d/connect c)
                store (:store @c2)
                found (d/q '[:find ?b . :where [?e :issue/title "crash report"] [?e :issue/attachment ?b]]
                           @c2)]
            (is (= blob found) "the reference round-trips through the store")
            (is (= "the attachment payload"
                   (String. ^bytes (k/bget store found
                                           (fn [{:keys [input-stream]}]
                                             (.readAllBytes input-stream))
                                           {:sync? true})
                            "UTF-8"))
                "and the bytes are still there")
            (d/release c2)))))
    (d/delete-database c)))

(deftest retracted-blob-is-collected
  (testing "retract the datom and the blob becomes garbage — reclamation IS the GC's job"
    (let [c (cfg "collect")]
      (d/delete-database c)
      (d/create-database c)
      (let [conn  (d/connect c)
            _     (d/transact conn schema)
            store (:store @conn)
            bytes (.getBytes "soon to be orphaned" "UTF-8")
            blob  (put-blob! conn bytes {:issue/title "temp"})]
        ;; still referenced => spared
        (is (not (contains? (set (<?? S (gc/gc-storage! @conn))) blob)))
        ;; retract the entity; nothing names the blob any more
        (let [eid (d/q '[:find ?e . :where [?e :issue/title "temp"]] @conn)]
          (d/transact conn [[:db/retractEntity eid]]))
        (let [swept (set (<?? S (gc/gc-storage! @conn)))]
          (is (contains? swept blob)
              "an unreferenced blob is ordinary garbage")
          (is (not (contains? (keys-of store) blob))
              "and it is gone from the store"))
        (d/release conn))
      (d/delete-database c))))

(deftest attribute-refs-are-translated
  (testing "with :attribute-refs? the datoms hold the attribute's EID, not its ident"
    ;; The mark slices AEVT by attribute. Slicing by the ident would find NOTHING
    ;; here, mark no blobs, and sweep every one of them — silently.
    (let [c (assoc (cfg "attr-refs") :attribute-refs? true)]
      (d/delete-database c)
      (d/create-database c)
      (let [conn (d/connect c)]
        (d/transact conn schema)
        (let [blob (put-blob! conn (.getBytes "refs payload" "UTF-8")
                              {:issue/title "with attribute refs"})]
          (doseq [i (range 20)]
            (d/transact conn [{:issue/title (str "noise-" i)}]))
          (let [swept (set (<?? S (gc/gc-storage! @conn)))]
            (is (pos? (count swept)) "precondition: swept real garbage")
            (is (not (contains? swept blob))
                "the blob survives — the slice used the attribute's eid")))
        (d/release conn))
      (d/delete-database c))))

(deftest schema-shapes-that-would-lose-blobs-are-rejected
  ;; Both of these defeat the mark SILENTLY, so they are refused at transact time
  ;; rather than documented.
  (let [c (cfg "reject")]
    (d/delete-database c)
    (d/create-database c)
    (let [conn (d/connect c)]
      (testing "a tuple cannot hold a store-ref — its valueType is :db.type/tuple, so
                the keys nested in the vector are invisible to the mark"
        (is (thrown-with-msg?
             Exception #"tuple cannot hold"
             (d/transact conn [{:db/ident :issue/attachments
                                :db/valueType :db.type/tuple
                                :db/tupleType :db.type/store-ref
                                :db/cardinality :db.cardinality/one}]))))
      (testing ":db/noHistory cannot hold a store-ref — a retracted value is not kept
                in the temporal indices, so history would name a collected object"
        (is (thrown-with-msg?
             Exception #"noHistory cannot be combined"
             (d/transact conn [{:db/ident :issue/scratch
                                :db/valueType :db.type/store-ref
                                :db/noHistory true
                                :db/cardinality :db.cardinality/one}]))))
      (d/release conn))
    (d/delete-database c)))

(deftest external-blobs-mark-without-sweep
  ;; The EXTERNAL deployment: the bytes never enter the konserve store at all. A
  ;; browser PUTs them to s3://bucket/tenant/{id}/blobs/{uuid} with a presigned URL
  ;; and a content-type — they never transit the JVM, which is the entire point.
  ;;
  ;; Datahike cannot delete from there and does not pretend to. It owns the HARD
  ;; half — which ids are still named, including from retained history and other
  ;; branches — and hands that set to the application, which owns the easy half.
  (let [c (cfg "external")]
    (d/delete-database c)
    (d/create-database c)
    (let [conn  (d/connect c)
          _     (d/transact conn schema)
          store (:store @conn)
          ;; a blob id for bytes that live SOMEWHERE ELSE — nothing is written here
          external (h/uuid (.getBytes "lives in s3://bucket/tenant/x/blobs/…" "UTF-8"))]
      (d/transact conn [{:issue/title "has an external attachment"
                         :issue/attachment external}])
      (testing "the mark reports it, though its bytes are not in this store"
        (is (contains? (<?? S (gc/reachable-store-refs @conn)) external))
        (is (not (contains? (keys-of store) external))
            "precondition: nothing was written to the konserve store"))
      (testing "and a collection is a harmless no-op for it"
        ;; whitelisting a key the store does not have does nothing
        (is (not (contains? (set (<?? S (gc/gc-storage! @conn))) external))))
      (testing "retracting the datom drops it from the live set — that is the sweep signal"
        (let [eid (d/q '[:find ?e . :where [?e :issue/title "has an external attachment"]] @conn)]
          (d/transact conn [[:db/retractEntity eid]]))
        (is (not (contains? (<?? S (gc/reachable-store-refs @conn)) external))
            "the application now knows it may delete the object from its own store"))
      (d/release conn))
    (d/delete-database c)))

(deftest retained-history-keeps-blobs-live
  ;; Retention is not a special case — a store-ref named only by history is still
  ;; live, exactly as an index node would be. This matters because the point of
  ;; keeping history is being able to read it, and a retracted attachment you can
  ;; no longer fetch is not history.
  (let [c (assoc (cfg "history") :keep-history? true :commit-graph? false)]
    (d/delete-database c)
    (d/create-database c)
    (let [conn (d/connect c)]
      (d/transact conn schema)
      (let [blob (put-blob! conn (.getBytes "still readable as-of" "UTF-8")
                            {:issue/title "historic"})
            eid  (d/q '[:find ?e . :where [?e :issue/title "historic"]] @conn)]
        (d/transact conn [[:db/retractEntity eid]])
        (is (contains? (<?? S (gc/reachable-store-refs @conn)) blob)
            "retracted, but :keep-history? keeps the datom — so the blob is still live")
        (is (not (contains? (set (<?? S (gc/gc-storage! @conn))) blob))
            "and the sweep leaves it alone"))
      (d/release conn))
    (d/delete-database c)))

(deftest content-addressing-dedups-and-survives-rename
  ;; The two things a content id buys, and the reason a PATH must never be the id.
  ;;
  ;; The git model: blobs are addressed by CONTENT; trees map NAMES to hashes. So the
  ;; name lives in its own datom, where it also sorts and seeks — you lose nothing.
  (let [c (cfg "content")]
    (d/delete-database c)
    (d/create-database c)
    (let [conn (d/connect c)]
      (d/transact conn [{:db/ident :blob/id :db/valueType :db.type/store-ref
                         :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :blob/path :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one :db/index true}])
      (let [bytes-1 (.getBytes "the invoice" "UTF-8")
            bytes-2 (.getBytes "the invoice" "UTF-8")]  ;; distinct arrays, same content
        (testing "the same bytes are the same id — two uploads, one object"
          (is (not (identical? bytes-1 bytes-2)))
          (is (= (blob/blob-id bytes-1) (blob/blob-id bytes-2))
              "content-addressed: re-uploading is idempotent and identical content dedups"))

        (let [id (blob/blob-id bytes-1)]
          (<?? S (k/bassoc (:store @conn) id bytes-1 {:sync? false}))
          (d/transact conn [{:blob/id id :blob/path "tenant/acme/2026/invoice.pdf"}])

          (testing "the path sorts and seeks — that is what you wanted a string for"
            (is (= ["tenant/acme/2026/invoice.pdf"]
                   (mapv :v (d/datoms @conn :avet :blob/path)))
                "indexed, so a prefix like \"tenant/acme/2026/\" is a range scan"))

          (testing "a rename touches ONE datom — the object does not move"
            (let [e (d/q '[:find ?e . :where [?e :blob/id]] @conn)]
              (d/transact conn [{:db/id e :blob/path "archive/2026/invoice.pdf"}]))
            (is (= id (d/q '[:find ?b . :where [?e :blob/id ?b]] @conn))
                "the id is unchanged — every historical reference still resolves")
            (is (contains? (keys-of (:store @conn)) id)
                "and the bytes are still at the same key"))))
      (d/release conn))
    (d/delete-database c)))

(deftest unreferenced-blob-is-not-kept
  (testing "writing bytes into the store does NOT make them live — only a datom does"
    (let [c (cfg "orphan")]
      (d/delete-database c)
      (d/create-database c)
      (let [conn  (d/connect c)
            _     (d/transact conn schema)
            store (:store @conn)
            bytes (.getBytes "nobody points at me" "UTF-8")
            orphan (h/uuid bytes)]
        (<?? S (k/bassoc store orphan bytes {:sync? false}))
        (let [swept (set (<?? S (gc/gc-storage! @conn)))]
          (is (contains? swept orphan)
              "the database is the root set: an object nothing references is garbage"))
        (d/release conn))
      (d/delete-database c))))
