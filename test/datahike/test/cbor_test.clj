(ns datahike.test.cbor-test
  "The datahike CBOR codec.

  Codec-level, on purpose: this is where the encoding decisions are made, so
  this is where they are pinned. The peer/wire behaviour is
  `datahike.kabel.*`'s and is tested there.

  The size assertions are not decoration. The choice to carry a Datom
  positionally rather than as a field map was made from a measurement, and a
  measurement that lives only in a commit message stops being true."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [boring.core :as boring]
            [boring.data :as bdata]
            [datahike.api :as d]
            [datahike.cbor :as dcbor]
            [datahike.datom :as dd]
            [datahike.writing :as dw]))

(def ^:private test-dir
  (str (System/getProperty "java.io.tmpdir") "/datahike-cbor-codec-test"))

;; The store :id is not incidental here: it is the key the shared
;; persistent-sorted-set storage registry uses, and it is what a flushed root
;; stamps into its meta as :pss/storage-id. Reconstructing a DB is exactly the
;; act of looking this up.
(def ^:private store-id #uuid "b0e11a6f-0000-4000-8000-000000000001")

(def ^:private cfg
  {:store {:backend :file :path test-dir :id store-id}
   :keep-history? false
   :schema-flexibility :read
   :attribute-refs? false})

(defn- clean! []
  (when (.exists (io/file test-dir))
    (doseq [f (reverse (file-seq (io/file test-dir)))] (.delete f)))
  (try (d/delete-database cfg) (catch Exception _ nil)))

(use-fixtures :each (fn [f] (clean!) (try (f) (finally (clean!)))))

;; ---------------------------------------------------------------------------
;; Datoms
;; ---------------------------------------------------------------------------

(deftest datom-round-trips
  (let [reg (dcbor/registry)]
    (doseq [d [(dd/datom 1 :name "Alice" 536870913 true)
               (dd/datom 2 :age 42 536870914 true)
               (dd/datom 3 :friend 1 536870915 false)
               (dd/datom 4 :tags [:a :b] 536870916 true)]]
      (testing (pr-str d)
        (let [back (boring/decode (boring/encode d {:registry reg}) {:registry reg})]
          (is (= d back))
          (is (instance? datahike.datom.Datom back))
          (is (= (dd/datom-added d) (dd/datom-added back))
              "the added flag is the fifth positional value and is easy to drop"))))))

(deftest datom-is-positional-on-the-wire
  (testing "a Datom rides tag 27 with a five-element ARGUMENT VECTOR, not a
            field map. Decoding without the registry shows exactly what is on
            the wire, which is the only way to assert framing rather than
            round-trip luck."
    (let [d (dd/datom 1 :name "Alice" 536870913 true)
          raw (boring/decode (boring/encode d {:registry (dcbor/registry)}) {})]
      ;; A positional payload decodes to a TaggedLiteral, not an
      ;; UnknownRecord: the latter presents its payload as a map, which is
      ;; sound only over a map. It used to be returned for every shape, so a
      ;; Datom frame claimed IPersistentMap and then threw from keys/assoc.
      ;; `frame-name`/`frame-payload` read either shape without branching.
      (is (bdata/tagged-frame? raw))
      (is (tagged-literal? raw))
      (is (= "datahike.datom.Datom" (bdata/frame-name raw)))
      (is (= [1 :name "Alice" 536870913 true] (vec (bdata/frame-payload raw)))))))

(deftest positional-datoms-are-half-the-size-of-a-field-map
  (testing "the measurement the encoding decision rests on. If a future change
            makes a field map competitive, this fails and the decision should be
            revisited — that is the point of asserting it."
    (let [reg (dcbor/registry)
          attrs [:person/name :person/age :person/email :person/friend :person/city]
          datoms (vec (for [i (range 512)]
                        (dd/datom (+ 100000 i)
                                  (nth attrs (mod i (count attrs)))
                                  (if (even? i) (str "value-" i) (long (* i 37)))
                                  (+ 536870912 (quot i 20))
                                  true)))
          positional (count (boring/encode datoms {:registry reg}))
          as-maps (count (boring/encode
                          (mapv (fn [d]
                                  (bdata/tagged-value
                                   27 ["datahike.datom.Datom"
                                       {:e (.-e d) :a (.-a d) :v (.-v d)
                                        :tx (dd/datom-tx d) :added (dd/datom-added d)}]))
                                datoms)
                          {}))]
      (is (< positional (* 0.6 as-maps))
          (format "positional %d B (%.1f B/datom) vs field map %d B (%.1f B/datom)"
                  positional (/ (double positional) 512)
                  as-maps (/ (double as-maps) 512))))))

(deftest an-unregistered-reader-keeps-the-datom-rather-than-losing-it
  (testing "no datahike handlers at all: the values and the type name survive.
            This is what makes a boring dump inspectable by a reader that has
            never heard of datahike, and it is the reason for tag 27 over a
            private tag number."
    (let [d (dd/datom 7 :attr "v" 536870920 true)
          raw (boring/decode (boring/encode d {:registry (dcbor/registry)}) {})]
      (is (some? raw))
      (is (= 7 (nth (vec (bdata/frame-payload raw)) 0)))
      (testing "and it re-encodes to the identical bytes, so passthrough is lossless"
        (is (= (vec (boring/encode d {:registry (dcbor/registry)}))
               (vec (boring/encode raw {}))))))))

;; ---------------------------------------------------------------------------
;; The vertical: a real database
;; ---------------------------------------------------------------------------

;; Big enough that the index is a BRANCH over several leaves, not one leaf.
;; The branching factor is 512, so a few hundred datoms would exercise only
;; `blob->leaf` and the branch path would be dead code that looks tested. Same
;; trap as a 500-element sorted set collapsing to a single node.
(def ^:private entity-count 1000)

(defn- with-db
  "A connection whose index is deep enough to have interior nodes."
  []
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn (vec (for [i (range entity-count)]
                            {:name (str "person-" i) :age (mod i 90)})))
    conn))

(deftest a-flushed-db-round-trips-and-is-still-queryable
  (testing "the whole point: a DB encoded to CBOR and decoded back answers the
            same query. Its index roots are PSS nodes resolved through the
            shared storage registry, so this exercises the datahike handlers
            AND the persistent-sorted-set ones together."
    (let [conn (with-db)
          db (d/db conn)
          store (:store @conn)
          store-config (get-in (:config @conn) [:store])]
      (dcbor/register-store! store-config store)
      (try
        (let [reg (dcbor/registry)
              bs (boring/encode db {:registry reg})
              back (boring/decode bs {:registry reg})]
          (is (some? back))
          (testing "and answers the same query as the original"
            (let [q '[:find ?n :where [?e :name ?n]]]
              (is (= (set (d/q q db)) (set (d/q q back)))
                  "same names out of the decoded DB")
              (is (= entity-count (count (d/q q back))))))
          (testing "and the index really was deep — a single leaf would leave
                    the branch path untested while looking covered"
            (is (> (count (:eavt @conn)) 512)
                "more datoms than one leaf holds")))
        (finally
          (dcbor/unregister-store! store-config)
          (d/release conn))))))

(deftest a-db-without-a-registered-store-degrades-to-its-stored-map
  (testing "reconstruct-db falls back rather than throwing, which is what lets a
            peer inspect a DB whose store it does not hold. Asserted because the
            fallback is easy to turn into a raise during a refactor."
    (let [conn (with-db)
          db (d/db conn)
          reg (dcbor/registry)
          bs (boring/encode db {:registry reg})]
      (try
        (let [back (boring/decode bs {:registry reg})]
          (is (map? back))
          (is (contains? back :config) "the raw stored map came back"))
        (finally (d/release conn))))))

(deftest tx-report-carries-both-dbs-in-stored-form
  (let [conn (with-db)
        store-config (get-in (:config @conn) [:store])]
    (dcbor/register-store! store-config (:store @conn))
    (try
      (let [report (d/transact conn [{:name "Dave" :age 19}])
            reg (dcbor/registry)
            back (boring/decode (boring/encode report {:registry reg}) {:registry reg})]
        (is (map? back) "a TxReport stays a plain map on read — the writer
                         reconstructs it once sync completes")
        (is (contains? back :db-before))
        (is (contains? back :db-after))
        (is (seq (:tx-data back)))
        (is (every? #(instance? datahike.datom.Datom %) (:tx-data back))
            "tx-data datoms come back as Datoms, not as vectors"))
      (finally
        (dcbor/unregister-store! store-config)
        (d/release conn)))))

(deftest tx-report-survives-a-non-db-stub
  (testing "a TxReport can carry a plain map where a DB belongs — test stubs do
            exactly this. db->stored raises on that, so the projection is
            guarded; without the guard a serialisation concern surfaces as an
            unrelated test failure."
    (let [reg (dcbor/registry)
          stub (datahike.db.TxReport. {:max-tx 0} {:max-tx 1} [] {} nil)
          back (boring/decode (boring/encode stub {:registry reg}) {:registry reg})]
      (is (= {:max-tx 0} (:db-before back)))
      (is (= {:max-tx 1} (:db-after back))))))

;; ---------------------------------------------------------------------------
;; Parity with the format it replaces
;; ---------------------------------------------------------------------------

(deftest wire-content-matches-the-fressian-handlers
  (testing "the two formats must carry the SAME VALUES, so a switch is
            provably content-preserving rather than merely working. Compared at
            the value level because the framings differ by construction."
    (let [d (dd/datom 1 :name "Alice" 536870913 true)]
      (is (= (vec (seq d)) [1 :name "Alice" 536870913 true])
          "fressian writes (vec (seq d)); boring writes the same vector")
      (is (= d (dd/datom-from-reader (vec (seq d))))
          "and both read it back through datom-from-reader"))))
