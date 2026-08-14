(ns datahike.test.migrate-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [clj-cbor.core :as cbor]
            [datahike.api :as d]
            [datahike.constants :as c]
            [datahike.datom :as datom]
            [datahike.db.interface :as dbi]
            [datahike.db.utils :as dbu]
            [datahike.migrate :as m]
            [datahike.migrate.manifest :as mman]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.blobs :as mblobs]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.digest :as dig]
            [datahike.blob :as blob]
            [datahike.db]
            [konserve.core :as k]
            [clojure.core.async :as a :refer [go]]
            [superv.async :refer [<?? S]]
            [clojure.string :as str]
            [konserve.store :as ks]
            [datahike.migrate.store :as mstore]
            [datahike.test.utils :as utils]))

;; ---------------------------------------------------------------------------
;; core `load-entities` behaviour (independent of the dump format)

(defn- teardown [conn]
  (let [cfg (:config @conn)]
    (d/release conn)
    (d/delete-database cfg)))

(def tx-data [[:db/add 1 :db/cardinality :db.cardinality/one 536870913 true]
              [:db/add 1 :db/ident :name 536870913 true]
              [:db/add 1 :db/index true 536870913 true]
              [:db/add 1 :db/unique :db.unique/identity 536870913 true]
              [:db/add 1 :db/valueType :db.type/string 536870913 true]
              [:db/add 2 :db/cardinality :db.cardinality/one 536870913 true]
              [:db/add 2 :db/ident :age 536870913 true]
              [:db/add 2 :db/valueType :db.type/long 536870913 true]
              [:db/add 3 :age 25 536870913 true]
              [:db/add 3 :name "Alice" 536870913 true]
              [:db/add 4 :age 35 536870913 true]
              [:db/add 4 :name "Bob" 536870913 true]])

(deftest load-entities-test
  (testing "Test migrate simple datoms without attribute refs"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))
          cfg           {:store         {:backend :memory
                                         :id      #uuid "001d0000-0000-0000-0000-00000000001d"}
                         :keep-history? true
                         :attribute-refs false}
          conn (utils/setup-db cfg)]
      @(d/load-entities conn source-datoms)
      (is (= (into #{} source-datoms)
             (d/q '[:find ?e ?a ?v ?t ?op :where [?e ?a ?v ?t ?op]] @conn)))
      (teardown conn))))

;; ---------------------------------------------------------------------------
;; helpers for semantic (id-independent) comparison of two databases

(defn- normv
  "Normalise a value so array/bytes values compare structurally rather than by
   identity, while keeping enough to distinguish class (float vs double)."
  [v]
  (cond
    (bytes? v)                              [:bytes (vec v)]
    (= (class v) (class (float-array 0)))   [:farray (mapv float (vec v))]
    (= (class v) (class (double-array 0)))  [:darray (vec v)]
    :else                                   v))

(defn- a->ident [db a] (if (number? a) (dbi/-ident-for db a) a))

(defn- user-triples
  "Set of [attr-ident normalised-value op] over user-transaction datoms of a
   database's full history — an id-independent fingerprint of its content."
  [conn]
  (let [hdb (d/history @conn)]
    (set (for [dm (d/datoms hdb :eavt)
               :when (> (datom/datom-tx dm) c/tx0)]
           [(a->ident hdb (nth dm 1)) (normv (nth dm 2)) (nth dm 4)]))))

(defn- mem-cfg [{:keys [history? attribute-refs?]}]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history?      (boolean history?)
   :schema-flexibility :write
   :attribute-refs?    (boolean attribute-refs?)})

(def rich-schema
  [{:db/ident :name   :db/valueType :db.type/string  :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :score  :db/valueType :db.type/double  :db/cardinality :db.cardinality/one}
   {:db/ident :ratio  :db/valueType :db.type/float   :db/cardinality :db.cardinality/one}
   {:db/ident :big    :db/valueType :db.type/bigdec  :db/cardinality :db.cardinality/one}
   {:db/ident :huge   :db/valueType :db.type/bigint  :db/cardinality :db.cardinality/one}
   {:db/ident :when   :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/ident :id     :db/valueType :db.type/uuid    :db/cardinality :db.cardinality/one}
   {:db/ident :sym    :db/valueType :db.type/symbol  :db/cardinality :db.cardinality/one}
   {:db/ident :blob   :db/valueType :db.type/bytes   :db/cardinality :db.cardinality/one}
   {:db/ident :vec    :db/valueType :db.type/float-array  :db/cardinality :db.cardinality/one}
   {:db/ident :dvec   :db/valueType :db.type/double-array :db/cardinality :db.cardinality/one}
   {:db/ident :pos    :db/valueType :db.type/tuple   :db/cardinality :db.cardinality/one
    :db/tupleTypes [:db.type/long :db.type/long :db.type/keyword]}
   {:db/ident :friend :db/valueType :db.type/ref     :db/cardinality :db.cardinality/many}])

(defn- populate-rich! [conn]
  (d/transact conn rich-schema)
  (d/transact conn [{:db/id "a" :name "Alice" :score 0.0 :ratio (float 0.5) :big 1.50M
                     :huge 123456789012345678901234567890N
                     :when #inst "2020-01-01" :id #uuid "00000000-0000-0000-0000-0000000000aa"
                     :sym 'ns/foo :blob (byte-array [0 1 2 127 -1])
                     :vec (float-array [1.0 -2.5 3.25]) :dvec (double-array [9.0 8.5])
                     :pos [7 11 :north]}
                    {:db/id "b" :name "Bob" :score 3.14 :friend "a"}])
  (d/transact conn [[:db/retractEntity [:name "Bob"]]]))

;; ---------------------------------------------------------------------------
;; T-ROUND / T-TYPE — full round-trip of every value type
;;
;; Chunk sizes rather than formats: a dump is a directory, and the only axis
;; left is how many chunk files it is split into. `4` forces many chunks so the
;; multi-chunk manifest and the per-chunk hashes are exercised; a large value
;; puts everything in one.

(deftest roundtrip-all-value-types-test
  (doseq [cs [4 1000000]]
    (testing (str "round-trip every value type, history, chunk-size " cs)
      (let [src (utils/setup-db (mem-cfg {:history? true}))
            _   (populate-rich! src)
            dir (str (System/getProperty "java.io.tmpdir") "/dh-rt-" cs "-" (utils/get-time))
            manifest (m/export-db @src dir {:history? true :chunk-size cs})
            tgt (utils/setup-db (mem-cfg {:history? true}))
            report (m/import-db tgt dir {})]
        (is (:verified? report) "post-import verification passes")
        ;; The PROPERTY, kept after `finalize-import!` and `:finalize?` were
        ;; removed. It used to be phrased as "finalize-import! removed
        ;; :migration", which credited a no-op: the id map is threaded through
        ;; the import and lands on the tx-REPORT, never on the db value, so
        ;; there was nothing for that `swap!` to clear. An import leaving an
        ;; O(entities) map on the db is still the regression worth catching —
        ;; only the reason it cannot happen changed.
        (is (nil? (:migration @tgt)) "an import leaves no id map on the db value")
        (is (= (user-triples src) (user-triples tgt))
            "id-independent content is identical after round-trip")
        (testing "#633: double/float classes preserved exactly"
          (let [q (fn [conn a] (d/q [:find (list 'pull '?e [a]) '. :where ['?e :name "Alice"]] @conn))]
            (is (= Double (class (:score (q src :score)))))
            (is (= Double (class (:score (q tgt :score)))) ":db.type/double stays Double (was Float via CBOR)")
            (is (= Float  (class (:ratio (q tgt :ratio)))) ":db.type/float stays Float")))
        (teardown src)
        (teardown tgt)))))

(deftest double-zero-regression-test ;; T-TYPE, #633 minimal
  (testing "double 0.0 / NaN round-trip as Double, float stays Float"
    ;; Through the real codec path: encode-value, then a full record encode and
    ;; decode. Under clj-cbor these three were the values that broke (#633); under
    ;; boring's :canonical profile they would break again, which is why the dump
    ;; uses :archival. See datahike.migrate.cbor.
    (let [rt (fn [v] (nth (mcbor/decode-record
                           (mcbor/encode-record [1 :x (mcbor/encode-value v) 2 true]))
                          2))]
      (is (= Double (class (rt 0.0))))
      (is (= 0.0 (rt 0.0)))
      (is (Double/isNaN ^double (rt (Double/NaN))))
      (is (= Double (class (rt (/ 1.0 0.0)))))
      (is (= Float (class (rt (float 0.5)))))
      (is (= (float 0.5) (rt (float 0.5)))))))

;; ---------------------------------------------------------------------------
;; T-REFS — attribute-refs database, translate-not-insert (#508 / #531)

(deftest attribute-refs-roundtrip-test
  (testing "attribute-refs db round-trips; refs to system entities are translated"
    (let [src (utils/setup-db (mem-cfg {:history? true :attribute-refs? true}))
          _   (d/transact src [{:db/ident :name :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                               {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])
          _   (d/transact src [{:db/id "a" :name "Alice"} {:db/id "b" :name "Bob" :pal "a"}])
          path (str (System/getProperty "java.io.tmpdir") "/dh-ar-" (utils/get-time))
          _   (m/export-db @src path {:history? true})
          tgt (utils/setup-db (mem-cfg {:history? true :attribute-refs? true}))
          report (m/import-db tgt path {})]
      (is (:verified? report))
      (is (= (user-triples src) (user-triples tgt)))
      (is (= #{["Alice"]}
             (d/q '[:find ?pn :where [?b :name "Bob"] [?b :pal ?p] [?p :name ?pn]] (d/history @tgt)))
          "ref resolves to the same target entity (no :db/ident collision, no re-inserted system datom)")
      (teardown src)
      (teardown tgt))))

;; ---------------------------------------------------------------------------
;; T-ORDER — schema replays before the data that uses it (#262)

(deftest empty-target-import-order-test
  (testing "import into a fresh empty target succeeds (schema-before-data by tx order)"
    (let [src (utils/setup-db (mem-cfg {:history? false}))
          _   (do (d/transact src [{:db/ident :name :db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/one}])
                  (d/transact src [{:name "x"} {:name "y"}]))
          path (str (System/getProperty "java.io.tmpdir") "/dh-order-" (utils/get-time))
          _   (m/export-db @src path {})
          tgt (utils/setup-db (mem-cfg {:history? false}))]
      (is (:verified? (m/import-db tgt path {})))
      (is (= #{["x"] ["y"]} (d/q '[:find ?n :where [?e :name ?n]] @tgt)))
      (teardown src)
      (teardown tgt))))

;; ---------------------------------------------------------------------------
;; Security & integrity

(deftest security-unknown-tag-test ;; §4.1
  (testing "a dump cannot execute code, and an unknown tag never becomes a value

            The EDN codec needed a closed `:readers` map with a throwing
            `:default` to get this, and was tested against `#evil/tag` and the
            `#=(...)` eval probe. CBOR has no reader-eval to defend against —
            there is no syntax that can name a constructor — so the `#=` half of
            that test has no analogue and its absence is not a gap.

            What DOES still matter is the unknown-tag half: a tag this version
            does not know must not silently decode to something usable. boring
            surfaces it as an inert TaggedValue rather than guessing, so a datom
            carrying one is detectable rather than wrong."
    (let [;; tag 55799 is CBOR's self-describe tag: valid CBOR, not one of ours
          unknown (byte-array [(unchecked-byte 0xd9) (unchecked-byte 0xd9)
                               (unchecked-byte 0xf7) (unchecked-byte 0x01)])
          decoded (mcbor/decode-record unknown)]
      (is (not (number? decoded))
          "an unregistered tag must NOT decode to a bare usable value")
      (is (some? decoded) "…but it is surfaced rather than swallowed"))))

(deftest security-bad-chunk-path-test ;; T-SEC-PATH, §4.2
  (testing "a manifest chunk path outside the dump dir is refused before any read"
    (let [src (utils/setup-db (mem-cfg {}))
          _   (do (d/transact src [{:db/ident :n :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
                  (d/transact src [{:n "a"}]))
          dir (str (System/getProperty "java.io.tmpdir") "/dh-path-" (utils/get-time))
          manifest (m/export-db @src dir {})
          mf (io/file dir "manifest.edn")
          poisoned (assoc-in (read-string (slurp mf)) [:chunks 0 :file] "../evil.cbor")]
      (spit mf (pr-str poisoned))
      (let [tgt (utils/setup-db (mem-cfg {}))]
        (is (= :import/bad-chunk-path
               (try (m/import-db tgt dir {}) nil
                    (catch clojure.lang.ExceptionInfo ex (:error (ex-data ex))))))
        (teardown tgt))
      (teardown src))))

(deftest tamper-detection-test ;; T-TAMPER, §9
  (testing "flipping a byte in a chunk is caught by sha256 before import"
    (let [src (utils/setup-db (mem-cfg {}))
          _   (do (d/transact src [{:db/ident :n :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
                  (d/transact src [{:n "alpha"} {:n "beta"}]))
          dir (str (System/getProperty "java.io.tmpdir") "/dh-tamper-" (utils/get-time))
          ;; `:compression :none` because this test edits the CBOR itself. The
          ;; property under test is the codec's, not the container's; the
          ;; compressed path has its own tamper test below.
          _   (m/export-db @src dir {:compression :none})
          chunk (io/file dir "datoms-000001.cbor")
          content (let [bos (java.io.ByteArrayOutputStream.)]
                    (with-open [in (io/input-stream chunk)] (io/copy in bos))
                    (.toByteArray bos))]
      ;; flip one byte of the CBOR text string "alpha" -> "alphX". A chunk is
      ;; binary now, so tampering is a byte edit rather than a string replace;
      ;; the property under test (the manifest SHA-256 catches it) is unchanged.
      (let [idx (first (for [i (range (- (alength content) 5))
                             :when (= "alpha" (String. content i 5 "UTF-8"))] i))]
        (aset-byte content (+ idx 4) (byte (int \X)))
        (with-open [out (io/output-stream chunk)] (.write out content)))
      (let [tgt (utils/setup-db (mem-cfg {}))]
        (is (= :import/checksum-failed
               (try (m/import-db tgt dir {}) nil
                    (catch clojure.lang.ExceptionInfo ex (:error (ex-data ex))))))
        (teardown tgt))
      (teardown src))))

(deftest non-empty-target-refused-test ;; §8.7 recreate-and-restart
  (testing "import into a non-empty target is refused (import is not resumable)"
    (let [src (utils/setup-db (mem-cfg {}))
          _   (do (d/transact src [{:db/ident :n :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
                  (d/transact src [{:n "a"}]))
          path (str (System/getProperty "java.io.tmpdir") "/dh-nonempty-" (utils/get-time))
          _   (m/export-db @src path {})
          tgt (utils/setup-db (mem-cfg {}))]
      (d/transact tgt [{:db/ident :n :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
      (d/transact tgt [{:n "pre-existing"}])
      (is (= :import/non-empty-target
             (try (m/import-db tgt path {}) nil
                  (catch clojure.lang.ExceptionInfo ex (:error (ex-data ex))))))
      (teardown src)
      (teardown tgt))))

;; ---------------------------------------------------------------------------
;; Determinism (§12) & backward compatibility (G9)

(deftest determinism-test ;; T-DETERMINISM
  (testing "exporting the same db twice yields byte-identical chunks + digests"
    (let [src (utils/setup-db (mem-cfg {:history? true}))
          _   (populate-rich! src)
          d1  (str (System/getProperty "java.io.tmpdir") "/dh-det1-" (utils/get-time))
          d2  (str (System/getProperty "java.io.tmpdir") "/dh-det2-" (utils/get-time))
          ;; `:compression :none` — this asserts that :archival makes the BYTES
          ;; a function of the data. Compressed determinism is a property of the
          ;; gzip encoder and is covered separately.
          m1  (m/export-db @src d1 {:history? true :chunk-size 5 :compression :none})
          m2  (m/export-db @src d2 {:history? true :chunk-size 5 :compression :none})]
      (is (= (:semantic-digest m1) (:semantic-digest m2)))
      (is (= (mapv :sha256 (:chunks m1)) (mapv :sha256 (:chunks m2))))
      (let [rd (fn [f] (let [bos (java.io.ByteArrayOutputStream.)]
                         (with-open [in (io/input-stream f)] (io/copy in bos))
                         (vec (.toByteArray bos))))]
        (is (= (rd (io/file d1 "datoms-000001.cbor"))
               (rd (io/file d2 "datoms-000001.cbor")))
            "byte-identical chunks — :archival makes the bytes a function of the data"))
      (teardown src))))

(deftest legacy-bytes-are-read-identically-test
  ;; The compat oracle for dropping clj-cbor. Legacy dumps in the wild were
  ;; WRITTEN by clj-cbor, so the question is not "does boring round-trip?" —
  ;; it is "does boring read clj-cbor's bytes the way clj-cbor does?". The
  ;; fixture is therefore produced by clj-cbor (test-scope dep) and read by
  ;; both; writing it with boring would test boring against itself.
  (testing "boring decodes clj-cbor-written values identically"
    (let [vals [(double 0.0) (double 1.5) (double 2.0) (double ##Inf) (float 1.5)
                (bigint 1) (bigint 123456789012345678901234567890N) 1.50M 1.5M
                "text" :kw 'sym true nil (long 42) [1 2] {:a 1} #{1 2}
                #uuid "00000000-0000-0000-0000-000000000001"
                (byte-array [0 1 127 -1])]
          f (java.io.File/createTempFile "dh-legacy-bytes" ".cbor")]
      (with-open [out (io/output-stream f)] (cbor/spit-all out vals))
      (let [via-cbor (with-open [in (io/input-stream f)] (doall (cbor/decode-seq in)))
            via-bor  (with-open [in (io/input-stream f)] (doall (mcbor/decode-records in)))
            norm     (fn [x] (if (bytes? x) (vec x) x))]
        (is (= (count via-cbor) (count via-bor)) "same number of items")
        (doseq [[i a b] (map vector (range) via-cbor via-bor)]
          ;; NaN is excluded above on purpose: (= ##NaN ##NaN) is false, so it
          ;; cannot be compared this way. Its class is covered by the codec test.
          (is (= (norm a) (norm b)) (str "item " i " value"))
          (is (= (class a) (class b))
              (str "item " i " class: " (class a) " vs " (class b)))))
      (.delete f))))

(deftest legacy-instant-is-normalised-test
  ;; The ONE construct where the two libraries genuinely differ: clj-cbor decodes
  ;; tag 1 to java.time.Instant, boring to java.util.Date. `instance-to-date` in
  ;; the legacy importer already normalised that before the swap, so the swap is
  ;; a no-op here — but it is the difference most likely to be "fixed" away by
  ;; someone deleting that conversion, so it is pinned.
  (testing "a legacy instant arrives as the class :db.type/instant uses"
    (let [f (java.io.File/createTempFile "dh-legacy-inst" ".cbor")]
      (with-open [out (io/output-stream f)]
        (cbor/spit-all out [#inst "2026-01-01T00:00:00.000-00:00"]))
      (let [via-cbor (first (with-open [in (io/input-stream f)] (doall (cbor/decode-seq in))))
            via-bor  (first (with-open [in (io/input-stream f)] (doall (mcbor/decode-records in))))]
        (is (instance? java.time.Instant via-cbor) "clj-cbor gives an Instant")
        (is (instance? java.util.Date via-bor) "boring gives a Date")
        (is (= (.toEpochMilli ^java.time.Instant via-cbor)
               (.getTime ^java.util.Date via-bor))
            "…and they denote the same moment, which is why the swap is safe"))
      (.delete f))))

(deftest legacy-cbor-import-test ;; T-LEGACY, G9
  (testing "an old flat CBOR dump still imports via the legacy path"
    (let [datoms (mapv #(vec (rest %)) tx-data)
          path (str (System/getProperty "java.io.tmpdir") "/dh-legacy-" (utils/get-time))
          conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                :schema-flexibility :read :keep-history? false})]
      (cbor/spit-all path datoms)
      ;; The binding must actually REACH the reader, and asserting on the
      ;; imported datoms cannot see that — the import succeeds identically at
      ;; any batch size, so this test passed whether or not the var was plumbed
      ;; through. Nor can the datoms' `:tx` show it: a legacy import PRESERVES
      ;; the tx from the dump (that is what `update-max-tx` is for), and every
      ;; datom in this fixture carries 536870913, so the count is 1 regardless.
      ;;
      ;; The batch sizes handed to `transact` are the only honest observable.
      (let [sizes (atom [])
            orig  d/transact]
        (with-redefs [d/transact (fn [c tx] (swap! sizes conj (count tx)) (orig c tx))]
          ;; `datahike.migrate/*import-batch-size*` — where the CHANGELOG
          ;; published it ([#845]) — NOT `datahike.migrate.legacy`, where the
          ;; reader that consumes it lives.
          (binding [m/*import-batch-size* 5]
            (m/import-db conn path)))
        (is (= (set (map #(apply datom/datom %) datoms))
               (set (filter #(< (:e %) (:max-tx @conn)) (d/datoms @conn :eavt)))))
        (is (= [5 5 2] @sizes)
            (str "expected 12 datoms to arrive as batches of 5, 5, 2; got "
                 (pr-str @sizes) " — a single batch means the binding never "
                 "reached the legacy reader")))
      (teardown conn)
      (.delete (io/file path)))))

;; ---------------------------------------------------------------------------
;; core `load-entities` reconstructs history (lower-level than the dump format)

(deftest load-entities-history-test
  (testing "Migrate predefined set with historical data"
    (let [source-cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                      :keep-history? true :schema-flexibility :write :attribute-refs? false}
          schema     [{:db/ident :name :db/cardinality :db.cardinality/one :db/index true
                       :db/unique :db.unique/identity :db/valueType :db.type/string}]
          source-conn (utils/setup-db source-cfg)
          txs        [schema
                      [{:name "Alice"} {:name "Bob"}]
                      [{:name "Charlie"} {:name "Daisy"}]
                      [[:db/retractEntity [:name "Alice"]]]]
          _          (doseq [tx-data txs] (d/transact source-conn {:tx-data tx-data}))
          datoms-to-export (d/datoms (d/history @source-conn) :eavt)
          export-data (->> datoms-to-export
                           (map (comp vec seq))
                           (sort-by (fn [[_e a _v tx]] [tx (case a :db/txInstant 0 1)]))
                           (into []))
          target-conn (utils/setup-db (assoc-in source-cfg [:store :id] (java.util.UUID/randomUUID)))
          _          @(d/load-entities target-conn export-data)
          current-q  (fn [conn] (d/q '[:find ?n :where [?e :name ?n]] @conn))
          history-q  (fn [conn] (d/q '[:find ?n ?t ?op :where [?e :name ?n ?t ?op]] (d/history @conn)))]
      (is (dbu/distinct-sorted-datoms? :eavt datoms-to-export))
      (is (= (current-q source-conn) (current-q target-conn)))
      (is (= (history-q source-conn) (history-q target-conn)))
      (teardown source-conn)
      (teardown target-conn))))

;; ---------------------------------------------------------------------------
;; Schema that evolved over time — new attributes added across transactions
;; must round-trip, because history mode replays schema datoms in tx order.

(deftest schema-evolution-test
  (testing "attributes added at later txs round-trip under history replay"
    (let [src (utils/setup-db (mem-cfg {:history? true}))]
      ;; tx1: schema v1 (just :a)
      (d/transact src [{:db/ident :a :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      ;; tx2: data under v1
      (d/transact src [{:db/id "e1" :a 1}])
      ;; tx3: schema grows — add a ref attr :b (card-many) mid-history
      (d/transact src [{:db/ident :b :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])
      ;; tx4: data using the new attr
      (d/transact src [{:db/id "e2" :a 2 :b "e1"}])
      ;; tx5: schema grows again — add a unique string attr :c
      (d/transact src [{:db/ident :c :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
      ;; tx6: data using the new unique attr + a retraction
      (d/transact src [{:db/id "e3" :a 3 :c "hello"}])
      (d/transact src [[:db/retractEntity [:c "hello"]]])
      (let [path (str (System/getProperty "java.io.tmpdir") "/dh-schema-evo-" (utils/get-time))
            _   (m/export-db @src path {:history? true})
            tgt (utils/setup-db (mem-cfg {:history? true}))
            report (m/import-db tgt path {})]
        (is (:verified? report))
        (is (= (user-triples src) (user-triples tgt))
            "content identical despite schema added across history")
        ;; the target's final schema knows all three attributes
        (is (= #{[:a] [:b] [:c]}
               (d/q '[:find ?id :where [?e :db/ident ?id]
                      [(contains? #{:a :b :c} ?id)]] @tgt)))
        (teardown src)
        (teardown tgt)))))

;; ---------------------------------------------------------------------------
;; Scale / streaming — tiny :sort-buffer and :chunk-size force many spill runs
;; and many chunks, exercising external-sort + streaming import end to end.

(deftest streaming-scale-test
  (testing "many-run external sort + multi-chunk streaming import round-trips"
    (let [src (utils/setup-db (mem-cfg {:history? true}))
          n   4000]
      (d/transact src [{:db/ident :k :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                       {:db/ident :s :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
      ;; several transactions so history has many tx entities too
      (doseq [batch (partition-all 500 (range n))]
        (d/transact src (mapv (fn [i] {:k i :s (str "v" i)}) batch)))
      (let [dir (str (System/getProperty "java.io.tmpdir") "/dh-scale-" (utils/get-time))
            manifest (m/export-db @src dir {:history? true
                                            :sort-buffer 300 :chunk-size 250})
            tgt (utils/setup-db (mem-cfg {:history? true}))
            report (m/import-db tgt dir {:batch-size 200})]
        (is (> (count (:chunks manifest)) 1) "produced multiple chunks")
        (is (:verified? report) "streamed import verifies")
        (is (= (:count (:semantic-digest manifest)) (:datom-count report)))
        (is (= n (count (d/q '[:find ?i :where [?e :k ?i]] @tgt))) "all entities present")
        (is (= (user-triples src) (user-triples tgt)) "content identical at scale")
        (teardown src)
        (teardown tgt)))))

;; ---------------------------------------------------------------------------
;; Memory estimation — tell the operator how much heap to give an import

(deftest estimate-import-memory-test
  (testing "estimate reports a recommended heap that scales with entity count"
    (let [make (fn [n]
                 (let [c (utils/setup-db (mem-cfg {:history? true}))]
                   (d/transact c [{:db/ident :k :db/valueType :db.type/long
                                   :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
                   (doseq [b (partition-all 500 (range n))]
                     (d/transact c (mapv (fn [i] {:k i}) b)))
                   c))
          small (make 200)
          big   (make 2000)
          ps    (str (System/getProperty "java.io.tmpdir") "/dh-est-s-" (utils/get-time))
          pb    (str (System/getProperty "java.io.tmpdir") "/dh-est-b-" (utils/get-time))
          _     (m/export-db @small ps {:history? true})
          man-b (m/export-db @big pb {:history? true})
          es    (m/estimate-import-memory ps)
          eb    (m/estimate-import-memory pb)]
      (is (pos? (:recommended-heap-bytes es)))
      (is (string? (:recommended-heap es)) "human-readable heap string present")
      (is (contains? es :sufficient?))
      (is (>= (:entities eb) (:entities es)) "more entities reported for the bigger db")
      (is (> (:id-map-bytes eb) (:id-map-bytes es)) "id-map estimate grows with entities")
      ;; the manifest carries the stats the estimate relies on (no scan needed)
      (is (number? (:max-eid (:stats man-b))) "manifest :stats has :max-eid")
      (is (number? (:max-tx (:stats man-b))) "manifest :stats has :max-tx")
      (teardown small)
      (teardown big))))

;; ---------------------------------------------------------------------------
;; External store target — export/import through a konserve store (stands in for
;; S3 / S3-compatible; the only difference in prod is the backend config).

(deftest store-target-roundtrip-test
  (testing "export/import through a konserve store, all value types + history"
    (let [store  (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
          target {:store store :prefix "backup-1"}
          src    (utils/setup-db (mem-cfg {:history? true}))
          _      (populate-rich! src)
          man    (m/export-db @src target {:history? true :chunk-size 4})
          est    (m/estimate-import-memory target)
          tgt    (utils/setup-db (mem-cfg {:history? true}))
          rep    (m/import-db tgt target {})]
      (is (> (count (:chunks man)) 1) "wrote multiple chunk keys")
      (is (:verified? rep) "store import verifies")
      (is (= (:count (:semantic-digest man)) (:datom-count rep)))
      (is (= (user-triples src) (user-triples tgt)) "content identical via store round-trip")
      (is (string? (:recommended-heap est)) "estimate works against a store target")
      (teardown src)
      (teardown tgt))))

;; ---------------------------------------------------------------------------
;; No-scratch streaming export (:sort? false) — for hard read-only / diskless
;; targets. Two lazy :eavt passes, no temp files, straight to the target.

(deftest no-scratch-streaming-export-test
  (testing ":sort? false round-trips (file + store) with no scratch"
    ;; filesystem, every value type + history
    (let [src (utils/setup-db (mem-cfg {:history? true}))
          _   (populate-rich! src)
          path (str (System/getProperty "java.io.tmpdir") "/dh-nosort-" (utils/get-time))
          _   (m/export-db @src path {:history? true :sort? false})
          tgt (utils/setup-db (mem-cfg {:history? true}))
          rep (m/import-db tgt path {})]
      (is (:verified? rep) "no-scratch export verifies")
      (is (= (user-triples src) (user-triples tgt)) "content identical, unsorted order")
      (teardown src)
      (teardown tgt))
    ;; straight to a konserve store with no scratch (the diskless path)
    (let [store  (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
          target {:store store :prefix "diskless"}
          src (utils/setup-db (mem-cfg {:history? true :attribute-refs? true}))
          _   (d/transact src [{:db/ident :name :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                               {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])
          _   (d/transact src [{:db/id "a" :name "Alice"} {:db/id "b" :name "Bob" :pal "a"}])
          _   (m/export-db @src target {:history? true :sort? false :chunk-size 4})
          tgt (utils/setup-db (mem-cfg {:history? true :attribute-refs? true}))
          rep (m/import-db tgt target {})]
      (is (:verified? rep) "no-scratch export to store, attribute-refs, verifies")
      (is (= (user-triples src) (user-triples tgt)))
      (teardown src)
      (teardown tgt))))

;; ---------------------------------------------------------------------------
;; Tiered verify (0–3) and per-datom :on-error :collect

(deftest verify-tiers-test
  (testing "verify confirms a faithful import (tiers 1–3) and catches divergence"
    (let [src  (utils/setup-db (mem-cfg {:history? true}))
          _    (populate-rich! src)
          path (str (System/getProperty "java.io.tmpdir") "/dh-verify-" (utils/get-time))
          _    (m/export-db @src path {:history? true})
          tgt  (utils/setup-db (mem-cfg {:history? true}))
          _    (m/import-db tgt path {:verify? false})
          ok   (m/verify-against @tgt path)]
      (is (:ok? ok))
      (is (get-in ok [:tier1 :match?]))
      (is (get-in ok [:tier2 :match?]) "id-independent value digest + ref topology match")
      (is (:ok? (:tier3 ok)) "sampled structural diff clean")
      (d/transact tgt [{:name "extra-entity" :score 1.0}])
      (let [bad (m/verify-against @tgt path)]
        (is (not (:ok? bad)))
        (is (not (get-in bad [:tier2 :match?])) "tier2 catches the extra content"))
      (teardown src)
      (teardown tgt))))

(deftest on-error-collect-test
  (testing ":collect skips exactly the bad datom and lands the rest; :abort throws"
    (let [src  (utils/setup-db (mem-cfg {:history? true}))
          _    (d/transact src [{:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
          _    (d/transact src [{:db/id "a" :age 30}])
          path (str (System/getProperty "java.io.tmpdir") "/dh-collect-" (utils/get-time))
          man  (m/export-db @src path {:history? true})
          good-count (:count (:semantic-digest man))
          ;; inject a datom load-entities rejects — a numeric attribute in a
          ;; non-ref db — INTO the same tx as the good data datoms, so the
          ;; per-tx/per-datom narrowing re-attempts good datoms after the failed
          ;; batch attempt (exercising the writer's failure-atomicity).
          ;; APPEND the bad record's CBOR bytes to the last chunk. A CBOR
          ;; sequence has no delimiter, so appending one more encoded item is
          ;; exactly how you add a record.
          ;;
          ;; The manifest's per-chunk SHA-256 is then recomputed, because it is
          ;; now a real check: a dump is a directory, and `open-dump` verifies
          ;; every chunk before the import touches the database. Leaving it stale
          ;; would make this test fail as a CHECKSUM error and never reach the
          ;; :on-error behaviour it exists to exercise. (The flat format skipped
          ;; chunk verification, which is how it went unnoticed that this fixture
          ;; was producing a dump that did not describe itself.)
          ;; A gzip chunk is a sequence of MEMBERS, and concatenated members are
          ;; one valid stream, so the bad record is appended as its own member —
          ;; which is exactly how the writer appends a block.
          _    (let [chunk (io/file path (:file (last (:chunks man))))
                     codec (:compression man)
                     bad (mcbor/encode-record [9999 42 "x" 536870914 true])]
                 (with-open [out (java.io.FileOutputStream. chunk true)]
                   (.write out ^bytes (mz/compress-bytes codec bad)))
                 (let [mf (io/file path "manifest.edn")
                       m0 (read-string (slurp mf))
                       last-ix (dec (count (:chunks m0)))
                       ;; :sha256 is over the RECORDS, so hash the decompressed
                       ;; content rather than the file
                       sha (dig/sha256-hex (mz/decompress-bytes
                                            codec
                                            (java.nio.file.Files/readAllBytes (.toPath chunk))))]
                   (spit mf (pr-str (-> m0
                                        (assoc-in [:chunks last-ix :sha256] sha)
                                        (assoc-in [:chunks last-ix :bytes] (.length chunk)))))))]
      (testing ":abort halts with the offending datom"
        (let [t1 (utils/setup-db (mem-cfg {:history? true}))]
          ;; `:import/malformed-record`, not `:import/corrupt-datom`: the fixture
          ;; injects `[9999 42 "x" …]`, whose attribute is a NUMBER. A dump never
          ;; carries numeric attributes — `datom->record` resolves them to keyword
          ;; idents even in an attribute-refs database — so the record is
          ;; malformed by the format's own contract and is now refused at the
          ;; record seam rather than deep inside the transactor. What is being
          ;; tested here is unchanged: :abort halts on a bad record.
          (is (= :import/malformed-record
                 (try (m/import-db t1 path {:on-error :abort :verify? false}) nil
                      (catch clojure.lang.ExceptionInfo ex (:error (ex-data ex))))))
          (teardown t1)))
      (testing ":collect records only the bad datom, imports everything else"
        (let [t2  (utils/setup-db (mem-cfg {:history? true}))
              rep (m/import-db t2 path {:on-error :collect :verify? false})]
          (is (= 1 (count (:errors rep))) "exactly one datom collected")
          (is (= [9999 42 "x" 536870914 true] (:datom (first (:errors rep)))))
          (is (= 30 (d/q '[:find ?a . :where [?e :age ?a]] @t2)) "good data still imported")
          (is (= good-count
                 (count (filter #(> (datom/datom-tx %) c/tx0)
                                (d/datoms (d/history @t2) :eavt))))
              "every good datom landed EXACTLY once — a failed load-entities call applies nothing, so the narrowing retry cannot double-apply")
          (teardown t2)))
      (teardown src))))

;; ---------------------------------------------------------------------------
;; Schema-on-read databases (no declared schema; codec keys on runtime class)

(deftest schema-on-read-roundtrip-test
  (testing "a :schema-flexibility :read db round-trips with exact value classes"
    (let [cfg  (fn [] {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                       :keep-history? true :schema-flexibility :read})
          src  (utils/setup-db (cfg))
          _    (d/transact src [{:db/id 1 :name "Alice" :score 0.0 :ratio (float 0.5)
                                 :blob (byte-array [3 1 4])}
                                {:db/id 2 :name "Bob" :score 2.5}])
          path (str (System/getProperty "java.io.tmpdir") "/dh-sor-" (utils/get-time))
          _    (m/export-db @src path {:history? true})
          tgt  (utils/setup-db (cfg))
          rep  (m/import-db tgt path {})]
      (is (:verified? rep))
      (is (= (user-triples src) (user-triples tgt)))
      (let [v (fn [conn n a] (d/q [:find '?v '. :where ['?e :name n] ['?e a '?v]] @conn))]
        (is (= Double (class (v tgt "Alice" :score))) "double stays Double without schema")
        (is (= Float  (class (v tgt "Alice" :ratio))) "float stays Float without schema"))
      (teardown src)
      (teardown tgt))))

;; ---------------------------------------------------------------------------
;; store-ref blobs

(defn- blob-fixture
  "A file-backed db with one in-store blob, plus one store-ref whose bytes were
   never written here — i.e. a blob living outside this store, which is the case
   a dump cannot carry."
  []
  (let [path (str "/tmp/dh-blob-test-" (java.util.UUID/randomUUID))
        cfg  {:store {:backend :file :path path :id (java.util.UUID/randomUUID)}
              :schema-flexibility :write :keep-history? true}
        _    (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :doc/name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :doc/file :db/valueType :db.type/store-ref
                       :db/cardinality :db.cardinality/one}])
    (let [payload (.getBytes "the actual blob payload" "UTF-8")
          id      (blob/blob-id payload)
          ext-id  (blob/blob-id (.getBytes "lives in a raw bucket" "UTF-8"))]
      (<?? S (k/bassoc (:store @conn) id payload))
      (d/transact conn [{:doc/name "carried" :doc/file id}])
      {:conn conn :cfg cfg :payload payload :id id :ext-id ext-id})))

(defn- read-blob-from-store [store id]
  (<?? S (k/bget store id
                 (fn [{:keys [input-stream]}]
                   (go (when input-stream
                         (let [bos (java.io.ByteArrayOutputStream.)]
                           (io/copy input-stream bos)
                           (.toByteArray bos))))))))

(deftest store-ref-blobs-are-carried-test
  ;; A store-ref datom holds a content id; exporting it exports the REFERENCE.
  ;; Without carrying the bytes the datom restores perfectly and names an object
  ;; that is not in the target store — a backup that silently lost its blobs.
  (let [{:keys [conn payload id]} (blob-fixture)
        dump (str "/tmp/dh-blob-dump-" (java.util.UUID/randomUUID))]
    (.mkdirs (io/file dump))
    (let [manifest (m/export-db @conn dump {})]
      (testing "the manifest declares what it carries"
        (is (= true (:self-contained? (:store-refs manifest))))
        (is (= 1 (:carried-count (:store-refs manifest))))
        (is (= [id] (:carried (:store-refs manifest)))))
      (testing "the bytes are in the dump, named by their content id"
        ;; the file name IS the checksum, so verification needs no side table
        (is (.exists (io/file dump mblobs/dir-name (str id))))))
    (let [path2 (str "/tmp/dh-blob-target-" (java.util.UUID/randomUUID))
          cfg2  {:store {:backend :file :path path2 :id (java.util.UUID/randomUUID)}
                 :schema-flexibility :write :keep-history? true}
          _     (d/create-database cfg2)
          conn2 (d/connect cfg2)
          rep   (m/import-db conn2 dump {})]
      (testing "datoms and blobs both arrive"
        (is (:verified? rep))
        (is (= #{["carried" id]}
               (d/q '[:find ?n ?f :where [?e :doc/name ?n] [?e :doc/file ?f]] (d/db conn2))))
        (is (= (seq payload) (seq (read-blob-from-store (:store @conn2) id)))
            "the referent must be present, not merely the reference")))))

(deftest store-ref-blobs-outside-the-store-test
  ;; `:db.type/store-ref` says WHAT an object is, never where it lives: bytes in a
  ;; raw bucket a browser PUT to never transit this JVM. We cannot copy those, so
  ;; the dump must say so rather than appear complete.
  (let [{:keys [conn ext-id]} (blob-fixture)]
    (d/transact conn [{:doc/name "external" :doc/file ext-id}])
    (let [plan (mblobs/plan @conn (:store @conn) {:sync? true})]
      (is (= [ext-id] (:external plan)))
      (is (false? (:self-contained? plan)))
      (testing "import refuses a dump it cannot fully honour"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not self-contained"
                              (mblobs/check-importable plan {}))))
      (testing "…unless the operator accepts it explicitly"
        (is (nil? (mblobs/check-importable plan {:accept-external-blobs? true})))))))

(deftest store-ref-blob-corruption-is-detected-test
  ;; Blobs are content-addressed, so a wrong object under a content-addressed key
  ;; would be trusted by every later reader. Verify on the way in.
  (let [{:keys [conn id]} (blob-fixture)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not match its content id"
                          (mblobs/copy-in! (:store @conn) {:carried [id]}
                                           (fn [_ _] (.getBytes "tampered" "UTF-8"))
                                           {:sync? true})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"missing a blob it declares"
                          (mblobs/copy-in! (:store @conn) {:carried [id]} (fn [_ _] nil)
                                           {:sync? true})))))

(deftest store-ref-walk-is-skipped-without-store-ref-attrs-test
  ;; The reachability walk needs a FLUSHED index, and plenty of legitimate
  ;; exports are of an unflushed in-memory db. Gating on the schema keeps those
  ;; working — and makes the common case pay nothing.
  (let [db (d/db-with (datahike.db/empty-db {:name {:db/cardinality :db.cardinality/one}})
                      [{:name "no blobs here"}])]
    (is (false? (mblobs/schema-has-store-refs? db)))))

;; ---------------------------------------------------------------------------
;; provenance + capabilities

(deftest dump-declares-capabilities-test
  ;; A version stamp is the right check for a STORE — `connector/version-check`
  ;; refuses one written by a newer datahike, because the on-disk index is not
  ;; forward compatible. A dump is logical, so a v3 dump using no v3-only feature
  ;; IS readable by v2, and refusing it on the stamp would work against
  ;; datahike's backwards-compat commitment. So the dump declares what it NEEDS,
  ;; and the reader compares against what it HAS.
  (let [{:keys [conn id]} (blob-fixture)
        dump (str "/tmp/dh-cap-dump-" (java.util.UUID/randomUUID))]
    (d/transact conn [{:db/ident :doc/vec :db/valueType :db.type/double-array
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:doc/name "vec" :doc/vec (double-array [1.5 2.5])}])
    (.mkdirs (io/file dump))
    (let [manifest (m/export-db @conn dump {})
          requires (set (:requires manifest))]
      (testing "provenance rides in the same shape the store uses"
        (is (contains? (:datahike/meta manifest) :datahike/version))
        (is (contains? (:datahike/meta manifest) :konserve/version)))
      (testing "capabilities are derived from what the dump actually uses"
        (is (contains? requires :datahike.migrate/history))
        (is (contains? requires :datahike.migrate/store-ref-blobs))
        ;; value types come from the schema, not a hand-maintained list — so a
        ;; type added later shows up here automatically
        (is (contains? requires :db.type/double-array))
        (is (contains? requires :db.type/store-ref)))
      (testing "and this version can honour its own dump"
        (is (nil? (m/check-capabilities! manifest)))
        (is (= [id] (:carried (:store-refs manifest))))))))

(deftest unsupported-capability-is-refused-precisely-test
  ;; The point of a capability set over a version number: say WHICH feature is
  ;; missing. "requires :db.type/double-array" is actionable; "newer version" is
  ;; not — and silently importing everything else would drop that attribute.
  (testing "an unknown capability is named"
    (let [e (is (thrown-with-msg?
                 clojure.lang.ExceptionInfo #"cannot interpret"
                 (m/check-capabilities!
                  {:requires [:db.type/string :datahike.migrate/quantum-index]
                   :datahike/meta {:datahike/version "99.0"}})))]
      (is (= [:datahike.migrate/quantum-index] (:missing (ex-data e)))
          "only the unknown capability is reported, not the ones we support")))
  (testing "a dump predating capability declaration reads as before"
    (is (nil? (m/check-capabilities! {})))
    (is (nil? (m/check-capabilities! {:requires []})))))

(deftest verify-covers-blobs-test
  ;; Blobs verify at the SAME tier as the datom chunks. The case that matters is
  ;; a dump whose datoms are perfect and whose `store-refs/` is short: counts
  ;; match, the semantic digest matches, every pre-existing tier passes — and the
  ;; dump is unrestorable. Reporting :ok? true there is exactly the reassurance
  ;; nobody should get.
  (let [{:keys [conn id]} (blob-fixture)
        dump (str "/tmp/dh-verify-blob-" (java.util.UUID/randomUUID))]
    (.mkdirs (io/file dump))
    (m/export-db @conn dump {})
    (testing "an intact dump verifies, and says what it checked"
      (let [v (m/verify dump)]
        (is (:ok? v))
        (is (= 1 (:declared (:blobs v))))
        (is (= 1 (:verified (:blobs v))))
        (is (empty? (:missing (:blobs v)))))
      (is (:ok? (m/verify-against @conn dump))))
    (testing "a missing blob fails verification even though every other tier passes"
      (io/delete-file (io/file dump mblobs/dir-name (str id)))
      (let [v (m/verify-against @conn dump)]
        (is (false? (:ok? v)))
        (is (true? (:match? (:tier1 v))) "datom counts still match")
        (is (true? (:match? (:tier2 v))) "the semantic digest still matches")
        (is (false? (:ok? (:blobs v))) "…and the blob tier is what fails")
        (is (= [id] (:missing (:blobs v))))))
    (testing "a blob with the right name and the wrong bytes is corrupt"
      ;; only detectable because the file NAME is the content hash
      (spit (io/file dump mblobs/dir-name (str id)) "tampered")
      (let [v (m/verify dump)]
        (is (false? (:ok? v)))
        (is (= [id] (:corrupt (:blobs v))))))))

;; ---------------------------------------------------------------------------
;; three claims the code made about itself that were not true

(deftest tx-count-is-exact-under-both-sort-modes
  (testing "the reported transaction count must be the number of transactions.

            It used to be the number of TRANSITIONS between adjacent `t` values
            in the record stream, which equals the transaction count only when
            every transaction's records are contiguous. Under `:sort? false`
            they are not: `export-records-streaming` makes two passes over
            :eavt, emitting a transaction's schema/meta datoms in the first and
            its data datoms in the second. Measured before the fix: 25 reported
            for 13 actual.

            The count now comes from the id-remap's `:tids`, which holds one
            entry per distinct source `t` and is already in memory."
    (let [src (utils/setup-db (mem-cfg {:history? true}))]
      (d/transact src [{:db/ident :name :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                       {:db/ident :n :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}])
      (doseq [i (range 12)]
        (d/transact src [{:name (str "e" i) :n i}]))
      (let [actual (count (distinct (map :tx (d/datoms (d/history @src) :eavt))))]
        (is (= 13 actual) "precondition: 13 transactions (schema + 12)")
        (doseq [sort? [true false]]
          (let [path (str (System/getProperty "java.io.tmpdir") "/dh-txc-"
                          sort? "-" (utils/get-time))
                _ (m/export-db @src path {:history? true :sort? sort?})
                tgt (utils/setup-db (mem-cfg {:history? true}))
                rep (m/import-db tgt path {:verify? false})]
            (is (= actual (:tx-count rep)) (str ":sort? " sort?))
            (teardown tgt))))
      (teardown src))))

(deftest verify-refuses-a-corrupted-store-dump
  (testing "`verify` on a store dump must check the chunks it says it checked.

            It read only the manifest and then reported
            `:tier0 {:checksums :ok}` regardless — so a store dump with a
            corrupted chunk came back `{:ok? true}` from the very call an
            operator makes to find out whether a backup is intact. The identical
            corruption on a filesystem dump threw, because `open-dump` hashes
            every chunk. Both media are pinned here so they cannot drift apart
            again."
    (let [src (utils/setup-db (mem-cfg {:history? true}))]
      (d/transact src [{:db/ident :name :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}])
      (d/transact src [{:name "x"} {:name "y"}])
      (testing "store medium"
        (let [store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                                     {:sync? true})
              target {:store store :prefix "vt"}
              man (m/export-db @src target {:history? true :chunk-size 2})]
          (is (= {:ok? true} (select-keys (m/verify target) [:ok?]))
              "an intact store dump verifies")
          (k/bassoc store ["datahike.migrate" "vt" (:file (first (:chunks man)))]
                    (byte-array (map unchecked-byte (repeat 40 0x77))) {:sync? true})
          (let [r (m/verify target)]
            (is (false? (:ok? r))
                "a corrupted store dump is not certified intact")
            (is (contains? #{:import/checksum-failed :import/corrupt-chunk}
                           (:error (:integrity r)))
                "and it is REPORTED by a named datahike error rather than thrown —
                 which error depends on whether the damage survives decompression.
                 It used to throw; `verify` is the call an operator makes to ask
                 whether a backup is intact, and a finding answers that question
                 while an exception only ends it."))))
      (testing "filesystem medium, same corruption"
        (let [path (str (System/getProperty "java.io.tmpdir") "/dh-vfs-" (utils/get-time))
              man (m/export-db @src path {:history? true :chunk-size 2})
              chunk (io/file path (:file (first (:chunks man))))]
          (with-open [o (java.io.FileOutputStream. chunk)]
            (.write o (byte-array (map unchecked-byte (repeat 40 0x77)))))
          (let [r (m/verify path)]
            (is (false? (:ok? r)))
            (is (contains? #{:import/checksum-failed :import/corrupt-chunk}
                           (:error (:integrity r)))
                "the same answer on both media, which is what this test exists for"))))
      (teardown src))))

(deftest a-non-empty-target-is-refused-before-any-blob-is-written
  (testing "the guard rails run before `restore-blobs!`, not after.

            `run-import` performed them under a comment reading \"all before
            touching the db\" while `import-db` had already restored the blobs —
            so importing a blob-carrying dump into a non-empty database wrote
            every blob into the target store and only then refused. Content-
            addressed and unreferenced, so the GC reclaims them, but on a large
            blob set that is gigabytes written to be told the target was never
            eligible."
    (let [{:keys [conn payload id]} (blob-fixture)
          dump (str "/tmp/dh-guard-dump-" (java.util.UUID/randomUUID))]
      (.mkdirs (io/file dump))
      (m/export-db @conn dump {})
      (let [path2 (str "/tmp/dh-guard-target-" (java.util.UUID/randomUUID))
            cfg2  {:store {:backend :file :path path2 :id (java.util.UUID/randomUUID)}
                   :schema-flexibility :write :keep-history? true}
            _     (d/create-database cfg2)
            conn2 (d/connect cfg2)]
        ;; make the target non-empty
        (d/transact conn2 [{:db/ident :other :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}])
        (is (= :import/non-empty-target
               (try (m/import-db conn2 dump {}) nil
                    (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))
            "the import is refused")
        (is (nil? (read-blob-from-store (:store @conn2) id))
            "and the dump's blob was never written into the target store")
        (d/release conn2)
        (d/delete-database cfg2))
      (d/release conn))))

;; ---------------------------------------------------------------------------
;; compression

(deftest gzip-is-the-default-and-round-trips
  (testing "a dump compresses by default, names its chunks `.gz`, and restores
            identically. Measured on a real database: ~7x, because the
            redundancy is ACROSS records — repeated attribute idents, sequential
            entity ids, shared transaction ids — which CBOR's own stringref
            cannot reach at one-record-per-item granularity."
    (let [src (utils/setup-db (mem-cfg {:history? true}))
          _   (populate-rich! src)
          dir (str (System/getProperty "java.io.tmpdir") "/dh-gz-" (utils/get-time))
          man (m/export-db @src dir {:history? true})]
      (is (= :gzip (:compression man)) "gzip without being asked")
      (is (every? #(re-find #"\.cbor\.gz$" (:file %)) (:chunks man))
          "chunk files say what they are, so `gzip -d` works on them")
      (is (every? #(.exists (io/file dir (:file %))) (:chunks man)))
      (let [tgt (utils/setup-db (mem-cfg {:history? true}))
            rep (m/import-db tgt dir {})]
        (is (:verified? rep))
        (is (= (user-triples src) (user-triples tgt)))
        (teardown tgt))
      (teardown src))))

(deftest the-digest-is-over-records-so-codecs-compare-equal
  (testing "THE property that makes compression a transport detail.

            `:sha256` and the semantic digest cover the uncompressed records, so
            the same database exported with and without gzip produces the same
            hashes. Without this a dump would only verify against one written by
            an identical encoder — and compressed output is not stable even
            within one runtime (nodejs/node#58392), let alone across JVM and
            Node."
    (let [src (utils/setup-db (mem-cfg {:history? true}))
          _   (populate-rich! src)
          d1  (str (System/getProperty "java.io.tmpdir") "/dh-cz-none-" (utils/get-time))
          d2  (str (System/getProperty "java.io.tmpdir") "/dh-cz-gzip-" (utils/get-time))
          m1  (m/export-db @src d1 {:history? true :chunk-size 5 :compression :none})
          m2  (m/export-db @src d2 {:history? true :chunk-size 5 :compression :gzip})]
      (is (= (:semantic-digest m1) (:semantic-digest m2))
          "the semantic digest does not depend on the codec")
      (is (= (mapv :sha256 (:chunks m1)) (mapv :sha256 (:chunks m2)))
          "nor do the per-chunk hashes")
      (is (not= (mapv :bytes (:chunks m1)) (mapv :bytes (:chunks m2)))
          "while :bytes DOES describe what was stored, so a restore can be sized")
      (testing "and each imports into the other's shape"
        (doseq [d [d1 d2]]
          (let [tgt (utils/setup-db (mem-cfg {:history? true}))]
            (is (:verified? (m/import-db tgt d {})))
            (is (= (user-triples src) (user-triples tgt)))
            (teardown tgt))))
      (teardown src))))

(deftest a-tampered-gzip-chunk-is-caught
  (testing "corruption is caught whether it survives decompression or not — a
            flipped byte usually breaks the gzip member itself, and if it does
            not, the record hash catches it."
    (let [src (utils/setup-db (mem-cfg {}))
          _   (do (d/transact src [{:db/ident :n :db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/one}])
                  (d/transact src [{:n "alpha"} {:n "beta"}]))
          dir (str (System/getProperty "java.io.tmpdir") "/dh-gztamper-" (utils/get-time))
          man (m/export-db @src dir {})
          chunk (io/file dir (:file (first (:chunks man))))
          content (java.nio.file.Files/readAllBytes (.toPath chunk))]
      ;; flip a byte in the deflate payload, past the 10-byte gzip header
      (aset-byte content 15 (byte (bit-xor (aget content 15) 0x5a)))
      (with-open [out (io/output-stream chunk)] (.write out content))
      (let [tgt (utils/setup-db (mem-cfg {}))]
        (is (contains? #{:import/checksum-failed :import/corrupt-chunk}
                       (try (m/import-db tgt dir {}) nil
                            (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))
            "a corrupted compressed chunk is refused by a NAMED error, not a
             raw ZipException")
        (teardown tgt))
      (teardown src))))

(deftest an-unknown-codec-is-refused-by-name
  (testing "a dump written by a future datahike with a codec we do not have must
            say so, rather than failing inside a decoder."
    (let [src (utils/setup-db (mem-cfg {}))
          _   (d/transact src [{:db/ident :n :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one}])
          dir (str (System/getProperty "java.io.tmpdir") "/dh-codec-" (utils/get-time))
          _   (m/export-db @src dir {})
          mf  (io/file dir "manifest.edn")]
      (spit mf (pr-str (assoc (read-string (slurp mf)) :compression :brotli)))
      (let [tgt (utils/setup-db (mem-cfg {}))]
        (is (= :import/unsupported-compression
               (try (m/import-db tgt dir {}) nil
                    (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
        (teardown tgt))
      (teardown src))))

;; ---------------------------------------------------------------------------
;; Option guards at the public entry
;;
;; Both refuse by name before any work starts. That placement is the point: an
;; export that fails inside the block compressor or the chunk writer has already
;; created files, and the caller is left with a partial directory and an error
;; naming neither the option nor the value they passed.

(deftest export-refuses-a-codec-it-cannot-write
  (testing "`mz/supported` was the READ guard only — a dump naming an unknown
            codec is refused by name. Writing had no check, so the value reached
            `mz/compress-bytes`, a `case` with no default, and surfaced as
            `No matching clause: :zstd` from inside the compressor, with the
            first chunk's .tmp file already on disk."
    (let [conn (utils/setup-db (mem-cfg {:history? true}))
          path (str (System/getProperty "java.io.tmpdir") "/dh-codec-" (utils/get-time))]
      (try
        (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:n 1}])
        (doseq [bad [:zstd :brotli "gzip"]]
          (testing (str "codec " (pr-str bad))
            (let [e (try (m/export-db @conn path {:compression bad}) nil
                         (catch clojure.lang.ExceptionInfo ex ex))]
              (is (some? e) "must throw")
              (is (= :migrate/unsupported-codec (:error (ex-data e))))
              (is (= bad (:codec (ex-data e))) "and name the value it was given")
              (is (not (.exists (io/file path)))
                  "and refuse before creating the dump directory"))))
        (testing "nil is refused too, rather than silently meaning `the default`
                  — `export-db` merges its defaults BEFORE the guard, so an
                  explicit nil has already overridden `:gzip`"
          (let [e (try (m/export-db @conn path {:compression nil}) nil
                       (catch clojure.lang.ExceptionInfo ex ex))]
            (is (= :migrate/unsupported-codec (:error (ex-data e))))))
        (testing "and both supported codecs are accepted"
          (doseq [ok [:gzip :none]]
            (let [p (str path "-" (name ok))]
              (is (map? (m/export-db @conn p {:compression ok}))))))
        (finally (teardown conn))))))

(deftest export-refuses-a-non-positive-window
  (testing "`:sort-buffer`, `:chunk-size` and `:batch-size` each drive a
            take/drop recurrence. At zero every pass takes nothing and makes no
            progress while still writing an empty run or chunk each time round,
            so the loop never terminates and the output grows without bound."
    (let [conn (utils/setup-db (mem-cfg {:history? true}))
          path (str (System/getProperty "java.io.tmpdir") "/dh-window-" (utils/get-time))]
      (try
        (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:n 1}])
        (doseq [k [:sort-buffer :chunk-size]
                v [0 -1]]
          (testing (str k " = " v)
            (let [e (try (m/export-db @conn path {k v}) nil
                         (catch clojure.lang.ExceptionInfo ex ex))]
              (is (some? e) "must throw rather than loop")
              (is (= :migrate/bad-size (:error (ex-data e))))
              (is (= k (:option (ex-data e)))))))
        (finally (teardown conn))))))

(deftest verify-refuses-things-that-are-not-dumps
  (testing "`verify` is the call an operator makes to ask whether a backup is
            intact, and it answered `{:ok? true}` for a plain text file.

            `manifest-of` classifies ANY existing non-directory as the legacy
            single-file format and synthesises a manifest with no chunks — so
            there was nothing to checksum and nothing said otherwise. Directories
            failed, but as whatever the read threw: FileNotFoundException on a
            missing manifest.edn, `EOF while reading` on a truncated one. None of
            those answers the question."
    (let [base (str (System/getProperty "java.io.tmpdir") "/dh-notdump-" (utils/get-time))
          err  (fn [f] (try (m/verify f) ::no-throw
                            (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))]
      (testing "a path that does not exist"
        (is (= :import/no-such-dump (err (str base "-missing")))))
      (testing "a plain file — this is the one that reported :ok? true"
        (let [f (io/file (str base "-file"))]
          (spit f "this is not a dump")
          (is (= :import/not-a-dump (err (.getPath f))))))
      (testing "an empty directory"
        (let [d (io/file (str base "-empty"))]
          (.mkdirs d)
          (is (= :import/not-a-dump (err (.getPath d))))))
      (testing "a directory of unrelated files"
        (let [d (io/file (str base "-junk"))]
          (.mkdirs d)
          (spit (io/file d "notes.txt") "hello")
          (is (= :import/not-a-dump (err (.getPath d))))))
      (testing "a manifest.edn that does not parse"
        (let [d (io/file (str base "-badedn"))]
          (.mkdirs d)
          (spit (io/file d "manifest.edn") "{:truncated")
          (is (= :import/not-a-dump (err (.getPath d))))))
      (testing "a well-formed EDN map that is not a datahike manifest — it has
                no format marker, and was previously read as a dump with no
                chunks, i.e. as intact"
        (let [d (io/file (str base "-notours"))]
          (.mkdirs d)
          (spit (io/file d "manifest.edn") (pr-str {:chunks [] :semantic-digest {:count 0}}))
          (is (= :import/not-a-dump (err (.getPath d)))))))))

(deftest verify-still-accepts-a-real-dump
  (testing "the guards above must not refuse the thing they are guarding"
    (let [conn (utils/setup-db (mem-cfg {:history? true}))
          path (str (System/getProperty "java.io.tmpdir") "/dh-realdump-" (utils/get-time))]
      (try
        (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:n 1} {:n 2}])
        (m/export-db @conn path {:history? true})
        (let [r (m/verify path)]
          (is (true? (:ok? r)))
          (is (some? (get-in r [:tier0 :format])) "and reports the format it found")
          (is (pos? (get-in r [:tier1 :manifest-count]))))
        (finally (teardown conn))))))

;; ---------------------------------------------------------------------------
;; A :db.type/ref whose value names a TRANSACTION
;;
;; Filed as "a ref value naming a transaction gets a dangling eid". It is not
;; that. With :keep-history? true it round-trips correctly, and with
;; :keep-history? false the reference is dangling in the SOURCE — datahike keeps
;; no transaction entities there, so it never resolved and the dump is faithful.
;;
;; What IS real is that the two export modes then disagree about what the
;; unresolvable value BECOMES, from byte-identical records. `migrated-eid`
;; consults `:tids` first, and a transaction enters `:tids` when it is first seen
;; as some record's `t`. The sorted export orders by `t` ascending and a ref can
;; only name an earlier-or-equal transaction, so the tx is always known by then.
;; The streaming export emits schema/meta first and data in EAVT order, so a
;; datom referencing tx T can precede every datom FROM tx T — `migrated-eid`
;; answers nil and the "ref not added yet" branch allocates an ENTITY eid.
;;
;; These tests PIN that, they do not endorse it. Phase 2 (`:eids` policy) reworks
;; the id mapping and should make a deliberate choice here — routing a tx-range
;; value through `:tids` is the obvious candidate — at which point the second
;; test must be updated rather than silently drifting.

(defn- tx-ref-db!
  "A database whose entity carries a ref to the transaction that created it."
  [conn]
  (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one
                     :db/unique :db.unique/identity}
                    {:db/ident :asserted-by :db/valueType :db.type/ref
                     :db/cardinality :db.cardinality/one}])
  (let [tx1 (:max-tx (:db-after (d/transact conn [{:db/id -1 :name "a"}])))]
    (d/transact conn [{:db/id [:name "a"] :asserted-by tx1}])
    tx1))

(defn- roundtrip-asserted-by [conn hist? sort?]
  (let [path (str (System/getProperty "java.io.tmpdir") "/dh-txref-" (utils/get-time)
                  "-" sort?)]
    (m/export-db @conn path {:history? hist? :sort? sort?})
    (let [tgt (utils/setup-db (mem-cfg {:history? hist?}))]
      (m/import-db tgt path {})
      (let [db (d/db tgt)
            v  (:v (first (d/datoms db :aevt :asserted-by)))]
        [tgt v (some? (first (d/datoms db :eavt v :db/txInstant)))]))))

(deftest a-ref-to-a-transaction-survives-when-history-is-kept
  (testing "the transaction entity is in the dump, so the ref resolves to it —
            in BOTH export modes. This is the property that must not regress."
    (doseq [sort? [true false]]
      (let [conn (utils/setup-db (mem-cfg {:history? true}))]
        (try
          (tx-ref-db! conn)
          (let [[tgt v resolves?] (roundtrip-asserted-by conn true sort?)]
            (is (>= v c/tx0) (str ":sort? " sort? " — value stays in the tx range"))
            (is (true? resolves?)
                (str ":sort? " sort? " — and names a transaction that exists"))
            (teardown tgt))
          (finally (teardown conn)))))))

(deftest without-history-the-two-export-modes-disagree
  (testing "PINNED, NOT ENDORSED — see the comment above.

            :keep-history? false keeps no transaction entities, so this ref is
            already unresolvable in the source and no import can repair it. The
            defect is that the two modes produce DIFFERENT values for it."
    (let [conn (utils/setup-db (mem-cfg {:history? false}))]
      (try
        (tx-ref-db! conn)
        (is (nil? (first (d/datoms (d/db conn) :aevt :db/txInstant)))
            "precondition: the source really has no transaction entities")
        (let [[s-tgt s-v s-res] (roundtrip-asserted-by conn false true)
              [a-tgt a-v a-res] (roundtrip-asserted-by conn false false)]
          (is (false? s-res) "dangling either way — sorted")
          (is (false? a-res) "dangling either way — streaming")
          (is (>= s-v c/tx0)
              ":sort? true keeps it in the tx range, so it still LOOKS like a
               broken transaction reference")
          (is (< a-v c/tx0)
              ":sort? false rewrites it into the ENTITY range — a phantom entity
               rather than a recognisably broken tx ref. This is the asymmetry
               Phase 2 should resolve.")
          (teardown s-tgt) (teardown a-tgt))
        (finally (teardown conn))))))

(deftest the-two-media-write-the-same-dump
  (testing "`datahike.migrate.store`'s docstring claims the store dump's format,
            per-chunk SHA-256 and semantic digest are IDENTICAL to the filesystem
            dump's. Nothing checked it, and it has already been false once —
            `chunk-name` omitted the `.cbor` extension on the store side, so the
            two media wrote different values into the same manifest field and the
            validation that would have caught it is only applied to one of them.

            This is the regression test for that claim, and the prerequisite for
            any refactor that tries to unify the two write paths: it is the only
            thing that would notice a change in chunk boundaries, record order or
            hashing."
    (let [conn (utils/setup-db (mem-cfg {:history? true}))]
      (try
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :score :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :tag :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/many}])
        ;; enough history, and enough shapes, that a boundary or ordering change
        ;; would move something
        (doseq [i (range 30)]
          (d/transact conn [{:db/id -1 :name (str "e" i) :score i :tag :a}]))
        (doseq [i (range 0 30 3)]
          (d/transact conn [{:db/id [:name (str "e" i)] :score (+ 100 i)}]))
        (doseq [i (range 0 30 7)]
          (d/transact conn [[:db/retract [:name (str "e" i)] :tag :a]]))

        (doseq [codec [:gzip :none]
                chunk-size [7 1000]]
          (testing (str "codec " codec ", chunk-size " chunk-size)
            (let [dir   (str (System/getProperty "java.io.tmpdir") "/dh-both-"
                             (utils/get-time) "-" (name codec) "-" chunk-size)
                  store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                                         {:sync? true})
                  opts  {:history? true :compression codec :chunk-size chunk-size}
                  fs-man (m/export-db @conn dir opts)
                  st-man (m/export-db @conn {:store store :prefix "both"} opts)]

              (testing "the semantic digest — the whole-dump content identity"
                (is (= (:semantic-digest fs-man) (:semantic-digest st-man))))

              (testing "the chunk list, field for field"
                (is (= (count (:chunks fs-man)) (count (:chunks st-man)))
                    "same number of chunks, so the boundaries agree")
                (doseq [[a b] (map vector (:chunks fs-man) (:chunks st-man))]
                  (is (= (:file a) (:file b)) "same name — this is what drifted before")
                  (is (= (:sha256 a) (:sha256 b)) "same hash, over the same bytes")
                  (is (= (:count a) (:count b)) "same record count")
                  (is (= (:raw-bytes a) (:raw-bytes b)) "same decoded size")))

              (testing "and the same declared compression"
                (is (= (:compression fs-man) (:compression st-man) codec)))

              (testing "and both import to the same database"
                (let [t1 (utils/setup-db (mem-cfg {:history? true}))
                      t2 (utils/setup-db (mem-cfg {:history? true}))]
                  (m/import-db t1 dir {})
                  (m/import-db t2 {:store store :prefix "both"} {})
                  (is (= (:hash @t1) (:hash @t2)))
                  (is (= (vec (d/datoms (d/history @t1) :eavt))
                         (vec (d/datoms (d/history @t2) :eavt))))
                  (teardown t1) (teardown t2))))))
        (finally (teardown conn))))))

;; ---------------------------------------------------------------------------
;; The read side takes a snapshot, not a connection
;; ---------------------------------------------------------------------------

(deftest the-read-side-refuses-a-connection
  (testing "`export-db`, `export-to-sink` and `verify` take a database SNAPSHOT.
            They used to accept a connection and deref it, which bought one
            character over `@conn` and cost a wider contract than the code
            needs: nothing downstream ever wanted the connection, and accepting
            one hid a choice of TIME behind a call that reads as though it
            exported the connection itself.

            Pinned because a silent coercion is exactly the kind of convenience
            that gets reintroduced, and because the refusal has to NAME the fix
            to be worth having over a NullPointerException somewhere inside."
    (let [cfg  {:store {:backend :memory :id (random-uuid)}
                :keep-history? true
                :schema-flexibility :read}
          _    (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:name "Amara"}])
        (doseq [[label f] {"export-db"      #(m/export-db conn "/tmp/should-not-exist")
                           "export-to-sink" #(m/export-to-sink conn {:open (fn [_] nil)
                                                                     :write (fn [c _] c)
                                                                     :close (fn [_] nil)})
                           ;; `verify` takes no db now — the comparison does.
                           "verify-against" #(m/verify-against conn "/tmp/should-not-exist")}]
          (testing label
            (let [e (is (thrown? clojure.lang.ExceptionInfo (f)))]
              (is (= :datahike/db-expected (:type (ex-data e)))
                  "a typed error, so a caller can dispatch on it")
              (is (str/includes? (ex-message e) "@conn")
                  "the message must name the fix, not just the complaint"))))

        (testing "and the snapshot forms are all accepted — history/as-of/since
                  are snapshots too, so gating on `db?` rather than a class
                  check is what keeps exporting a history view legitimate"
          (let [db @conn]
            (doseq [[label snap] {"db"      db
                                  "history" (d/history db)
                                  "as-of"   (d/as-of db (java.util.Date.))
                                  "since"   (d/since db (java.util.Date. 0))}]
              (testing label
                (is (identical? snap (mman/ensure-db snap "t")))))))
        (finally (d/release conn))))))

(deftest options-are-validated-without-being-closed
  (testing "Two dials, set independently. A wrong VALUE under a known key is
            unambiguous misuse and is refused. An UNKNOWN key is reported and
            tolerated — closing the map would break a caller forwarding opts
            from a newer datahike, and rigidity there buys nothing.

            The motivating case is `:xform`, whose docstring names per-tenant
            dump splitting: `{:xfrom …}` was silently dropped, the export
            SUCCEEDED, the manifest said `:transformed? false` and `verify` said
            `:ok? true` — a dump holding every tenant, certified intact. Absent
            and misspelled were indistinguishable, and absent means \"export
            everything\". The schema does not make that impossible, but it makes
            it loud, and names the key you meant."
    (let [cfg  {:store {:backend :memory :id (random-uuid)}
                :keep-history? true :schema-flexibility :read}
          _    (d/create-database cfg)
          conn (d/connect cfg)
          tmp  #(str (System/getProperty "java.io.tmpdir") "/dh-opts-" (System/nanoTime))]
      (try
        (d/transact conn [{:name "Ines"}])
        (testing "a bad value under a known key is refused, with the key named"
          (let [e (is (thrown? clojure.lang.ExceptionInfo
                               (m/export-db @conn (tmp) {:history? :yes})))]
            (is (= :migrate/invalid-opts (:error (ex-data e))))))

        (testing "where a purpose-built guard already exists it keeps the
                  answer: the schema runs AFTER `assert-sizes-positive!`, so a
                  non-positive size still gets its own specific error rather
                  than a generic schema one. Pinned because the ordering is the
                  whole reason adding the schema was non-breaking."
          (let [e (is (thrown? clojure.lang.ExceptionInfo
                               (m/export-db @conn (tmp) {:chunk-size 0})))]
            (is (= :migrate/bad-size (:error (ex-data e))))))

        (testing "the option sets are per-entry-point, not one shared bag —
                  `:on-error` is an import option, so on export it is an unknown
                  key (reported), while on import a bad value is refused"
          (let [dir (tmp)]
            (m/export-db @conn dir {:history? true})
            (is (map? (m/export-db @conn (tmp) {:on-error :abort}))
                "unknown on export — warned, not fatal")
            (let [e (is (thrown? clojure.lang.ExceptionInfo
                                 (m/import-db conn dir {:on-error :skip})))]
              (is (= :migrate/invalid-opts (:error (ex-data e)))
                  "known on import, and :skip is not one of :abort | :collect"))))

        (testing "an unknown key does NOT stop the export — the map stays open"
          (is (map? (m/export-db @conn (tmp) {:xfrom identity}))
              "a forward-compatible caller must not be broken by a key we do not know"))

        (testing "estimate-import-memory validates its sizes like every sibling.
                  It did not, and an operator sizes -Xmx from what it returns."
          (let [dir (tmp)]
            (m/export-db @conn dir {:history? true})
            (is (thrown? clojure.lang.ExceptionInfo
                         (m/estimate-import-memory dir {:batch-size 0})))))

        (testing "and the write side refuses a snapshot, mirroring the read side"
          (let [dir (tmp)]
            (m/export-db @conn dir {:history? true})
            (let [e (is (thrown? clojure.lang.ExceptionInfo (m/import-db @conn dir {})))]
              (is (= :datahike/conn-expected (:type (ex-data e)))
                  "a one-sided tightening would teach `@conn` and then punish it"))))
        (finally (d/release conn))))))

(deftest verify-and-verify-against-agree-about-a-broken-dump
  (testing "`verify` asks whether a dump is intact; `verify-against` asks that
            AND whether it matches a live database. Splitting them by name was
            not cosmetic: as two arities of one function the first argument
            changed meaning with the arity, so `(verify a b)` gave a reader no
            way to tell whether `b` was the source or the opts map every sibling
            puts there — and the taken leading slot meant `verify` could never
            gain opts at all.

            They also disagreed about the same fault. A corrupt chunk was a
            FINDING from one and a THROWN exception from the other, though the
            docstring argues at length that \"it threw\" is the wrong answer to
            \"is my backup intact?\". This pins that they now agree, and that one
            handler works over either report."
    (let [cfg  {:store {:backend :memory :id (random-uuid)}
                :keep-history? true :schema-flexibility :read}
          _    (d/create-database cfg)
          conn (d/connect cfg)
          dir  (str (System/getProperty "java.io.tmpdir") "/dh-verify-" (System/nanoTime))]
      (try
        (d/transact conn (vec (for [i (range 30)] {:name (str "p" i)})))
        (m/export-db @conn dir {:history? true :chunk-size 5})

        (testing "an intact dump: both say so, and their key sets are identical
                  so a caller can write one handler"
          (is (:ok? (m/verify dir)))
          (is (:ok? (m/verify-against @conn dir)))
          (is (= (set (keys (m/verify dir)))
                 (set (keys (m/verify-against @conn dir))))))

        (testing "and the integrity-only report carries the comparison keys as
                  nil rather than omitting them"
          (let [r (m/verify dir)]
            (is (contains? (:tier1 r) :live-count))
            (is (contains? (:tier1 r) :match?))
            (is (nil? (:tier2 r)))))

        (testing "a corrupt chunk is a FINDING from both, not a throw from one.
                  Corrupting the gzip member rather than the hash is deliberate:
                  `compress/decompress-bytes` says a broken member and a hash
                  mismatch are both corruption, differing only in which the
                  reader notices first — and this was the one that threw."
          (spit (first (filter #(re-find #"datoms-" (.getName %)) (file-seq (io/file dir))))
                "garbage")
          (doseq [[label f] {"verify"         #(m/verify dir)
                             "verify-against" #(m/verify-against @conn dir)}]
            (testing label
              (let [r (f)]
                (is (false? (:ok? r)) "reported, not thrown")
                (is (some? (:integrity r)) "and it says what was wrong")))))
        (finally (d/release conn))))))

(deftest a-failed-reachability-walk-is-not-an-empty-blob-plan
  (testing "`plan` took `(sort (<?- (gc/reachable-store-refs …)))`. `go-try-`
            turns a thrown Exception into a channel value but does NOT cover a
            channel that CLOSES — a JVM Error (an assert; the :test alias runs
            with -ea) or a cljs throw of a non-js/Error — and `<?-` then yields
            nil. `(sort nil)` is `()`, so the loop exited on its first iteration
            and a FAILED walk returned `{:carried [] :external []
            :self-contained? true}`.

            That is not a cosmetic wrong answer: no blob bytes get written, the
            manifest omits `:store-refs`, `restore-blobs!` becomes a no-op and
            `verify` reports `:ok? true` — a dump missing every blob, certified
            intact. `manifest.cljc`'s own docstring records the identical
            outcome reached by a different route, and the 39 tests that caught
            THAT one do not catch this, because the plan is only wrong when the
            walk fails.

            nil is unambiguously failure here: `reachable-store-refs` returns a
            set, `#{}` when a schema has store-refs but no blob values."
    (with-redefs [datahike.gc/reachable-store-refs
                  (fn [& _] (go (throw (AssertionError. "boom"))))]
      ;; `:sync? false` on purpose: under `:sync? true` `async+sync` compiles to
      ;; a plain `let` and `<?-` is identity, so the stub's channel would be
      ;; taken as a VALUE and the test would fail for the wrong reason.
      (let [r (a/<!! (mblobs/plan :fake-db :fake-store {:sync? false}))]
        (is (instance? Throwable r)
            (str "a failed walk must surface, not return a plan; got " (pr-str r)))
        (is (= :export/reachable-store-refs (:op (ex-data r)))
            "and it names the operation that failed, rather than reporting
             a self-contained dump with no blobs")))))
