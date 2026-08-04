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
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.blobs :as mblobs]
            [datahike.migrate.legacy :as mlegacy]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.digest :as dig]
            [datahike.blob :as blob]
            [datahike.db]
            [konserve.core :as k]
            [clojure.core.async :refer [go]]
            [superv.async :refer [<?? S]]
            [clojure.string :as str]
            [konserve.store :as ks]
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
            manifest (m/export-db src dir {:history? true :chunk-size cs})
            tgt (utils/setup-db (mem-cfg {:history? true}))
            report (m/import-db tgt dir {})]
        (is (:verified? report) "post-import verification passes")
        (is (true? (:finalized? report)) "migration state cleared")
        (is (nil? (:migration @tgt)) "finalize-import! removed :migration")
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
          _   (m/export-db src path {:history? true})
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
          _   (m/export-db src path {})
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
          manifest (m/export-db src dir {})
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
          _   (m/export-db src dir {:compression :none})
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
          _   (m/export-db src path {})
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
          m1  (m/export-db src d1 {:history? true :chunk-size 5 :compression :none})
          m2  (m/export-db src d2 {:history? true :chunk-size 5 :compression :none})]
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
      (binding [mlegacy/*import-batch-size* 5]
        (m/import-db conn path))
      (is (= (set (map #(apply datom/datom %) datoms))
             (set (filter #(< (:e %) (:max-tx @conn)) (d/datoms @conn :eavt)))))
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
            _   (m/export-db src path {:history? true})
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
            manifest (m/export-db src dir {:history? true
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
          _     (m/export-db small ps {:history? true})
          man-b (m/export-db big pb {:history? true})
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
          man    (m/export-db src target {:history? true :chunk-size 4})
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
          _   (m/export-db src path {:history? true :sort? false})
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
          _   (m/export-db src target {:history? true :sort? false :chunk-size 4})
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
          _    (m/export-db src path {:history? true})
          tgt  (utils/setup-db (mem-cfg {:history? true}))
          _    (m/import-db tgt path {:verify? false})
          ok   (m/verify tgt path)]
      (is (:ok? ok))
      (is (get-in ok [:tier1 :match?]))
      (is (get-in ok [:tier2 :match?]) "id-independent value digest + ref topology match")
      (is (:ok? (:tier3 ok)) "sampled structural diff clean")
      (d/transact tgt [{:name "extra-entity" :score 1.0}])
      (let [bad (m/verify tgt path)]
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
          man  (m/export-db src path {:history? true})
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
          (is (= :import/corrupt-datom
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
          _    (m/export-db src path {:history? true})
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
      (is (:ok? (m/verify @conn dump))))
    (testing "a missing blob fails verification even though every other tier passes"
      (io/delete-file (io/file dump mblobs/dir-name (str id)))
      (let [v (m/verify @conn dump)]
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
                _ (m/export-db src path {:history? true :sort? sort?})
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
              man (m/export-db src target {:history? true :chunk-size 2})]
          (is (= {:ok? true} (select-keys (m/verify target) [:ok?]))
              "an intact store dump verifies")
          (k/bassoc store ["datahike.migrate" "vt" (:file (first (:chunks man)))]
                    (byte-array (map unchecked-byte (repeat 40 0x77))) {:sync? true})
          (is (contains? #{:import/checksum-failed :import/corrupt-chunk}
                         (try (m/verify target) nil
                              (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))
              "a corrupted store dump is refused, by a NAMED datahike error —
               which one depends on whether the damage survives decompression")))
      (testing "filesystem medium, same corruption"
        (let [path (str (System/getProperty "java.io.tmpdir") "/dh-vfs-" (utils/get-time))
              man (m/export-db src path {:history? true :chunk-size 2})
              chunk (io/file path (:file (first (:chunks man))))]
          (with-open [o (java.io.FileOutputStream. chunk)]
            (.write o (byte-array (map unchecked-byte (repeat 40 0x77)))))
          (is (contains? #{:import/checksum-failed :import/corrupt-chunk}
                         (try (m/verify path) nil
                              (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))))
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
          man (m/export-db src dir {:history? true})]
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
          m1  (m/export-db src d1 {:history? true :chunk-size 5 :compression :none})
          m2  (m/export-db src d2 {:history? true :chunk-size 5 :compression :gzip})]
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
          man (m/export-db src dir {})
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
          _   (m/export-db src dir {})
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
            (let [e (try (m/export-db conn path {:compression bad}) nil
                         (catch clojure.lang.ExceptionInfo ex ex))]
              (is (some? e) "must throw")
              (is (= :migrate/unsupported-codec (:error (ex-data e))))
              (is (= bad (:codec (ex-data e))) "and name the value it was given")
              (is (not (.exists (io/file path)))
                  "and refuse before creating the dump directory"))))
        (testing "nil is refused too, rather than silently meaning `the default`
                  — `export-db` merges its defaults BEFORE the guard, so an
                  explicit nil has already overridden `:gzip`"
          (let [e (try (m/export-db conn path {:compression nil}) nil
                       (catch clojure.lang.ExceptionInfo ex ex))]
            (is (= :migrate/unsupported-codec (:error (ex-data e))))))
        (testing "and both supported codecs are accepted"
          (doseq [ok [:gzip :none]]
            (let [p (str path "-" (name ok))]
              (is (map? (m/export-db conn p {:compression ok}))))))
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
            (let [e (try (m/export-db conn path {k v}) nil
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
        (m/export-db conn path {:history? true})
        (let [r (m/verify path)]
          (is (true? (:ok? r)))
          (is (some? (get-in r [:tier0 :format])) "and reports the format it found")
          (is (pos? (get-in r [:tier1 :manifest-count]))))
        (finally (teardown conn))))))
