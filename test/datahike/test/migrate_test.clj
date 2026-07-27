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
            [datahike.migrate.edn :as medn]
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
   {:db/ident :friend :db/valueType :db.type/ref     :db/cardinality :db.cardinality/many}])

(defn- populate-rich! [conn]
  (d/transact conn rich-schema)
  (d/transact conn [{:db/id "a" :name "Alice" :score 0.0 :ratio (float 0.5) :big 1.50M
                     :huge 123456789012345678901234567890N
                     :when #inst "2020-01-01" :id #uuid "00000000-0000-0000-0000-0000000000aa"
                     :sym 'ns/foo :blob (byte-array [0 1 2 127 -1])
                     :vec (float-array [1.0 -2.5 3.25]) :dvec (double-array [9.0 8.5])}
                    {:db/id "b" :name "Bob" :score 3.14 :friend "a"}])
  (d/transact conn [[:db/retractEntity [:name "Bob"]]]))

;; ---------------------------------------------------------------------------
;; T-ROUND / T-TYPE — full round-trip of every value type, flat and chunked

(deftest roundtrip-all-value-types-test
  (doseq [fmt [:flat :chunked]]
    (testing (str "round-trip every value type, history, format " fmt)
      (let [src (utils/setup-db (mem-cfg {:history? true}))
            _   (populate-rich! src)
            dir (str (System/getProperty "java.io.tmpdir") "/dh-rt-" (name fmt) "-" (utils/get-time))
            manifest (m/export-db src dir {:format fmt :history? true :chunk-size 4})
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
    (let [rt (fn [v] (nth (medn/read-record (medn/write-record [1 :x (medn/encode-value v) 2 true])) 2))]
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
          _   (m/export-db src path {:format :flat :history? true})
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
          _   (m/export-db src path {:format :flat})
          tgt (utils/setup-db (mem-cfg {:history? false}))]
      (is (:verified? (m/import-db tgt path {})))
      (is (= #{["x"] ["y"]} (d/q '[:find ?n :where [?e :name ?n]] @tgt)))
      (teardown src)
      (teardown tgt))))

;; ---------------------------------------------------------------------------
;; Security & integrity

(deftest security-eval-and-unknown-tag-test ;; §4.1
  (testing "unknown reader tag fails safely, no evaluation"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown reader tag"
                          (medn/read-record "[1 :a #evil/tag \"x\" 2 true]")))
    (is (= :import/unknown-tag
           (try (medn/read-record "[1 :a #evil/tag \"x\" 2 true]")
                (catch clojure.lang.ExceptionInfo ex (:error (ex-data ex))))))
    (testing "the #=(...) eval probe is refused (never evaluated)"
      (is (thrown? Exception
                   (medn/read-record "[1 :a #=(java.lang.System/exit 1) 2 true]"))))))

(deftest security-bad-chunk-path-test ;; T-SEC-PATH, §4.2
  (testing "a manifest chunk path outside the dump dir is refused before any read"
    (let [src (utils/setup-db (mem-cfg {}))
          _   (do (d/transact src [{:db/ident :n :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
                  (d/transact src [{:n "a"}]))
          dir (str (System/getProperty "java.io.tmpdir") "/dh-path-" (utils/get-time))
          manifest (m/export-db src dir {:format :chunked})
          mf (io/file dir "manifest.edn")
          poisoned (assoc-in (read-string (slurp mf)) [:chunks 0 :file] "../evil.edn")]
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
          _   (m/export-db src dir {:format :chunked})
          chunk (io/file dir "datoms-000001.edn")
          content (slurp chunk)]
      (spit chunk (clojure.string/replace-first content "alpha" "alphX"))
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
          _   (m/export-db src path {:format :flat})
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
          m1  (m/export-db src d1 {:format :chunked :history? true :chunk-size 5})
          m2  (m/export-db src d2 {:format :chunked :history? true :chunk-size 5})]
      (is (= (:semantic-digest m1) (:semantic-digest m2)))
      (is (= (mapv :sha256 (:chunks m1)) (mapv :sha256 (:chunks m2))))
      (is (= (slurp (io/file d1 "datoms-000001.edn"))
             (slurp (io/file d2 "datoms-000001.edn"))))
      (teardown src))))

(deftest legacy-cbor-import-test ;; T-LEGACY, G9
  (testing "an old flat CBOR dump still imports via the legacy path"
    (let [datoms (mapv #(vec (rest %)) tx-data)
          path (str (System/getProperty "java.io.tmpdir") "/dh-legacy-" (utils/get-time))
          conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                :schema-flexibility :read :keep-history? false})]
      (cbor/spit-all path datoms)
      (binding [m/*import-batch-size* 5]
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
