(ns datahike.test.migrate-ident-timeline-test
  "An attribute's ident is a property of its entity that can CHANGE, and a dump
   has to survive that.

   Under `:attribute-refs?` a datom holds the attribute's ENTITY id, so the NAME
   is not in the datom and the exporter looks it up. Looking it up in the current
   database names every historical datom with the FINAL ident — so a datom
   written before a rename is exported carrying a name that did not exist yet,
   while the `:db/ident` records still replay the rename in causal order. The
   importer then meets that datom before the name is installed.

   Without `:attribute-refs?` none of this applies: the datom holds the keyword,
   so the name IS ground truth and nothing is resolved at either end. A keyword
   database can still CARRY a timeline (an import can install one — that path
   bypasses the deferred schema validators) but its datoms record the name they
   were written with, so resolving them through a timeline would emit a name the
   datom does not hold. `ident-timeline` is empty there by construction, and
   `keyword-mode-carries-no-timeline` pins that.

   `migrate-manifest-test` covers `ident-at`, the pure resolution half."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.manifest :as mman]
            [datahike.migrate.cbor :as mcbor]
            [datahike.constants :as const]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn- cfg [refs?]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write :attribute-refs? refs?})

(defn- teardown [conn]
  (let [c (:config @conn)] (d/release conn) (d/delete-database c)))

(defn- declared+used [conn]
  (d/transact conn [{:db/ident :p/name :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])
  (d/transact conn [{:db/id 1000 :p/name "Ann"}])
  conn)

(defn- rename! [conn from to]
  (let [aid (:db/id (d/entity @conn from))]
    ;; retract-then-add: the one spelling that retires the old ident (#966)
    (d/transact conn [[:db/retract aid :db/ident from]
                      [:db/add aid :db/ident to]])
    aid))

(deftest a-renamed-attribute-gets-a-timeline
  (testing "both names, in transaction order, keyed by the attribute ENTITY —
            which is the thing that did not change"
    (let [conn (declared+used (d/connect (doto (cfg true) d/create-database)))]
      (try
        (let [aid (rename! conn :p/name :p/renamed)
              tl (mman/ident-timeline @conn)]
          (is (= [aid] (keys tl)) "only the attribute that moved")
          (is (= [:p/name :p/renamed] (mapv second (get tl aid))))
          (is (apply < (map first (get tl aid))) "ascending by t")
          (testing "and it resolves the name in force at each point"
            (let [[[t1 _] [t2 _]] (get tl aid)]
              (is (= :p/name (mman/ident-at tl aid t1)))
              (is (= :p/renamed (mman/ident-at tl aid t2)))
              (is (= :p/name (mman/ident-at tl aid (dec (long t2))))
                  "a datom written the instant before the rename is still :p/name"))))
        (finally (teardown conn))))))

(deftest an-unrenamed-database-gets-no-timeline
  (testing "a single-entry history says nothing a reader cannot already see, and
            emitting one would put a table in every attribute-refs dump for
            nothing"
    (let [conn (declared+used (d/connect (doto (cfg true) d/create-database)))]
      (try (is (= {} (mman/ident-timeline @conn)))
           (finally (teardown conn))))))

(deftest keyword-mode-carries-no-timeline
  (testing "empty by construction, and that is a CORRECTNESS requirement rather
            than an optimisation — in keyword mode the datom holds the name, so
            resolving through a timeline would emit a name the datom does not
            hold"
    (let [conn (declared+used (d/connect (doto (cfg false) d/create-database)))]
      (try
        (is (= {} (mman/ident-timeline @conn)))
        (testing "and the live path refuses the rename that would create one"
          (is (thrown? Exception (rename! conn :p/name :p/renamed))))
        (finally (teardown conn))))))

(deftest the-manifest-declares-a-timeline-only-when-it-has-one
  (testing "a dump with no renamed attribute is byte-identical to one written
            before this key existed, and an older reader keeps reading it.

            `:datahike.migrate/ident-timeline` is a CAPABILITY rather than a
            `format-version` bump for the reason `dump-requires` states: a dump
            declares what it needs and the reader compares against what it has,
            so a feature nobody used costs nobody anything."
    (doseq [[refs? rename? expect] [[false false false]
                                    [true  false false]
                                    [true  true  true]]]
      (let [conn (declared+used (d/connect (doto (cfg refs?) d/create-database)))
            path (str (System/getProperty "java.io.tmpdir")
                      "/dh-tl-" (java.util.UUID/randomUUID))]
        (try
          (when rename? (rename! conn :p/name :p/renamed))
          (m/export-db @conn path {:history? true})
          (let [man (edn/read-string {:readers *data-readers*}
                                     (slurp (io/file path "manifest.edn")))
                declared? (boolean (some #{:datahike.migrate/ident-timeline}
                                         (:requires man)))]
            (is (= expect (contains? man :ident-timeline))
                (str "manifest key, refs?=" refs? " rename?=" rename?))
            (is (= expect declared?)
                (str "capability, refs?=" refs? " rename?=" rename?)))
          (finally (teardown conn)))))))

;; ---------------------------------------------------------------------------
;; the READER names it, and the two targets want different names

(def ^:private attr-e 100)

(defn- renamed-source-records
  "What a source emits for an attribute renamed at t3: the data datom names the
   attribute by ENTITY, so no naming decision is baked in.

   Reachable through `import-source` rather than `import-db`, deliberately —
   `config-must-match` refuses a dump that crosses `:attribute-refs?`, so the
   keyword arm of `resolve-attr-refs` has no dump path at all. It has a RECORD
   path, which is what the Datomic adapter will use."
  []
  (let [t1 (+ const/tx0 1) t2 (+ const/tx0 2) t3 (+ const/tx0 3)]
    [[t1 :db/txInstant #inst "2021-01-01" t1 true]
     [attr-e :db/ident :p/name t1 true]
     [attr-e :db/valueType :db.type/string t1 true]
     [attr-e :db/cardinality :db.cardinality/one t1 true]
     [t2 :db/txInstant #inst "2021-02-01" t2 true]
     ;; written BEFORE the rename — the datom the old exporter mislabelled
     [200 (mcbor/->AttrRef attr-e) "Ann" t2 true]
     [t3 :db/txInstant #inst "2021-03-01" t3 true]
     [attr-e :db/ident :p/name t3 false]
     [attr-e :db/ident :p/renamed t3 true]
     [(+ const/tx0 4) :db/txInstant #inst "2021-04-01" (+ const/tx0 4) true]
     ;; and one written AFTER it
     [201 (mcbor/->AttrRef attr-e) "Bob" (+ const/tx0 4) true]]))

(def ^:private timeline
  {attr-e [[(+ const/tx0 1) :p/name] [(+ const/tx0 3) :p/renamed]]})

(defn- import-records [refs?]
  (let [conn (d/connect (doto (cfg refs?) d/create-database))]
    (try
      (m/import-source conn (m/records->chunk-src (renamed-source-records))
                       {:sync? true :verify? false :eids :allocate :schema {}
                        :source-meta {:history? true :ident-timeline timeline}})
      {:new (into #{} (map first) (d/q '[:find ?v :where [?e :p/renamed ?v]] @conn))
       :old (into #{} (map first) (d/q '[:find ?v :where [?e :p/name ?v]] @conn))}
      (finally (teardown conn)))))

(deftest an-attribute-refs-target-gets-every-datom-under-the-new-name
  (testing "resolved PERIOD-CORRECT, which is what makes it resolvable at all:
            the datom written before the rename names `:p/name`, installed at
            that point, and the attribute ENTITY carries the data forward when
            the ident moves. Naming it `:p/renamed` — what the old exporter did —
            is the bug: the importer meets it before that name exists."
    (let [{:keys [new old]} (import-records true)]
      (is (= #{"Ann" "Bob"} new) "both datoms answer to the attribute's final name")
      (is (empty? old) "and none to the retired one"))))

(deftest a-keyword-target-gets-them-flattened-to-the-final-name
  (testing "there the datom stores the KEYWORD, so a period-correct name would
            split the attribute across both — the outcome
            `validate-ident-renames!` refuses on the live path. Flattening keeps
            the database queryable; the rename as an EVENT is what is lost, and a
            keyword-attribute database cannot represent one anyway."
    (let [{:keys [new old]} (import-records false)]
      (is (= #{"Ann" "Bob"} new) "both datoms under one name")
      (is (empty? old) "not split across the old one"))))
