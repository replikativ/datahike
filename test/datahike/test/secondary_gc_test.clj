(ns datahike.test.secondary-gc-test
  "GC must not collect a secondary index's storage.

   `datahike.gc` unions `mark-from-key-map` over every stored secondary
   key-map and treats the result as the reachable set. Whatever that returns is
   the ONLY thing standing between an index and the sweep — anything it omits
   is deleted. Until now nothing exercised that path: `mark-from-key-map` had
   no test at any level, for any backend.

   These tests pin the two halves of the contract. An index whose storage
   konserve does not own contributes nothing and must survive a collection
   anyway. An index that declares konserve backing must be marked — and if
   nobody taught it how, the collection must FAIL rather than quietly report
   the index as garbage."
  (:require
   [clojure.test :refer [deftest testing is]]
   [datahike.api :as d]
   [datahike.gc :as gc]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary :as sec]
   [datahike.index.secondary.scriptum]
   [scriptum.core :as sc]
   [konserve.store :as ks]
   [clojure.core.async :refer [<!!]]))

(def ^:private proximum-available?
  ;; Proximum ships Java 22 class files; load it lazily so this namespace still
  ;; loads on older JVMs and the proximum test skips itself instead.
  (try (require 'datahike.index.secondary.proximum) true
       (catch Throwable _ false)))

(defn- wait-for-index
  "Block until `idx-ident` is instantiated and reports :ready, or give up.

   The index is backfilled asynchronously after the tx that declares it, so a
   test that searched immediately would race the build. Polling the status the
   writer publishes is the coordination datahike actually offers here."
  [conn idx-ident]
  (loop [n 0]
    (let [db (d/db conn)
          status (get-in db [:schema idx-ident :db.secondary/status])]
      (cond
        (and (= :ready status) (get-in db [:secondary-indices idx-ident])) db
        (> n 300) (throw (ex-info "secondary index never became ready"
                                  {:idx idx-ident :status status}))
        :else (do (Thread/sleep 20) (recur (inc n)))))))

(defn- fulltext-hits [db q]
  (let [idx (get-in db [:secondary-indices :idx/fulltext])]
    (es/entity-bitset-cardinality (sec/-search idx {:query q :field :value} nil))))

(deftest gc-leaves-a-path-backed-index-alone
  (testing "a scriptum index on the filesystem contributes no konserve keys, and
            a full collection must still leave it searchable"
    ;; File-backed deliberately: the :memory backend cannot be marked
    ;; ("Index needs to be properly flushed before marking"), so a GC test on it
    ;; would prove nothing about GC.
    (let [dir (str "/tmp/datahike-secondary-gc-" (random-uuid))
          ;; :exclusive, and the collection below passes {:min-age-ms 0}: under
          ;; the default shared ownership the sweep carries a fifteen-minute
          ;; floor, and a test whose whole point is a REAL sweep would pass
          ;; vacuously with everything spared by age.
          cfg {:store {:backend :file :path dir :id (java.util.UUID/randomUUID)}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false
               :schema-flexibility :write}
          path (str dir "-ft")]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (try
          (d/transact conn [{:db/ident :person/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :person/bio
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}])
          (d/transact conn [{:person/name "Alice" :person/bio "Machine learning researcher"}
                            {:person/name "Bob" :person/bio "Database engineer"}])
          (d/transact conn [{:db/ident :idx/fulltext
                             :db.secondary/type :scriptum
                             :db.secondary/attrs [:person/name :person/bio]
                             :db.secondary/config {:path path}}])
          (let [db (wait-for-index conn :idx/fulltext)]
            (is (= 1 (fulltext-hits db "machine"))
                "backfill must have indexed the data that predated the index"))

          ;; Run a real collection over the store the index lives beside.
          (let [collected (<!! (gc/gc-storage! (d/db conn) (java.util.Date.) {:min-age-ms 0}))]
            (is (set? collected) "gc-storage! returns the collected set"))

          ;; The index must still answer, and the database must still be usable.
          (let [db (d/db conn)]
            (is (= 1 (fulltext-hits db "machine"))
                "GC must not have disturbed a path-backed index")
            (is (= 2 (count (d/q '[:find ?e :where [?e :person/name]] db)))
                "and the primary data must be intact"))
          (finally (d/release conn))))
      (d/delete-database cfg))))

(deftest gc-refuses-to-mark-an-unmarkable-konserve-backed-index
  (testing "an index that declares :backing :konserve but implements no marking
            must make the collection FAIL — reporting it as unreachable would
            hand the whole index to the sweep"
    ;; A type nobody has taught to mark. The default method must refuse it.
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"implements no mark-from-key-map"
         (sec/mark-from-key-map {:type :test/unmarkable :backing :konserve} nil))))

  (testing "the same type WITHOUT the declaration marks empty, because an index
            that keeps its bytes elsewhere genuinely contributes nothing"
    (is (= #{} (sec/mark-from-key-map {:type :test/unmarkable} nil)))
    (is (= #{} (sec/mark-from-key-map {:type :test/unmarkable :backing :fs} nil)))))

(deftest scriptum-marking-tracks-where-its-bytes-live
  (testing "path-backed: konserve owns nothing of it, so nothing to mark"
    (is (= #{} (sec/mark-from-key-map
                {:type :scriptum :path "/tmp/x" :branch "main"} nil)))
    (is (= #{} (sec/mark-from-key-map
                {:type :scriptum :backing :filesystem :path "/tmp/x" :branch "main"} nil))
        "-sec-flush now declares :backing :filesystem explicitly"))

  (testing "konserve-backed: delegates to scriptum.konserve/mark — the real
            root set, not #{} (which the sweep reads as 'delete the index')
            and not a refusal (scriptum ships the mark now)"
    ;; A REAL store-backed index, not an empty store: the vacuous version of
    ;; this test passed with the old #{} implementation.
    (let [store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
          cache (str "/tmp/datahike-secondary-gc-cache-" (random-uuid))
          writer (sc/open-store-index store cache "main")]
      (try
        (sc/commit! writer "seed" {})
        (let [marked (sec/mark-from-key-map
                      {:type :scriptum :backing :konserve :branch "main"} store)]
          (is (seq marked)
              "a store holding an index marks its roots — #{} here is the data-loss bug")
          (is (contains? marked [:scriptum :branches])
              "the branch registry is the root that, swept, takes the whole index"))
        (finally
          (when (instance? java.io.Closeable writer)
            (.close ^java.io.Closeable writer)))))))

(deftest proximum-must-not-share-datahikes-store
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [shared-id (java.util.UUID/randomUUID)]
      (testing "declaration: the factory refuses a :store-config naming datahike's store"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"must use its own store"
             (sec/create-index :proximum
                               {:attrs #{:doc/embedding}
                                :dim 4
                                :store-config {:backend :file :path "/tmp/x" :id shared-id}
                                :datahike.index.secondary/primary-store-id shared-id}
                               nil))))
      (testing "collection: a PRE-EXISTING shared key-map refuses to mark as #{}
              — reporting empty would hand the vector index to the sweep"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"names the very store being collected"
             (sec/mark-from-key-map
              {:type :proximum :backing :external :commit-id (java.util.UUID/randomUUID)
               :store-config {:backend :file :path "/tmp/x" :id shared-id}}
              {:datahike/store-id shared-id}))))
      (testing "a distinct store passes both"
        (is (= #{} (sec/mark-from-key-map
                    {:type :proximum :backing :external
                     :store-config {:backend :file :path "/tmp/x" :id (java.util.UUID/randomUUID)}}
                    {:datahike/store-id shared-id})))))))

(deftest gc-leaves-a-stratum-index-in-the-shared-store-alone
  (testing "stratum writes its datasets and PSS nodes into datahike's OWN store;
            its mark walk is the only thing keeping them — a full unfloored
            sweep, a cold reconnect and a query prove it does"
    (let [dir (str "/tmp/datahike-secondary-gc-stratum-" (random-uuid))
          cfg {:store {:backend :file :path dir :id (java.util.UUID/randomUUID)}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false
               :schema-flexibility :write}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (try
          (d/transact conn [{:db/ident :m/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :m/value
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}])
          (d/transact conn (mapv (fn [i] {:m/name (str "m" i) :m/value i}) (range 50)))
          (d/transact conn [{:db/ident :idx/columnar
                             :db.secondary/type :stratum
                             :db.secondary/attrs [:m/name :m/value]}])
          (wait-for-index conn :idx/columnar)
          ;; More commits so the sweep has real work on the primary side too.
          (d/transact conn (mapv (fn [i] {:m/name (str "n" i) :m/value (+ 100 i)}) (range 50)))
          (let [collected (<!! (gc/gc-storage! (d/db conn) (java.util.Date.) {:min-age-ms 0}))]
            (is (set? collected)))
          (finally (d/release conn))))
      ;; Cold: everything the query needs must come back from the swept store.
      (let [conn2 (d/connect cfg)]
        (try
          (is (= 100 (count (d/q '[:find ?e :where [?e :m/name]] (d/db conn2)))))
          (is (= :ready (get-in (d/db conn2) [:schema :idx/columnar :db.secondary/status])))
          (finally (d/release conn2))))
      (d/delete-database cfg))))

(deftest konserve-backed?-distinguishes-the-two-empty-sets
  (testing "the whole point of the flag: 'nothing of mine is in konserve' and
            'nobody taught me to mark' are both #{} but opposite in meaning"
    (is (true? (sec/konserve-backed? {:backing :konserve})))
    (is (false? (sec/konserve-backed? {:backing :fs})))
    (is (false? (sec/konserve-backed? {})))))
