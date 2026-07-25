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
   [clojure.core.async :refer [<!!]]))

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
          cfg {:store {:backend :file :path dir :id (java.util.UUID/randomUUID)}
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
          (let [collected (<!! (gc/gc-storage! (d/db conn)))]
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
                {:type :scriptum :path "/tmp/x" :branch "main"} nil))))

  (testing "konserve-backed: refuses rather than returning #{}, which the sweep
            would read as 'delete the index'"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"cannot be marked yet"
         (sec/mark-from-key-map
          {:type :scriptum :backing :konserve :branch "main"} nil)))))

(deftest konserve-backed?-distinguishes-the-two-empty-sets
  (testing "the whole point of the flag: 'nothing of mine is in konserve' and
            'nobody taught me to mark' are both #{} but opposite in meaning"
    (is (true? (sec/konserve-backed? {:backing :konserve})))
    (is (false? (sec/konserve-backed? {:backing :fs})))
    (is (false? (sec/konserve-backed? {})))))
