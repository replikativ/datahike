(ns datahike.test.migrate-resume-test
  "`{:resume? true}`: an interrupted streaming import continues from the cursor
   its last committed batch recorded, and ends as the uninterrupted import would.

   The oracle is an UNINTERRUPTED import of the same source with the same
   options, compared datom for datom over the full history — entity ids,
   transaction ids, values, added flags — plus `:max-eid`/`:max-tx` and the
   report's verification. Each crash is injected at a different seam:

     between commits   the writer call for batch k throws before applying it
     inside a commit   a konserve write throws partway through a batch's nodes
     in the batcher    the record stream throws between two flushes

   and the interrupted connection is released and a NEW one opened on the same
   file store before resuming, the way a restarted process would."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.constants :as const]
            [datahike.migrate :as m]
            [datahike.migrate.fs :as fs]
            [datahike.writer :as dwriter]
            [konserve.core :as k]
            [konserve.store :as ks]))

;; ---------------------------------------------------------------------------
;; fixtures

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(defn- file-cfg []
  {:store {:backend :file :path (fs/temp-store-path! "dh-resume-test-")
           :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(defn- source-db
  "History worth comparing: overwrites, card-many add/retract, refs, a fully
   retracted entity — over enough transactions for many batches."
  []
  (let [cfg (mem-cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn schema)
    (doseq [k (range 12)]
      (d/transact conn (vec (for [i (range 5)]
                              {:name (str "e" k "-" i) :score (* k i)
                               :tag [(keyword (str "t" i))]}))))
    (doseq [k (range 12)]
      (d/transact conn [{:db/id [:name (str "e" k "-0")] :score (- k)}
                        [:db/retract [:name (str "e" k "-1")] :tag :t1]
                        {:db/id [:name (str "e" k "-2")] :pal [:name (str "e" k "-3")]}]))
    (d/transact conn [[:db/retractEntity [:name "e3-4"]]])
    conn))

(defn- dump!
  "The source as a dump in a konserve memory store, small chunks so a source
   spans many."
  []
  (let [src (source-db)
        store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
        target {:store store :prefix "resume"}]
    (m/export-db @src target {:history? true :chunk-size 40})
    (let [cfg (:config @src)] (d/release src) (d/delete-database cfg))
    target))

(defn- open [cfg] (d/connect cfg))

(defn- create! [cfg] (d/create-database cfg) (open cfg))

(defn- crash!
  "Drop a connection the way a dying process does: whatever release can manage."
  [conn]
  (try (d/release conn) (catch Throwable _ nil)))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (crash! conn) (d/delete-database cfg)))

(defn- fingerprint
  "Everything an import decides: full history, datom for datom, and the id
   high-water marks."
  [db]
  {:history (mapv (juxt :e :a :v :tx :added) (d/datoms (d/history db) :eavt))
   :max-eid (:max-eid db)
   :max-tx (:max-tx db)})

(def ^:private base-opts {:sync? true :batch-size 25})

(defn- reference
  "The uninterrupted import, into a fresh memory store."
  [dump opts]
  (let [conn (create! (mem-cfg))]
    (try
      (let [rep (m/import-db conn dump (merge base-opts opts))]
        (assoc (fingerprint @conn) :report (select-keys rep [:verified? :tx-count :datom-count])))
      (finally (teardown conn)))))

(defn- interrupted-then-resumed
  "Import `dump` into a file store with `inject` active until it throws, crash,
   reconnect, resume (possibly several times). Returns the fingerprint and the
   reports."
  [dump opts inject {:keys [resume-opts] :or {resume-opts {}}}]
  (let [cfg (file-cfg)
        conn (create! cfg)
        first-run (try (inject #(m/import-db conn dump (merge base-opts opts {:resume? true})))
                       :completed
                       (catch Throwable e (or (:error (ex-data e)) :threw)))
        _ (crash! conn)
        conn2 (open cfg)
        cursor (get-in @conn2 [:meta :datahike.import/cursor])]
    (try
      (let [rep (m/import-db conn2 dump (merge base-opts opts resume-opts {:resume? true}))]
        (assoc (fingerprint @conn2)
               :first-run first-run
               :cursor-before cursor
               :resume-report rep
               :report (select-keys rep [:verified? :tx-count :datom-count])))
      (finally (teardown conn2)))))

;; ---------------------------------------------------------------------------
;; crash injection

(defn- fail-writer-call
  "The k-th `load-entities-migrating` throws before reaching the writer: batches
   1..k-1 are committed, batch k is not."
  [k]
  (fn [f]
    (let [n (atom 0)
          orig dwriter/load-entities-migrating]
      (with-redefs [dwriter/load-entities-migrating
                    (fn [& args]
                      (when (= k (swap! n inc))
                        (throw (ex-info "injected: writer unreachable" {:error :test/injected})))
                      (apply orig args))]
        (f)))))

(defn- fail-konserve-write
  "Let `after` commits (on a multi-key store), or `node-after` index-node writes
   (on a per-key store such as `:file`), through, then tear the next commit:
   write the first half of its keys — index nodes, which a commit writes before
   the head — and fail. A store that went away in the middle of a commit, after
   some of its nodes landed and before the head moved. Covers both commit paths:
   one `multi-assoc` per commit on a multi-key store, one `assoc` per key
   otherwise."
  [after node-after]
  (fn [f]
    (let [n (atom 0)
          nodes (atom 0)
          torn? (atom false)
          orig-multi k/multi-assoc
          orig-assoc k/assoc
          err #(ex-info "injected: store write failed" {:error :test/injected})
          finish (fn [res opts]
                   (if (:sync? opts)
                     (throw (err))
                     (async/go (async/<! res) (err))))]
      (with-redefs [k/multi-assoc
                    (fn [store writes metas opts]
                      (if (or @torn? (> (swap! n inc) after))
                        (let [part (vec (take (quot (count writes) 2) writes))]
                          (reset! torn? true)
                          (finish (orig-multi store part (select-keys metas (map first part)) opts) opts))
                        (orig-multi store writes metas opts)))
                    k/assoc
                    (fn
                      ([store key val] (orig-assoc store key val))
                      ([store key val opts] (orig-assoc store key val opts))
                      ([store key val meta opts]
                       (if (and (map? meta) (:immutable? meta)
                                (or @torn? (> (swap! nodes inc) node-after)))
                         (do (reset! torn? true)
                             (finish (async/go nil) opts))
                         (orig-assoc store key val meta opts))))]
        (f)))))

(defn- injected-xform
  "A stateless transducer that throws at raw record `k` — the batcher dies with
   records buffered but not flushed."
  [k]
  (let [n (atom 0)]
    (map (fn [r]
           (when (= k (swap! n inc))
             (throw (ex-info "injected: batcher died" {:error :test/injected})))
           r))))

(defn- no-injection [f] (f))

;; ---------------------------------------------------------------------------

(deftest a-resumed-import-equals-an-uninterrupted-one
  (let [dump (dump!)]
    (doseq [eids [:preserve :offset]
            [label inject fresh?] [["between commits (batch 3)" (fail-writer-call 3) false]
                                   ["between commits (batch 1): nothing committed" (fail-writer-call 1) true]
                                   ["inside a commit" (fail-konserve-write 3 40) false]]]
      (testing (str eids ", " label)
        (let [ref (reference dump {:eids eids})
              r (interrupted-then-resumed dump {:eids eids} inject {})]
          (is (not= :completed (:first-run r)) "precondition: the first run really was interrupted")
          (is (= (:history ref) (:history r)) "the full history, datom for datom")
          (is (= (select-keys ref [:max-eid :max-tx]) (select-keys r [:max-eid :max-tx])))
          (is (= (:report ref) (:report r)) "verified, with the same tx-count and datom-count")
          (is (true? (get-in r [:report :verified?])))
          (if fresh?
            (do (is (nil? (:cursor-before r)) "no batch committed, so no cursor")
                (is (false? (get-in r [:resume-report :resumed?])) "and the second run starts fresh"))
            (do (is (= :in-progress (get-in r [:cursor-before :status])))
                (is (true? (get-in r [:resume-report :resumed?])))
                (is (= :in-progress (get-in r [:resume-report :resumed-from :status]))))))))))

(deftest a-batcher-crash-resumes-from-the-last-committed-batch
  (let [dump (dump!)
        ref (reference dump {:eids :preserve :xform (map identity)})
        r (interrupted-then-resumed dump
                                    {:eids :preserve :xform (injected-xform 170)
                                     :xform-stateless? true :resume-key "identity-v1"}
                                    no-injection
                                    {:resume-opts {:xform (map identity)}})]
    (is (not= :completed (:first-run r)) "precondition: the batcher died")
    (is (= :in-progress (get-in r [:cursor-before :status])) "and a cursor was committed before it")
    (is (pos? (get-in r [:cursor-before :offset] 1)))
    (is (= (:history ref) (:history r)))
    (is (= (:report ref) (:report r)))))

(deftest two-crashes-then-a-resume
  (let [dump (dump!)
        cfg (file-cfg)
        ref (reference dump {:eids :preserve})
        conn (create! cfg)
        opts (merge base-opts {:eids :preserve :resume? true})]
    (is (thrown? Exception ((fail-writer-call 2) #(m/import-db conn dump opts))))
    (crash! conn)
    (let [conn (open cfg)]
      (is (thrown? Exception ((fail-writer-call 4) #(m/import-db conn dump opts))))
      (crash! conn))
    (let [conn (open cfg)
          rep (m/import-db conn dump opts)]
      (is (= (:history ref) (:history (fingerprint @conn))))
      (is (true? (:verified? rep)))
      (teardown conn))))

(deftest resuming-a-complete-import-only-verifies
  (let [dump (dump!)
        conn (create! (mem-cfg))
        opts (merge base-opts {:eids :preserve :resume? true})
        first (m/import-db conn dump opts)
        before (fingerprint @conn)
        again (m/import-db conn dump opts)]
    (is (true? (:verified? first)))
    (is (= :complete (get-in @conn [:meta :datahike.import/cursor :status])))
    (is (= before (fingerprint @conn)) "nothing re-applied")
    (is (true? (:verified? again)))
    (is (= (:tx-count first) (:tx-count again)))
    (teardown conn)))

;; ---------------------------------------------------------------------------
;; a source that splits one transaction across batches (`hard-max`), and a
;; computed mapping, through `import-source`

(def ^:private src-lo 13194139534312)

(defn- big-tx-records
  "Schema, three small transactions, then ONE transaction of 300 records — more
   than `4 × :batch-size`, so the batcher's backstop splits it — then more."
  []
  (let [t (fn [k] (+ const/tx0 1 k))
        tx (fn [k entities]
             (into [[(t k) :db/txInstant (java.util.Date. (+ 1609459200000 (* 1000 k))) (t k) true]]
                   (mapcat (fn [i]
                             (let [e (+ src-lo (* 1000 k) i)]
                               (cond-> [[e :name (str "n" k "-" i) (t k) true]]
                                 (pos? i) (conj [e :pal (dec e) (t k) true])))))
                   (range entities)))]
    (vec (concat [[(t 0) :db/txInstant #inst "2021-01-01" (t 0) true]
                  [100 :db/ident :name (t 0) true]
                  [100 :db/valueType :db.type/string (t 0) true]
                  [100 :db/cardinality :db.cardinality/one (t 0) true]
                  [101 :db/ident :pal (t 0) true]
                  [101 :db/valueType :db.type/ref (t 0) true]
                  [101 :db/cardinality :db.cardinality/one (t 0) true]]
                 (tx 1 5) (tx 2 5) (tx 3 5)
                 (tx 4 150)
                 (tx 5 5) (tx 6 5)))))

(defn- packer []
  (reify clojure.lang.ILookup
    (valAt [this k] (.valAt this k nil))
    (valAt [_ k nf]
      (cond (not (number? k)) nf
            (>= (long k) src-lo) (+ 1000 (- (long k) src-lo))
            :else k))))

(defn- source-opts []
  {:sync? true :batch-size 20 :eids (packer) :resume-key "packer-v1"
   :source-meta {:history? true :expected-count (count (big-tx-records))
                 :source-id :big-tx-fixture}})

(defn- import-source-into [conn extra]
  (m/import-source conn (m/records->chunk-src (big-tx-records) 30) (merge (source-opts) extra)))

(deftest a-split-transaction-resumes-onto-the-same-target-transaction
  (let [ref-conn (create! (mem-cfg))
        _ (import-source-into ref-conn {})
        ref (fingerprint @ref-conn)
        cfg (file-cfg)
        conn (create! cfg)
        ;; batches: schema+tx1-3, then the 300-record tx in pieces of 4×20 = 80
        _ (is (thrown? Exception ((fail-writer-call 4) #(import-source-into conn {:resume? true}))))
        _ (crash! conn)
        conn (open cfg)
        cursor (get-in @conn [:meta :datahike.import/cursor])
        rep (import-source-into conn {:resume? true})]
    (is (some? (:t-split cursor)) "precondition: the crash landed inside the split transaction")
    (is (= (:history ref) (:history (fingerprint @conn)))
        "the rest of it lands on the same target transaction")
    (is (= (:max-tx ref) (:max-tx @conn)))
    (is (true? (:verified? rep)))
    (is (= 7 (:tx-count rep)) "every source transaction counted once")
    (teardown ref-conn)
    (teardown conn)))

;; ---------------------------------------------------------------------------
;; refusals

(defn- refusal [f]
  (try (f) nil (catch Exception e (:error (ex-data e)))))

(deftest what-cannot-be-resumed-is-refused-before-anything-is-written
  (let [dump (dump!)]
    (doseq [[label opts err] [[":allocate keeps its id map in memory only"
                               {} :import/resume-needs-deterministic-mapping]
                              [":eids :allocate spelled out"
                               {:eids :allocate} :import/resume-needs-deterministic-mapping]
                              ["an :xform not declared stateless"
                               {:eids :preserve :xform (map identity)} :import/resume-stateful-xform]
                              ["an :xform without a :resume-key"
                               {:eids :preserve :xform (map identity) :xform-stateless? true}
                               :import/resume-needs-key]
                              ["a function :eids without a :resume-key"
                               {:eids (fn [e] e)} :import/resume-needs-key]
                              [":on-error :collect"
                               {:eids :preserve :on-error :collect} :import/resume-collect-unsupported]
                              [":build-indexes?"
                               {:eids :preserve :build-indexes? true} :import/resume-needs-scratch-dir]]]
      (testing label
        (let [conn (create! (mem-cfg))]
          (is (= err (refusal #(m/import-db conn dump (merge base-opts opts {:resume? true})))))
          (is (empty? (d/q '[:find ?e :where [?e :name _]] @conn)) "and nothing was written")
          (teardown conn))))))

(deftest a-cursor-for-a-different-source-or-options-is-refused
  (let [dump (dump!)
        other (let [src (source-db)
                    store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
                    target {:store store :prefix "other"}]
                (d/transact src [{:name "only-in-other"}])
                (m/export-db @src target {:history? true :chunk-size 40})
                (teardown src)
                target)
        cfg (mem-cfg)
        conn (create! cfg)
        opts (merge base-opts {:eids :preserve :resume? true})]
    (is (thrown? Exception ((fail-writer-call 3) #(m/import-db conn dump opts))))
    (let [before (fingerprint @conn)]
      (is (= :import/resume-mismatch (refusal #(m/import-db conn other opts))) "another dump")
      (is (= :import/resume-mismatch (refusal #(m/import-db conn dump (assoc opts :eids :offset))))
          "other options")
      (is (= before (fingerprint @conn)) "and neither wrote anything")
      (is (true? (:verified? (m/import-db conn dump opts))) "the right source and options still resume"))
    (teardown conn)))

(deftest a-non-empty-target-without-a-cursor-is-still-refused
  (let [dump (dump!)
        conn (create! (mem-cfg))]
    (d/transact conn schema)
    (d/transact conn [{:name "already here"}])
    (is (= :import/non-empty-target
           (refusal #(m/import-db conn dump (merge base-opts {:eids :preserve :resume? true})))))
    (teardown conn)))

(deftest a-batch-size-change-does-not-block-a-resume
  (let [dump (dump!)
        ref (reference dump {:eids :preserve})
        r (interrupted-then-resumed dump {:eids :preserve} (fail-writer-call 3)
                                    {:resume-opts {:batch-size 7}})]
    (is (= (:history ref) (:history r)))
    (is (true? (get-in r [:report :verified?])))))

;; ---------------------------------------------------------------------------
;; review findings (#1128): each a way the first version resumed to a different
;; database, or let something through it should have refused

(deftest a-commit-whose-reply-was-lost-is-not-applied-again
  (testing "The batch committed — cursor included — and then the caller died
            before hearing so. The resume must start AFTER it."
    (let [dump (dump!)
          ref (reference dump {:eids :preserve})
          r (interrupted-then-resumed
             dump {:eids :preserve}
             (fn [f]
               (let [n (atom 0)
                     orig dwriter/load-entities-migrating]
                 (with-redefs [dwriter/load-entities-migrating
                               (fn [& args]
                                 (let [p (apply orig args)]
                                   (when (= 3 (swap! n inc))
                                     @p ; the commit lands …
                                     (throw (ex-info "injected: reply lost" {:error :test/injected})))
                                   p))]
                   (f))))
             {})]
      (is (not= :completed (:first-run r)))
      (is (= (:history ref) (:history r)) "nothing applied twice")
      (is (true? (get-in r [:report :verified?]))))))

(deftest a-sort-false-dump-is-refused
  (let [src (source-db)
        store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
        dump {:store store :prefix "unsorted"}
        _ (m/export-db @src dump {:history? true :sort? false :chunk-size 40})
        conn (create! (mem-cfg))]
    (is (= :import/resume-unordered-source
           (refusal #(m/import-db conn dump (merge base-opts {:eids :preserve :resume? true})))))
    (is (empty? (d/q '[:find ?e :where [?e :name _]] @conn)) "before anything is written")
    (teardown conn)
    (teardown src)))

(deftest a-partial-map-is-refused-when-resumable
  (testing "What the importer allocates for an id a map does not name exists
            only in memory: a resume would allocate it again, elsewhere."
    (let [records (big-tx-records)
          partial (into {} (for [[e] records :when (and (< e const/tx0) (even? e))] [e (+ 5000 e)]))
          conn (create! (mem-cfg))]
      (is (= :import/resume-partial-mapping
             (:cause-error (ex-data (try (import-source-into conn {:resume? true :eids partial}) nil
                                         (catch Exception e e))))))
      (teardown conn))))

(deftest a-partial-map-cannot-merge-two-entities
  (testing "Uninterrupted, too: an id allocated for an unnamed entity must not
            then be handed to a named one by the map."
    (let [t (inc const/tx0)
          records [[t :db/txInstant #inst "2021-01-01" t true]
                   [100 :db/ident :name t true]
                   [100 :db/valueType :db.type/string t true]
                   [100 :db/cardinality :db.cardinality/one t true]
                   [7 :name "unnamed" t true]          ; allocated: the next free id
                   [8 :name "named" t true]]            ; mapped onto that same id (101:
                                                        ; 100 raised :max-eid to 100)
          conn (create! (mem-cfg))
          err (try (m/import-source conn (m/records->chunk-src records)
                                    {:sync? true :verify? false :eids {100 100 8 101}})
                   nil
                   (catch Exception e (ex-data e)))]
      (is (= :import/eid-collision (:cause-error err)))
      (teardown conn))))

(defn- gappy-records
  "Transaction ids with GAPS (as a Datomic source has: `tx0 + t`), and a ref to
   an earlier transaction."
  []
  (let [t (fn [k] (+ const/tx0 1 (* 1000 k)))
        tx (fn [k extra]
             (into [[(t k) :db/txInstant (java.util.Date. (+ 1609459200000 (* 1000 k))) (t k) true]]
                   (concat (for [i (range 8)] [(+ 2000 (* 10 k) i) :name (str "n" k "-" i) (t k) true])
                           extra)))]
    (vec (concat [[(t 0) :db/txInstant #inst "2021-01-01" (t 0) true]
                  [100 :db/ident :name (t 0) true]
                  [100 :db/valueType :db.type/string (t 0) true]
                  [100 :db/cardinality :db.cardinality/one (t 0) true]
                  [101 :db/ident :audit/tx (t 0) true]
                  [101 :db/valueType :db.type/ref (t 0) true]
                  [101 :db/cardinality :db.cardinality/one (t 0) true]]
                 (mapcat #(tx % []) (range 1 6))
                 ;; late: a ref to transaction 2
                 (tx 6 [[2900 :audit/tx (t 2) (t 6) true] [2900 :name "audit" (t 6) true]])))))

(defn- gappy-opts []
  {:sync? true :batch-size 10 :eids :preserve :resume? true
   :source-meta {:history? true :expected-count (count (gappy-records)) :source-id :gappy}})

(deftest non-consecutive-tx-ids-and-a-ref-to-a-transaction-survive-a-resume
  (let [ref-conn (create! (mem-cfg))
        _ (m/import-source ref-conn (m/records->chunk-src (gappy-records) 10) (gappy-opts))
        ref (fingerprint @ref-conn)
        cfg (file-cfg)
        conn (create! cfg)
        _ (is (thrown? Exception ((fail-writer-call 3)
                                  #(m/import-source conn (m/records->chunk-src (gappy-records) 10) (gappy-opts)))))
        _ (crash! conn)
        conn (open cfg)
        rep (m/import-source conn (m/records->chunk-src (gappy-records) 10) (gappy-opts))]
    (is (true? (:resumed? rep)))
    (is (= ref (fingerprint @conn)) "the same database as the uninterrupted resumable import")
    (is (= #{[(+ const/tx0 1 2000)]}
           (d/q '[:find ?tx :where [?e :audit/tx ?tx] [?e :name "audit"]] @conn))
        "the ref names transaction 2 by the source's own id")
    (is (true? (:verified? rep)))
    (teardown ref-conn)
    (teardown conn)))

(deftest a-write-between-crash-and-resume-is-refused
  (let [dump (dump!)
        cfg (file-cfg)
        conn (create! cfg)
        opts (merge base-opts {:eids :preserve :resume? true})]
    (is (thrown? Exception ((fail-writer-call 3) #(m/import-db conn dump opts))))
    (crash! conn)
    (let [conn (open cfg)]
      (d/transact conn [{:name "foreign"}])
      (is (= :import/resume-target-changed (refusal #(m/import-db conn dump opts))))
      (teardown conn))))

(deftest a-finished-import-written-to-and-reconnected
  (let [dump (dump!)
        cfg (file-cfg)
        conn (create! cfg)
        opts (merge base-opts {:eids :preserve :resume? true})
        first-rep (m/import-db conn dump opts)
        _ (crash! conn)
        conn (open cfg)
        again (m/import-db conn dump opts)]
    (is (true? (:verified? first-rep)))
    (is (true? (:verified? again)) "after a reconnect, a second call re-verifies")
    (d/transact conn [{:name "after the import"}])
    (let [later (m/import-db conn dump opts)]
      (is (= :target-changed-since-import (get-in later [:verification :reason]))
          "once written to, it says so instead of failing the count"))
    (is (< (count (pr-str (get-in @conn [:meta :datahike.import/cursor]))) 1500)
        "the cursor every later commit carries stays small")
    (teardown conn)))

(deftest two-resumers-cannot-both-apply-a-batch
  (let [dump (dump!)
        ref (reference dump {:eids :preserve})
        cfg (file-cfg)
        conn (create! cfg)
        opts (merge base-opts {:eids :preserve :resume? true})]
    (is (thrown? Exception ((fail-writer-call 2) #(m/import-db conn dump opts))))
    (crash! conn)
    (let [conn (open cfg)
          a (future (try (m/import-db conn dump opts) (catch Exception e (ex-data e))))
          b (future (try (m/import-db conn dump opts) (catch Exception e (ex-data e))))
          results [(deref a 60000 ::hung) (deref b 60000 ::hung)]]
      (is (not-any? #{::hung} results))
      (is (some :verified? results) "one of them finished")
      (is (every? #(or (:verified? %) (= :resume/conflict (:cause-error %))
                       (= :import/resume-target-changed (:error %)))
                  results)
          (str "the other stopped on the compare-and-set: " (pr-str results)))
      (is (= (:history ref) (:history (fingerprint @conn))) "and nothing was applied twice")
      (teardown conn))))

(deftest offset-over-huge-ids-is-refused-on-the-streaming-path
  (let [t (inc const/tx0)
        big 13194139534312
        records [[t :db/txInstant #inst "2021-01-01" t true]
                 [100 :db/ident :name t true]
                 [100 :db/valueType :db.type/string t true]
                 [100 :db/cardinality :db.cardinality/one t true]
                 [big :name "huge" t true]]
        conn (create! (mem-cfg))
        err (try (m/import-source conn (m/records->chunk-src records)
                                  {:sync? true :verify? false :eids :offset})
                 nil (catch Exception e (ex-data e)))]
    (is (= :import/eid-above-tx0 (:cause-error err)))
    (teardown conn)))
