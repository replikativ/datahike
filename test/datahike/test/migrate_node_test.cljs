(ns datahike.test.migrate-node-test
  "Export and import, running on Node.

   This is what the whole `async+sync` conversion was for. Everything else
   proved the SHAPE was right — that no IO hides inside a closure the `go` state
   machine cannot enter, that retries do not park inside a `catch`, that a store
   is not released before it is read. All of that was verified on the JVM, where
   a blocking take makes failures observable.

   What only Node can prove is the platform half: that `format`, `*err*`,
   `Class/forName`, `Runtime` and blocking derefs are genuinely gone from every
   path an export or import touches, and that the konserve store medium works
   under a runtime with no synchronous mode at all.

   Both orderings run here. `:sort? false` needs no scratch space at all;
   `:sort? true` spills sorted runs to local files and k-way merges them, which
   works on Node because every read in the merge is a synchronous local file
   read (`fs.readSync`) and no channel op ever occurs inside it.

   `:build-indexes? true` runs here too, against the same oracle the JVM suite
   uses (`migrate-init-import-test`): the database an index-build import produces
   must equal the one the streaming import produces from the same dump."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.core.async :refer [go <!]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [konserve.store :as ks]))

(defn- take-or-throw [v]
  (if (instance? js/Error v) (throw v) v))

(deftest export-and-import-round-trip-on-node
  (async done
         (go
           (let [cfg {:store {:backend :memory :id (random-uuid)}
                      :keep-history? true :schema-flexibility :write}]
             (try
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)]
                 (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                         :db/cardinality :db.cardinality/one
                                         :db/unique :db.unique/identity}
                                        {:db/ident :n :db/valueType :db.type/long
                                         :db/cardinality :db.cardinality/one}]))
                 (<! (d/transact! conn [{:name "a" :n 1} {:name "b" :n 2}]))
                 (let [store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                  {:sync? false}))
                       target {:store store :prefix "node-dump"}
                       man (take-or-throw
                            (<! (m/export-db @conn target {:history? true :sort? false})))]
                   (is (map? man) "export returned a manifest")
                   (is (pos? (count (:chunks man))) "and wrote chunks")
                   (is (= :gzip (:compression man)) "compressed by default, on Node too")
                   (is (every? #(re-find #"\.cbor\.gz$" (:file %)) (:chunks man)))

                   (testing "and it imports back into a fresh database"
                     (let [c2 {:store {:backend :memory :id (random-uuid)}
                               :keep-history? true :schema-flexibility :write}]
                       (<! (d/create-database c2))
                       (let [tgt (d/connect c2)
                             rep (take-or-throw (<! (m/import-db tgt target {})))]
                         (is (= (:count (:semantic-digest man)) (:datom-count rep))
                             "every datom landed")
                         (is (true? (:verified? rep)) "and post-import verification passed")
                         (is (= 2 (:tx-count rep)) "schema + data")
                         (is (= #{"a" "b"}
                                (set (map first (d/q '[:find ?n :where [?e :name ?n]] @tgt))))
                             "and the data is queryable"))
                       (<! (d/delete-database c2))))))
               (catch js/Error e
                 (is false (str "node round trip failed: " (.-message e)
                                "\nSTACK:\n" (.-stack e)))))
             (<! (d/delete-database cfg))
             (done)))))

(deftest sync-is-refused-by-name-on-node
  (testing "there is no blocking take here, so `:sync? true` cannot be honoured.
            Refusing by name beats failing deep inside the batcher with something
            about IDeref — the same choice `api/transact` makes."
    (async done
           (go
             (let [cfg {:store {:backend :memory :id (random-uuid)}
                        :keep-history? true :schema-flexibility :write}]
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)
                     store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                {:sync? false}))
                     target {:store store :prefix "refuse"}]
                 (is (thrown-with-msg? js/Error #"not supported in ClojureScript"
                                       (m/export-db @conn target {:sync? true})))
                 (is (thrown-with-msg? js/Error #"not supported in ClojureScript"
                                       (m/import-db conn target {:sync? true}))))
               (<! (d/delete-database cfg))
               (done))))))

(deftest the-external-sort-runs-on-node
  (testing "`:sort? true` used to be refused by name here, on the theory that a
            k-way merge over open files could not work off the JVM. The file
            handles were the portable part; `java.io.File` and
            `java.util.PriorityQueue` were not. With those gone the sort runs on
            Node, spilling runs through `fs.readSync` — so the two runtimes
            offer the same two orderings rather than Node offering one.

            A round trip rather than 'it did not throw': the sort rewrites the
            record stream, so the way it fails is a dump that is subtly
            misordered, which only reading it back detects.

            `:sort-buffer` is deliberately tiny so the export spills MANY runs
            and actually exercises the merge. At the default of 1,000,000 this
            database fits in one run and the k-way merge never runs at all."
    (async done
           (go
             (let [cfg {:store {:backend :memory :id (random-uuid)}
                        :keep-history? true :schema-flexibility :write}]
               (try
                 (<! (d/create-database cfg))
                 (let [conn (d/connect cfg)]
                   (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                           :db/cardinality :db.cardinality/one
                                           :db/unique :db.unique/identity}
                                          {:db/ident :n :db/valueType :db.type/long
                                           :db/cardinality :db.cardinality/one}]))
                   ;; several transactions, so transaction order is a real
                   ;; constraint the sort has to establish rather than inherit
                   (doseq [i (range 8)]
                     (<! (d/transact! conn [{:name (str "e" i) :n i}])))
                   ;; a card-one overwrite: the retract and the assert must come
                   ;; out retract-first, which is the ordering the sort exists for
                   (<! (d/transact! conn [{:name "e0" :n 99}]))
                   (let [store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                    {:sync? false}))
                         target {:store store :prefix "sorted"}
                         man (take-or-throw
                              (<! (m/export-db @conn target {:history? true
                                                             :sort? true
                                                             :sort-buffer 4})))]
                     (is (map? man) "the sorted export produced a manifest")
                     (is (pos? (count (:chunks man))) "and wrote chunks")

                     (testing "and the sorted dump imports back correctly"
                       (let [c2 {:store {:backend :memory :id (random-uuid)}
                                 :keep-history? true :schema-flexibility :write}]
                         (<! (d/create-database c2))
                         (let [tgt (d/connect c2)
                               rep (take-or-throw (<! (m/import-db tgt target {})))]
                           (is (= (:count (:semantic-digest man)) (:datom-count rep))
                               "every datom landed")
                           (is (true? (:verified? rep))
                               "and post-import verification passed")
                           (is (= 99 (ffirst (d/q '[:find ?n :where
                                                    [?e :name "e0"] [?e :n ?n]]
                                                  @tgt)))
                               "the overwritten value is the surviving one, so the
                                retract and assert were not transposed"))
                         (<! (d/delete-database c2))))))
                 (catch js/Error e
                   (is false (str "node sorted round trip failed: " (.-message e)
                                  "\nSTACK:\n" (.-stack e)))))
               (<! (d/delete-database cfg))
               (done))))))

;; ---------------------------------------------------------------------------
;; the index-build import

(def ^:private index-fields
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(def ^:private derived-fields
  ;; :op-count is excluded for the reason `migrate-init-import-test` records: it
  ;; is inert for persistent-set, and hitchhiker-tree — the only index that reads
  ;; it — is refused. :max-tx is excluded because it differs BY DESIGN and is
  ;; asserted separately.
  [:hash :schema :rschema :max-eid])

(defn- adversarial!
  "The shapes that make `:hash` and the temporal split non-obvious: a card-one
   overwrite, a card-many retraction, a retract-then-reassert, a ref, and a
   `:db/noHistory` attribute overwritten. A uniform random database exercises
   none of them, which is why the JVM suite uses the same generator."
  [conn]
  (go
    (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one
                            :db/unique :db.unique/identity}
                           {:db/ident :score :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :tag :db/valueType :db.type/keyword
                            :db/cardinality :db.cardinality/many}
                           {:db/ident :pal :db/valueType :db.type/ref
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :note :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one
                            :db/noHistory true}]))
    (<! (d/transact! conn [{:db/id -1 :name "a" :score 1 :tag :x}
                           {:db/id -2 :name "b" :score 2 :tag :y}
                           {:db/id -3 :name "c" :score 3}]))
    (<! (d/transact! conn [{:db/id [:name "a"] :score 100}]))
    (<! (d/transact! conn [{:db/id [:name "b"] :tag :z}]))
    (<! (d/transact! conn [[:db/retract [:name "b"] :tag :y]]))
    (<! (d/transact! conn [[:db/retract [:name "c"] :score 3]]))
    (<! (d/transact! conn [{:db/id [:name "c"] :score 3}]))
    (<! (d/transact! conn [{:db/id [:name "a"] :pal [:name "c"]}]))
    (<! (d/transact! conn [{:db/id [:name "a"] :note "no history kept for this"}]))
    (<! (d/transact! conn [{:db/id [:name "a"] :note "second value"}]))
    conn))

(defn- pss-cfg []
  {:store {:backend :memory :id (random-uuid)}
   :index :datahike.index/persistent-set
   :keep-history? true :schema-flexibility :write})

(deftest index-build-import-equals-the-streaming-import-on-node
  (testing "`:build-indexes? true` on Node, against the ONE oracle that matters:
            the database it produces must equal the one the streaming import
            produces from the same dump, field for field.

            This path used to be refused here twice over — once for being
            ClojureScript, once for `:sync? false` — and neither was a property
            the builder could not reproduce. What it actually needed was the dump
            read moved out of a lazy seq (a `go` state machine cannot park in
            one), a partial-cps→channel adapter for `init-index-sorted`'s
            ClojureScript result, and a GC guard released inside the go block
            rather than in a `finally` that fires the instant a channel is handed
            back.

            `:eids :allocate` explicitly: allocation is what the transact path
            does, so it is the mode in which the two must agree id-for-id. The
            default here is `:preserve`, which deliberately differs.

            Chunk size 5 so the dump spans several chunks — the index-build path
            normalises to a spool before sorting, and a chunk boundary falling
            between an entity's datoms would show up here and nowhere else."
    (async done
           (go
             (let [cfg (pss-cfg)]
               (try
                 (<! (d/create-database cfg))
                 (let [conn (d/connect cfg)]
                   (<! (adversarial! conn))
                   (let [store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                    {:sync? false}))
                         target {:store store :prefix "index-build"}
                         man (take-or-throw
                              (<! (m/export-db @conn target {:history? true :chunk-size 5})))
                         c-tx (pss-cfg)
                         c-bk (pss-cfg)]
                     (is (< 1 (count (:chunks man)))
                         "precondition: the dump spans more than one chunk")
                     (<! (d/create-database c-tx))
                     (<! (d/create-database c-bk))
                     (let [tx-tgt (d/connect c-tx)
                           bk-tgt (d/connect c-bk)
                           tx-rep (take-or-throw (<! (m/import-db tx-tgt target {})))
                           bk-rep (take-or-throw
                                   (<! (m/import-db bk-tgt target {:build-indexes? true
                                                                   :eids :allocate})))
                           a @tx-tgt
                           b @bk-tgt]
                       (is (true? (:build-indexes? bk-rep)) "the report says which path ran")
                       (is (true? (:verified? bk-rep)) "and verification ran and passed")
                       (is (= (:datom-count tx-rep) (:datom-count bk-rep)))
                       (is (= (:tx-count tx-rep) (:tx-count bk-rep))
                           "the distinct-transaction count agrees, which on Node comes
                            from a js/Set rather than the JVM's BitSet")
                       (doseq [k index-fields]
                         (is (= (vec (get a k)) (vec (get b k)))
                             (str k ": " (count (get a k)) " vs " (count (get b k)))))
                       (doseq [k derived-fields]
                         (is (= (get a k) (get b k)) (str k " differs")))
                       (testing ":max-tx is the ONE field that differs, and by exactly
                                 one. The streaming import ends via
                                 `transact-entities-directly`, which bumps max-tx once
                                 more; the index build does not transact at all."
                         (is (= 1 (- (:max-tx a) (:max-tx b)))))
                       (is (= #{"a" "b" "c"}
                              (set (map first (d/q '[:find ?n :where [?e :name ?n]] b))))
                           "and the bulk-built database is queryable"))
                     (<! (d/delete-database c-tx))
                     (<! (d/delete-database c-bk))))
                 (catch js/Error e
                   (is false (str "node index-build import failed: " (.-message e)
                                  " " (pr-str (ex-data e))
                                  "\nSTACK:\n" (.-stack e)))))
               (<! (d/delete-database cfg))
               (done))))))

(deftest a-bulk-built-database-survives-a-reconnect-on-node
  (testing "the trees have to be DURABLE, not merely in the connection's db value.

            The test above reads `@conn` in process, so a build that never
            flushed its nodes — or flushed them under the wrong keys — would pass
            it. This releases the connection and reconnects from the store, which
            reads the six trees back through `stored->db`.

            It is the assertion that the ClojureScript `:flush-fn` really wrote:
            on Node the drain is an async konserve write, and its result has to
            travel back into persistent-sorted-set's partial-cps `await`
            (`writing/as-awaitable`) or the builder proceeds over writes that
            never landed."
    (async done
           (go
             (let [cfg (pss-cfg)]
               (try
                 (<! (d/create-database cfg))
                 (let [conn (d/connect cfg)]
                   (<! (adversarial! conn))
                   (let [store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                    {:sync? false}))
                         target {:store store :prefix "reconnect"}
                         _ (take-or-throw (<! (m/export-db @conn target {:history? true})))
                         c2 (pss-cfg)]
                     (<! (d/create-database c2))
                     (let [tgt (d/connect c2)
                           _ (take-or-throw (<! (m/import-db tgt target {:build-indexes? true})))
                           before @tgt]
                       (d/release tgt)
                       (let [again @(d/connect c2)]
                         (doseq [k index-fields]
                           (is (= (vec (get before k)) (vec (get again k)))
                               (str k " did not survive the reconnect")))
                         (is (= (:schema before) (:schema again))
                             "the schema came back from the store, not from a lucky
                              in-memory value")
                         (is (= (:max-tx before) (:max-tx again)))
                         (is (= (:hash before) (:hash again)))))
                     (<! (d/delete-database c2))))
                 (catch js/Error e
                   (is false (str "node index-build reconnect failed: " (.-message e)
                                  "\nSTACK:\n" (.-stack e)))))
               (<! (d/delete-database cfg))
               (done))))))
