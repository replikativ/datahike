(ns datahike.test.migrate-import-source-node-test
  "`import-source` AND `export-to-sink` on Node, in the mode Node actually uses.

   The JVM covers this with an explicit `{:sync? false}`, which drives the same
   contract but not the same runtime — `datahike.migrate.init` carries a
   `pcps->chan` adapter precisely because two async worlds meet at this seam
   (datahike's write path is core.async, persistent-sorted-set's cljs builder is
   partial-cps). So a JVM test proves the shape and not the plumbing.

   These pass NO `:sync?` at all. `default-sync?` is FALSE on ClojureScript, so
   this is the default path here, and it is the one that was broken: the first
   `records->chunk-src` returned its chunk directly, and because `:read` is
   `<?-`'d that raised

     No implementation of method: :take! of protocol: ReadPort
     found for class: cljs.core/PersistentVector

   on every Node caller, while the JVM stayed green. The value of these tests is
   not the assertions — it is that they run in the configuration nobody has to
   remember to ask for.

   `export-to-sink` gets the same treatment for a stronger reason: it has THREE
   awaited callbacks to `import-source`'s one, so it has strictly more surface
   for the same failure, and nothing had exercised it outside the JVM."
  (:require [cljs.test :refer [deftest is async]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [cljs.core.async :refer [go <!] :include-macros true]
            [clojure.set]))

(def ^:private t1 536870913)
(def ^:private t2 536870914)
(def ^:private t3 536870915)

(defn- records
  "Transaction-grouped, `:db/txInstant` first, schema before the data using it.
   Carries a cardinality-one overwrite so the history assertion is not vacuous."
  []
  [[t1 :db/txInstant #inst "2021-01-01" t1 true]
   [100 :db/ident :person/name t1 true]
   [100 :db/valueType :db.type/string t1 true]
   [100 :db/cardinality :db.cardinality/one t1 true]
   [t2 :db/txInstant #inst "2021-02-01" t2 true]
   [200 :person/name "Ann" t2 true]
   [201 :person/name "Bob" t2 true]
   [t3 :db/txInstant #inst "2021-03-01" t3 true]
   [200 :person/name "Anna" t3 true]
   [200 :person/name "Ann" t3 false]])

(deftest import-source-on-node-default-mode
  (async done
         (go
           (let [cfg {:store {:backend :memory :id (random-uuid)}
                      :keep-history? true
                      :schema-flexibility :write}]
             (try
               (<! (d/create-database cfg))
               (let [conn   (d/connect cfg)
                     recs   (records)
                     ;; No `:sync?`. On Node that means async, and the report
                     ;; arrives on a channel.
                     report (<! (m/import-source conn (m/records->chunk-src recs 3)
                                                 {:source-meta {:history? true
                                                                :expected-count (count recs)
                                                                :max-tx t3}}))]
                 (if (instance? js/Error report)
                   (is false (str "import-source failed on Node: " (.-message report)))
                   (do
                     (is (true? (:verified? report))
                         "verification ran against :expected-count")
                     (is (= 3 (:tx-count report)))
                     (is (= #{"Anna" "Bob"}
                            (into #{} (map first)
                                  (d/q '[:find ?n :where [?e :person/name ?n]] @conn)))
                         "the cardinality-one overwrite applied")
                     (is (= #{["Ann" true] ["Ann" false] ["Anna" true] ["Bob" true]}
                            (into #{} (map (juxt first second))
                                  (d/q '[:find ?n ?op :where [?e :person/name ?n _ ?op]]
                                       (d/history @conn))))
                         "history: assert, retract and re-assert all present")
                     (is (= 3 (count (d/q '[:find ?i :where [_ :db/txInstant ?i]] @conn)))
                         "the SOURCE's three transaction times, not the import's"))))
               (catch js/Error e
                 (is false (str "import-source on Node threw: " (.-message e)))))
             (<! (d/delete-database cfg))
             (done)))))

(deftest a-hand-written-source-may-do-async-io
  (async done
         (go
           (let [cfg {:store {:backend :memory :id (random-uuid)}
                      :keep-history? false
                      :schema-flexibility :read}]
             (try
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)
                     recs (vec (for [i (range 30)]
                                 [(+ 100 i) :name (str "n" i) (+ 536870913 i) true]))
                     parts (vec (partition-all 10 recs))
                     ;; The point of the async contract: `:read` may PARK, which
                     ;; is what lets a real source fetch over the network or off
                     ;; disk. Simulated with a channel that delivers later.
                     src {:chunks (vec (range (count parts)))
                          :read (fn [i _opts]
                                  (go (<! (cljs.core.async/timeout 1))
                                      (nth parts i)))}
                     report (<! (m/import-source conn src {:verify? false}))]
                 (if (instance? js/Error report)
                   (is false (str "async :read failed: " (.-message report)))
                   (do
                     (is (= 30 (:datom-count report))
                         "every record arrived, though each chunk resolved asynchronously")
                     (is (= 30 (count (d/q '[:find ?e :where [?e :name _]] @conn)))))))
               (catch js/Error e
                 (is false (str "async :read threw: " (.-message e)))))
             (<! (d/delete-database cfg))
             (done)))))

(deftest export-to-sink-on-node-default-mode
  (async done
         (go
           (let [cfg {:store {:backend :memory :id (random-uuid)}
                      :keep-history? true
                      :schema-flexibility :write}]
             (try
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)]
                 (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                         :db/cardinality :db.cardinality/one}]))
                 (<! (d/transact! conn [{:name "Ann"} {:name "Bob"}]))
                 (<! (d/transact! conn [{:name "Cid"}]))
                 (let [seen   (atom [])
                       ;; No `:sync?`. On Node that is async, so ALL THREE
                       ;; callbacks must return channels — the contract `:read`
                       ;; violated on the import side while the JVM stayed green.
                       sink   {:open  (fn [_opts] (go {:n 0}))
                               :write (fn [ctx recs] (go (swap! seen conj recs)
                                                         (update ctx :n + (count recs))))
                               :close (fn [ctx] (go {:total (:n ctx)}))}
                       result (<! (m/export-to-sink @conn sink
                                                    {:history? true :chunk-size 2}))]
                   (if (instance? js/Error result)
                     (is false (str "export-to-sink failed on Node: " (.-message result)))
                     (let [all (vec (apply concat @seen))
                           ts  (mapv (fn [c] (into #{} (map #(nth % 3)) c)) @seen)]
                       (is (= {:total (count all)} result)
                           "the value :close resolved with is what comes back")
                       (is (pos? (count all)) "precondition: something was exported")
                       (is (= (map #(nth % 3) all) (sort (map #(nth % 3) all)))
                           "`t` ascending, same order as the JVM")
                       (is (every? (fn [g] (= :db/txInstant (nth (first g) 1)))
                                   (partition-by #(nth % 3) all))
                           ":db/txInstant leads every transaction")
                       (is (= (reduce + (map count ts))
                              (count (apply clojure.set/union ts)))
                           "and no transaction is split across chunks")))))
               (catch js/Error e
                 (is false (str "export-to-sink on Node threw: " (.-message e)))))
             (<! (d/delete-database cfg))
             (done)))))

(deftest the-two-seams-round-trip-on-node
  (async done
         (go
           (let [src-cfg {:store {:backend :memory :id (random-uuid)}
                          :keep-history? true :schema-flexibility :write}
                 tgt-cfg {:store {:backend :memory :id (random-uuid)}
                          :keep-history? true :schema-flexibility :write}]
             (try
               (<! (d/create-database src-cfg))
               (<! (d/create-database tgt-cfg))
               (let [src (d/connect src-cfg)]
                 (<! (d/transact! src [{:db/ident :name :db/valueType :db.type/string
                                        :db/cardinality :db.cardinality/one}]))
                 (<! (d/transact! src [{:name "Ann"} {:name "Bob"}]))
                 (let [captured (atom [])
                       out (<! (m/export-to-sink
                                @src
                                {:open  (fn [_] (go nil))
                                 :write (fn [_ recs] (go (swap! captured into recs) nil))
                                 :close (fn [_] (go :done))}
                                {:history? true :chunk-size 3}))]
                   (if (instance? js/Error out)
                     (is false (str "export leg failed: " (.-message out)))
                     (let [tgt (d/connect tgt-cfg)
                           rep (<! (m/import-source
                                    tgt (m/records->chunk-src @captured 3)
                                    {:source-meta {:history? true
                                                   :expected-count (count @captured)}}))]
                       (if (instance? js/Error rep)
                         (is false (str "import leg failed: " (.-message rep)))
                         (do
                           (is (true? (:verified? rep)))
                           (is (= #{"Ann" "Bob"}
                                  (into #{} (map first)
                                        (d/q '[:find ?n :where [?e :name ?n]] @tgt)))
                               "out through the sink and back in reproduces the database")))))))
               (catch js/Error e
                 (is false (str "round trip on Node threw: " (.-message e)))))
             (<! (d/delete-database src-cfg))
             (<! (d/delete-database tgt-cfg))
             (done)))))
