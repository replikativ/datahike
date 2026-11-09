(ns datahike.test.temporal-superseded-only-test
  "`:index-config {:temporal-superseded-only? true}` — the temporal trees hold
   only what the live trees no longer hold.

   The DEFAULT layout copies every cardinality-one assertion into the temporal
   trees as well (`datahike.index.persistent-set/temporal-upsert`), so on an
   append-only workload the history half is a byte-for-byte second copy of the
   live half: three extra index updates and three extra root writes per commit,
   for no information. Cardinality-many and `:db/noHistory` attributes already
   work the other way — their live datom exists only in the live tree and
   `datahike.db.utils/distinct-datoms` merges it back into the history view.

   This flag extends that rule to cardinality-one. The history view is then the
   union of BOTH trees, unfiltered.

   Every test here asserts the same thing from a different angle: the two
   layouts are OBSERVATIONALLY EQUAL. A scripted sequence of asserts,
   supersedes, retracts, re-asserts and cardinality-many changes is replayed
   into two databases that differ only in the flag, and `history`, `as-of` at
   every transaction, `since` at every transaction, and the present-tense
   indexes are compared datom for datom."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(def schema
  [{:db/ident :name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :age :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :tag :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/many}
   {:db/ident :scratch :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/noHistory true}])

(defn- script
  "The transactions both layouts replay. Deliberately covers every shape the
   write path distinguishes: a first assertion, a supersession over a different
   value, a restatement of the same value (which datahike drops entirely), an
   explicit retraction, a RE-assertion of a previously retracted value,
   cardinality-many add and retract, a `:db/noHistory` attribute, an
   assert+retract inside ONE transaction, and a full `retractEntity`."
  [eid]
  [[{:db/id -1 :name "alice" :age 30}]                       ;; already transacted by run-script!
   [{:db/id -2 :name "bob" :age 40 :tag #{:x :y}}]
   [[:db/add eid :age 31]]                                   ;; card-one supersede
   [[:db/add eid :age 31]]                                   ;; restatement: a no-op
   [[:db/add eid :age 32]]                                   ;; supersede again
   [[:db/retract eid :age 32]]                               ;; explicit retraction
   [[:db/add eid :age 33]]                                   ;; re-assert after retraction
   [[:db/add eid :age 31]]                                   ;; re-assert a PREVIOUSLY held value
   [[:db/add eid :tag :p] [:db/add eid :tag :q]]             ;; card-many
   [[:db/retract eid :tag :p]]
   [[:db/add eid :tag :p]]                                   ;; card-many re-assert
   [[:db/add eid :scratch "one"]]                            ;; :db/noHistory
   [[:db/add eid :scratch "two"]]
   [[:db/add eid :name "alice2"]]                            ;; unique-identity upsert
   [{:db/id -3 :name "ephemeral"}]
   [[:db/add eid :age 34] [:db/retract eid :age 34]]         ;; assert+retract in ONE tx
   [[:db/retractEntity [:name "bob"]]]])

(defn- run-script!
  "Create a database with `extra-config`, replay the script, return
   `{:conn conn :txs [tx-id ...]}`. The connection is the caller's to release."
  [extra-config]
  (let [conn (utils/setup-db (merge {:store {:backend :memory
                                             :id (random-uuid)}
                                     :keep-history? true
                                     :schema-flexibility :write}
                                    extra-config))
        _ (d/transact conn schema)
        _ (d/transact conn [{:db/id -1 :name "alice" :age 30}])
        eid (:e (first (d/datoms @conn :avet :name "alice")))
        txs (reduce (fn [acc tx-data]
                      (conj acc (:max-tx (:db-after (d/transact conn tx-data)))))
                    []
                    (rest (script eid)))]
    {:conn conn :eid eid :txs txs}))

(defn- sorted-datoms
  "Comparable projection of a datom seq. `:db/txInstant` is dropped: the two
   databases are built by two separate runs of the same script, so their
   transaction wall-clocks legitimately differ and nothing else about them does."
  [ds]
  (->> ds
       (remove #(= :db/txInstant (:a %)))
       (map (juxt :e :a :v :tx :added))
       (sort-by (juxt first second #(str (nth % 2)) #(nth % 3) #(nth % 4)))))

(defn- history-datoms [db]
  (sorted-datoms (d/datoms (d/history db) :eavt)))

(defn- current-datoms [db]
  (sorted-datoms (d/datoms db :eavt)))

(defn- as-of-datoms [db tx]
  (sorted-datoms (d/datoms (d/as-of db tx) :eavt)))

(defn- since-datoms [db tx]
  (sorted-datoms (d/datoms (d/since db tx) :eavt)))

(deftest the-two-layouts-are-observationally-equal
  (testing "history, as-of at every tx, since at every tx, and the present-tense
            indexes agree between the default and the superseded-only layout"
    (let [legacy (run-script! {})
          lean   (run-script! {:index-config {:temporal-superseded-only? true}})]
      (try
        (let [ldb @(:conn legacy)
              ndb @(:conn lean)]
          (is (= (current-datoms ldb) (current-datoms ndb))
              "the live indexes are untouched by the flag")
          (is (= (history-datoms ldb) (history-datoms ndb))
              "d/history is the same multiset of datoms")
          (doseq [tx (:txs legacy)]
            (is (= (as-of-datoms ldb tx) (as-of-datoms ndb tx))
                (str "as-of " tx))
            (is (= (since-datoms ldb tx) (since-datoms ndb tx))
                (str "since " tx))))
        (finally (d/release (:conn legacy)) (d/release (:conn lean)))))))

(deftest history-queries-agree
  (testing "a query over d/history returns the same versions under both layouts"
    (let [legacy (run-script! {})
          lean   (run-script! {:index-config {:temporal-superseded-only? true}})
          ages (fn [conn]
                 (sort (map first
                            (d/q '[:find ?a :in $ :where [?e :name "alice2"] [?e :age ?a]]
                                 (d/history @conn)))))
          live-ages (fn [conn]
                      (sort (map first
                                 (d/q '[:find ?a :in $ :where [?e :name "alice2"] [?e :age ?a]]
                                      @conn))))]
      (try
        (is (= (ages (:conn legacy)) (ages (:conn lean))))
        (is (= (live-ages (:conn legacy)) (live-ages (:conn lean))))
        (is (seq (ages (:conn lean))) "the history query is not vacuously empty")
        (finally (d/release (:conn legacy)) (d/release (:conn lean)))))))

(deftest temporal-trees-hold-only-what-the-live-trees-dropped
  (testing "under the flag no datom is in both a live tree and its temporal twin,
            and the temporal trees are strictly smaller than the legacy ones"
    (let [legacy (run-script! {})
          lean   (run-script! {:index-config {:temporal-superseded-only? true}})]
      (try
        (let [ldb @(:conn legacy)
              ndb @(:conn lean)
              key-of (fn [d] [(:e d) (:a d) (:v d) (:tx d) (:added d)])
              live (into #{} (map key-of) (seq (:eavt ndb)))
              temporal (into #{} (map key-of) (seq (:temporal-eavt ndb)))]
          (is (empty? (set/intersection live temporal))
              "disjoint: a live datom is never also a history entry")
          (is (< (count (:temporal-eavt ndb)) (count (:temporal-eavt ldb)))
              "the superseded-only temporal tree is smaller")
          (is (every? true? (map :added (seq (:eavt ndb))))
              "no retraction markers in the live index")
          ;; The exact relation between the two layouts, stated as an equation.
          ;; The legacy temporal tree is the union the read path now forms,
          ;; MINUS the live datoms that never reached temporal under either
          ;; layout: `:db/noHistory` and cardinality-many (excluded by
          ;; `keep-history?` in `with-datom`/`with-datom-upsert`) and
          ;; `:db/txInstant` (excluded by name in `with-datom-upsert`).
          (let [never-temporal? #{:scratch :tag :db/txInstant}]
            (is (= (into #{} (map key-of) (seq (:temporal-eavt ldb)))
                   (into (set temporal)
                         (remove (fn [[_ a]] (never-temporal? a)))
                         live))
                "legacy temporal == (live ∪ superseded-only temporal) minus the
                 live datoms no layout ever copies")))
        (finally (d/release (:conn legacy)) (d/release (:conn lean)))))))

(deftest append-only-writes-nothing-to-history
  (testing "a database that only ever asserts fresh cardinality-one datoms keeps
            an EMPTY temporal tree — the write-amplification this flag exists for"
    (let [{:keys [conn]} {:conn (utils/setup-db
                                 {:store {:backend :memory :id (random-uuid)}
                                  :keep-history? true :schema-flexibility :write
                                  :index-config {:temporal-superseded-only? true}})}]
      (try
        (d/transact conn schema)
        (doseq [i (range 20)]
          (d/transact conn [{:name (str "m" i) :age i}]))
        (is (zero? (count (:temporal-eavt @conn))))
        (is (= (count (d/datoms @conn :eavt))
               (count (d/datoms (d/history @conn) :eavt)))
            "history still sees every datom, via the live∪temporal merge")
        (finally (d/release conn))))))

(deftest reading-a-legacy-store-with-the-flag-on
  (testing "the dedup in the union makes a FULL-COPY temporal tree read correctly
            under the new code, so no migration is needed to turn the flag on"
    (let [id (random-uuid)
          store {:backend :memory :id id}
          cfg {:store store :keep-history? true :schema-flexibility :write}
          conn (utils/setup-db cfg)]
      (try
        (d/transact conn schema)
        (d/transact conn [{:db/id -1 :name "alice" :age 30}])
        (let [eid (:e (first (d/datoms @conn :avet :name "alice")))]
          (d/transact conn [[:db/add eid :age 31]])
          (d/transact conn [[:db/add eid :age 32]])
          (d/transact conn [[:db/retract eid :age 32]])
          (d/transact conn [[:db/add eid :age 33]]))
        (let [legacy-history (history-datoms @conn)
              legacy-current (current-datoms @conn)]
          (d/release conn)
          ;; Reconnect the SAME store with the flag on. The stored config says
          ;; false, so this is an explicit, unsafe override — which is the only
          ;; way to reach this state and exactly the state we want to pin.
          (let [conn2 (d/connect (assoc cfg
                                        :index-config {:temporal-superseded-only? true}
                                        :allow-unsafe-config true))]
            (try
              (is (= legacy-current (current-datoms @conn2)))
              (is (= legacy-history (history-datoms @conn2))
                  "the full-copy temporal tree is deduplicated against the live one")
              ;; and a write under the new layout into the legacy tree
              (let [eid (:e (first (d/datoms @conn2 :avet :name "alice")))]
                (d/transact conn2 [[:db/add eid :age 34]]))
              (is (= #{30 31 32 33 34}
                     (into #{} (map first)
                           (d/q '[:find ?a :where [?e :name "alice"] [?e :age ?a]]
                                (d/history @conn2))))
                  "every version, old and new-layout, is still in history")
              (finally (d/release conn2)))))
        (catch Throwable t (d/release conn) (throw t))))))

(deftest the-flag-cannot-be-turned-off-for-a-store-created-with-it
  (testing "reading a superseded-only store as a full-copy one would silently drop
            every current value from history; not even :allow-unsafe-config may ask for it"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? true :schema-flexibility :write
               :index-config {:temporal-superseded-only? true}}
          conn (utils/setup-db cfg)]
      (try
        (d/transact conn schema)
        (d/transact conn [{:db/id -1 :name "alice" :age 30}])
        (finally (d/release conn)))
      (testing "a reconnect that does not mention the flag adopts it from the store"
        (let [conn (d/connect (dissoc cfg :index-config))]
          (try
            (is (true? (get-in (:config @conn) [:index-config :temporal-superseded-only?])))
            (is (= #{30} (into #{} (map first)
                               (d/q '[:find ?a :where [?e :name "alice"] [?e :age ?a]]
                                    (d/history @conn)))))
            (finally (d/release conn)))))
      (doseq [unsafe? [false true]]
        (is (thrown-with-msg?
             #?(:clj clojure.lang.ExceptionInfo :cljs ExceptionInfo)
             #"cannot be read without it|differ from the stored configuration"
             (d/connect (cond-> (assoc cfg :index-config {:temporal-superseded-only? false})
                          unsafe? (assoc :allow-unsafe-config true))))
            (str "refused with :allow-unsafe-config " unsafe?))))))

(deftest purge-agrees-between-layouts
  (testing ":db/purge and :db.purge/entity remove the same datoms under both layouts"
    (let [mk (fn [extra]
               (let [conn (utils/setup-db (merge {:store {:backend :memory
                                                          :id (random-uuid)}
                                                  :keep-history? true
                                                  :schema-flexibility :write}
                                                 extra))]
                 (d/transact conn schema)
                 (d/transact conn [{:db/id -1 :name "alice" :age 30}])
                 (let [eid (:e (first (d/datoms @conn :avet :name "alice")))]
                   (d/transact conn [[:db/add eid :age 31]])
                   (d/transact conn [[:db/add eid :age 32]])
                   [conn eid])))
          [lc le] (mk {})
          [nc ne] (mk {:index-config {:temporal-superseded-only? true}})]
      (try
        (d/transact lc [[:db/purge le :age 31]])
        (d/transact nc [[:db/purge ne :age 31]])
        (is (= (history-datoms @lc) (history-datoms @nc)))
        (is (= (current-datoms @lc) (current-datoms @nc)))
        (d/transact lc [[:db.purge/entity le]])
        (d/transact nc [[:db.purge/entity ne]])
        (is (= (history-datoms @lc) (history-datoms @nc)))
        (is (= (current-datoms @lc) (current-datoms @nc)))
        (finally (d/release lc) (d/release nc))))))
