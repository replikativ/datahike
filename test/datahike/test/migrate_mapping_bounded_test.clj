(ns datahike.test.migrate-mapping-bounded-test
  "A computed id mapping must keep the streaming import's id state O(1).

   `:offset`, `:preserve`, a caller's ILookup and a caller's function exist so an
   import needs no table with one entry per source entity — the term
   `estimate-import-memory` calls dominant. #970 made `transact-entities-directly`
   memoise every id it wrote into `:allocated` whenever `:eids` was not a map, so
   all four quietly rebuilt that table: one entry per entity, plus one per
   transaction from the tx-entity alias. Nothing failed; the heap just grew.

   The state is observed where it lives: the `:migration` map the importer hands
   to each `load-entities-migrating` call."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.constants :as const]
            [datahike.migrate :as m]
            [datahike.writer :as dwriter]))

(def ^:private n-entities 600)
(def ^:private per-tx 20)
(def ^:private src-lo
  "Source entity ids start here: small enough that `:offset` keeps them below
   `tx0`, distinct from where the packer puts them (1000+)."
  5000)

(defn- records
  "A ref from each entity to the previous one,
   one schema transaction then `n-entities / per-tx` data transactions."
  []
  (let [t0 (inc const/tx0)]
    (into [[t0 :db/txInstant #inst "2021-01-01" t0 true]
           [100 :db/ident :name t0 true]
           [100 :db/valueType :db.type/string t0 true]
           [100 :db/cardinality :db.cardinality/one t0 true]
           [101 :db/ident :prev t0 true]
           [101 :db/valueType :db.type/ref t0 true]
           [101 :db/cardinality :db.cardinality/one t0 true]]
          (mapcat (fn [k]
                    (let [t (+ t0 1 k)]
                      (into [[t :db/txInstant (java.util.Date. (+ 1609459200000 (* 1000 (inc k)))) t true]]
                            (mapcat (fn [i]
                                      (let [e (+ src-lo (* k per-tx) i)]
                                        (cond-> [[e :name (str "n" (- e src-lo)) t true]]
                                          (> e src-lo) (conj [e :prev (dec e) t true])))))
                            (range per-tx)))))
          (range (quot n-entities per-tx)))))

(defn- packer
  "Total over this fixture: schema ids 100/101 pass through, source entities pack
   down to 1000+."
  []
  (reify clojure.lang.ILookup
    (valAt [this k] (.valAt this k nil))
    (valAt [_ k nf]
      (cond (not (number? k)) nf
            (>= (long k) src-lo) (+ 1000 (- (long k) src-lo))
            :else k))))

(defn- fresh []
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? true :schema-flexibility :write
             :index :datahike.index/persistent-set}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- state-size
  "Entries the import is REMEMBERING about ids: `:allocated`, plus `:eids` when it
   is a table rather than a computed mapping. `:tids` is per transaction and is
   not what this test is about."
  [migration]
  (+ (count (:allocated migration))
     (let [e (:eids migration)] (if (map? e) (count e) 0))))

(defn- import-observing
  "Import `(records)` under `eids`; return the largest id state any batch was
   handed, and the values that landed."
  [eids]
  (let [conn (fresh)
        seen (atom 0)
        orig dwriter/load-entities-migrating]
    (try
      (with-redefs [dwriter/load-entities-migrating
                    (fn [c batch migration]
                      (swap! seen max (state-size migration))
                      (orig c batch migration))]
        (m/import-source conn (m/records->chunk-src (records) 40)
                         {:sync? true :verify? false :batch-size 50 :eids eids}))
      {:max-state @seen
       :names (count (d/q '[:find ?n :where [_ :name ?n]] @conn))
       :chain (d/q '[:find ?a ?b :where [?x :prev ?y] [?x :name ?a] [?y :name ?b]] @conn)}
      (finally (teardown conn)))))

(deftest a-computed-mapping-remembers-nothing
  (testing "Under :offset, :preserve, an ILookup and a function, every id is
            answered by the mapping, so the importer has nothing to memoise. On
            #970 each of these grew one entry per entity and per transaction —
            measured here as the largest state handed to any batch."
    (let [reference (import-observing :allocate)]
      (is (= n-entities (:names reference)) "precondition: the fixture imports")
      ;; sampled at each batch's INPUT, so the last batch's entities are not in it
      (is (<= (quot n-entities 2) (:max-state reference))
          "precondition: :allocate DOES hold a table, so the probe can see one")
      (doseq [[label eids] [[":offset" :offset]
                            [":preserve-shaped ILookup" (packer)]
                            ["fn" (fn [e] (get (packer) e))]]]
        (let [r (import-observing eids)]
          (is (zero? (:max-state r))
              (str label ": no per-entity state, got " (:max-state r) " entries"))
          (is (= (select-keys reference [:names :chain]) (select-keys r [:names :chain]))
              (str label ": and the same values and refs land")))))))

(deftest a-partial-mapping-still-remembers-what-it-allocated
  (testing "The fix narrows memoisation to ids the importer ALLOCATED. A mapping
            with holes still needs those remembered, or each datom of an unnamed
            entity gets its own id (the scattering #970 fixed)."
    (let [holes #{(+ src-lo 3) (+ src-lo 7)}
          partial (reify clojure.lang.ILookup
                    (valAt [this k] (.valAt this k nil))
                    (valAt [_ k nf]
                      (cond (not (number? k)) nf
                            (contains? holes k) nf
                            (>= (long k) src-lo) (+ 1000 (- (long k) src-lo))
                            :else k)))
          r (import-observing partial)]
      (is (= n-entities (:names r)))
      (is (= (dec n-entities) (count (:chain r))) "every ref resolves to a named entity")
      (is (<= (:max-state r) (count holes))
          "only the unnamed entities are remembered"))))
