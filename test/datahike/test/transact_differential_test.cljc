(ns datahike.test.transact-differential-test
  "Seeded generative differential testing for the TRANSACTION path: the
   synchronous transaction core is the semantic reference; every generated
   transaction must produce the SAME tx-report under async-cold execution
   (d/with {:sync? false} against an async-only store) — or fail with the
   clean storage fault, never wrong data.

   Mirrors the query engine's three-mode methodology: the cljs axis replays
   a fixed seeded case stream as sync (warm) and async-cold, with an exact
   cold-coverage RATCHET; the JVM leg replays the same stream through the
   sync core only, pinning generator health and cross-platform determinism
   (store-less dbs stamp no txInstant, so reports are value-deterministic).

   The ratchet measures the async-transaction conversion: prefetch-based
   coverage today; the dual transaction spine over async tree ops should
   drive it to full coverage (rebalancing sibling loads become awaited
   reads instead of best-effort warming)."
  (:require
   #?(:cljs [cljs.test :refer-macros [is]]
      :clj  [clojure.test :refer [deftest is]])
   [clojure.test.check.generators :as gen]
   #?@(:clj [[clojure.test.check.clojure-test :refer [defspec]]
             [clojure.test.check.properties :as prop]])
   [datahike.api :as d]
   [datahike.db :as ddb]
   #?@(:cljs [[clojure.core.async :as a :refer [<!] :refer-macros [go]]
              [clojure.test.check.random :as tc-random]
              [clojure.test.check.rose-tree :as rose]
              [datahike.test.async :refer-macros [deftest-async]]
              [datahike.test.async-mode :as am]])))

(def ^:private num-cases
  (or (some-> #?(:clj (System/getenv "DATAHIKE_TX_DIFF_CASES")
                 :cljs (when (exists? js/process)
                         (aget (.-env js/process) "DATAHIKE_TX_DIFF_CASES")))
              parse-long)
      #?(:clj 1000 :cljs 200)))

;; Fixture: unique-identity upserts, a ref+component chain (5 -> 6) for
;; retraction cascades, card-many tags, and enough plain entities that
;; new-entity insertion paths are non-trivial.
(defonce ^:private base-db
  (delay
    (-> (ddb/empty-db {:td-email {:db/unique :db.unique/identity}
                       :td-friend {:db/valueType :db.type/ref}
                       :td-part {:db/valueType :db.type/ref
                                 :db/isComponent true}
                       :td-tags {:db/cardinality :db.cardinality/many}})
        (d/db-with (into [{:db/id 1 :td-email "a@x" :td-name "alice" :td-score 10}
                          {:db/id 2 :td-email "b@x" :td-name "bob" :td-score 20
                           :td-tags ["x" "y"]}
                          {:db/id 3 :td-name "carol" :td-friend 1}
                          {:db/id 5 :td-name "holder" :td-part 6}
                          {:db/id 6 :td-name "component"}]
                         (mapv (fn [i] {:db/id (+ 20 i) :td-name (str "n" i)})
                               (range 120)))))))

(def ^:private gen-op
  (gen/one-of
   [(gen/fmap (fn [i] [{:db/id (- -1 i) :td-name (str "new" i)}])
              (gen/choose 0 5))
    (gen/fmap (fn [v] [[:db/add 1 :td-score v]])
              (gen/choose 100 200))
    (gen/fmap (fn [v] [{:td-email "a@x" :td-score v}])
              (gen/choose 300 400))
    ;; within-tx double upsert of the same identity
    (gen/fmap (fn [v] [{:td-email "b@x" :td-score v}
                       {:td-email "b@x" :td-name (str "renamed" v)}])
              (gen/choose 500 600))
    (gen/fmap (fn [v] [[:db/add [:td-email "a@x"] :td-score v]])
              (gen/choose 700 800))
    ;; cas that SUCCEEDS against the fixture value
    (gen/return [[:db/cas 1 :td-score 10 11]])
    ;; cas that FAILS -> raise-agreement dimension
    (gen/return [[:db/cas 1 :td-score 999 1000]])
    (gen/return [[:db/retract 2 :td-name "bob"]])
    ;; retraction cascade through the component chain
    (gen/return [[:db/retractEntity 5]])
    (gen/return [[:db/retractEntity [:td-email "b@x"]]])
    (gen/fmap (fn [t] [[:db/add 2 :td-tags (str "t" t)]])
              (gen/choose 0 9))
    (gen/return [[:db/retract 2 :td-tags "x"]])]))

(def ^:private gen-tx
  ;; 1-3 op groups concatenated — cross-op interactions inside one
  ;; transaction (upsert-then-retract, double upserts, cascade + assert)
  ;; are where the transient-read subtleties live
  (gen/fmap (fn [groups] (vec (apply concat groups)))
            (gen/vector gen-op 1 3)))

(defn- report-norm [r]
  {:tx-data (mapv (fn [d] [(:e d) (:a d) (:v d) (:added d)]) (:tx-data r))
   :tempids (into {} (remove (fn [[k _]] (= k :db/current-tx))) (:tempids r))})

(defn- run-sync [db tx-data]
  (try {:result (report-norm (d/with db {:tx-data tx-data}))}
       (catch #?(:clj Exception :cljs :default) e {:error e})))

#?(:clj
   (defspec tx-generator-produces-deterministic-reports
     {:num-tests num-cases :seed 1721170000042}
     (prop/for-all [tx-data gen-tx]
                   ;; generator health: every generated tx either commits or
                   ;; raises DETERMINISTICALLY (same outcome on re-run) — the
                   ;; oracle the cljs axis compares against must be stable
                   (let [r1 (run-sync @base-db tx-data)
                         r2 (run-sync @base-db tx-data)]
                     (and (= (some? (:error r1)) (some? (:error r2)))
                          (= (:result r1) (:result r2)))))))

#?(:cljs
   (def ^:private expected-cold-covered
     "Exact number of the num-cases replayed transactions that COMPLETE
      (rather than cleanly fault) under async-cold d/with. Bump as the
      async-transaction conversion lands; a drop names a regressed seam.
      The dual transaction spine over async tree ops should take this to
      num-cases minus the raising transactions."
     ;; History: 129 on the prefetch/warming implementation (24 clean
     ;; faults = the rebalancing residue static warming cannot reach).
     ;; 153 = the dual transaction spine over async tree writes — every
     ;; non-raising transaction completes cold; the 47 raisers raise
     ;; identically in both modes. 153 is the ceiling for this stream.
     153))

#?(:cljs
   (defn- seeded-tx-seq [n seed]
     (let [r0 (tc-random/make-random seed)]
       (loop [i 0 r r0 size 0 acc []]
         (if (= i n)
           acc
           (let [[r1 r2] (tc-random/split r)]
             (recur (inc i) r2 (mod (inc size) 200)
                    (conj acc (rose/root (gen/call-gen gen-tx r1 size))))))))))

#?(:cljs
   (defonce ^:private tx-flush-handle
     ;; ONE flush per fixture (see am/flush-db!)
     (delay (am/flush-db! @base-db))))

#?(:cljs
   (deftest-async tx-three-mode-axis
     (let [cases (seeded-tx-seq num-cases 1721170000042)
           outcomes
           (loop [idx 0 [tx-data & more] cases acc []]
             (if (nil? tx-data)
               acc
               (let [sync-out (run-sync @base-db tx-data)
                     cold (am/cold-db @base-db @tx-flush-handle)
                     ch (a/promise-chan)
                     _ ((d/with cold {:tx-data tx-data :sync? false})
                        (fn [v] (a/put! ch {:result (report-norm v)}))
                        (fn [e] (a/put! ch (if (= :storage/sync-read-unavailable
                                                  (:error (ex-data e)))
                                             {:fault e}
                                             {:error e}))))
                     cold-out (<! ch)
                     outcome
                     (cond
                       (:error sync-out)
                       (if (or (:error cold-out) (:fault cold-out))
                         :raises
                         :raise-diverged)
                       (:fault cold-out) :cold-faulted
                       (:error cold-out) :cold-error
                       (not= (:result sync-out) (:result cold-out)) :cold-diverged
                       :else :cold-covered)]
                 (when (#{:raise-diverged :cold-error :cold-diverged} outcome)
                   (is false
                       (str outcome " on tx case " idx ": " (pr-str tx-data)
                            "\n  sync: " (pr-str (or (:result sync-out)
                                                     (some-> (:error sync-out) ex-message)))
                            "\n  cold: " (pr-str (or (:result cold-out)
                                                     (some-> (:error cold-out) ex-message)
                                                     (some-> (:fault cold-out) ex-message))))))
                 (recur (inc idx) more (conj acc outcome)))))
           covered (count (filter #(= :cold-covered %) outcomes))]
       (is (= expected-cold-covered covered)
           (str "tx cold-coverage ratchet: " covered "/" (count outcomes)
                " covered (expected " expected-cold-covered "); outcomes: "
                (pr-str (frequencies outcomes)))))))
