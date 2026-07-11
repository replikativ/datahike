(ns datahike.test.diff-buf-generative
  "Seeded end-to-end generative model test for diff-buf write-buffering. Random
   transact / value-upsert / retractEntity against a Clojure model, with a release+
   reconnect each cycle (forces a fressian reload from the store). This exercises the
   full stack together — PSS diff-buf + the fressian :slots handlers + commit-log + HEAD
   + crypto-hash — which the PSS-level harness (edn storage) can't reach.

   Deterministic via (java.util.Random seed): a failure reproduces from (seed, params).
   Swept over {diff-buf 0/256} × {crypto-hash off/on}. The bounded `diff-buf-generative`
   deftest runs in the suite; `run` drives bigger on-demand sweeps."
  (:require [datahike.api :as d]
            [datahike.audit :as audit]
            [datahike.index :as di]
            [konserve.core :as k]
            [clojure.test :refer [deftest is testing]])
  (:import [java.util Random]
           [org.replikativ.persistent_sorted_set Branch]))

(def schema
  [{:db/ident :id :db/valueType :db.type/long   :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :a  :db/valueType :db.type/long   :db/cardinality :db.cardinality/one}
   {:db/ident :b  :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(defn run-trial
  "One deterministic trial. Returns nil on success, or a failure map (never throws on a
   content mismatch — only real errors propagate)."
  [seed {:keys [idrange cycles ops crypto? diff-buf]}]
  (let [rng  (Random. seed)
        path (str (System/getProperty "java.io.tmpdir") "/dh-diffbuf-gen-" seed "-" diff-buf "-" (if crypto? "c" "p"))
        cfg  {:store {:backend :file :path path :id (java.util.UUID/randomUUID)}
              :schema-flexibility :write :keep-history? false
              :crypto-hash? (boolean crypto?)
              :index-config {:diff-buf-size diff-buf :branching-factor 16}}
        model (atom {})]                                  ; id -> {:a long :b string}
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (atom (d/connect cfg)) fail (atom nil)]
      (try
        (d/transact @conn schema)
        (dotimes [c cycles]
          (when-not @fail
            (dotimes [_ ops]
              (let [r (.nextInt rng 3) id (long (.nextInt rng (int idrange)))]
                (cond
                  (= r 0) (let [a (long (.nextInt rng 1000)) b (str (.nextInt rng 1000))]
                            (swap! model assoc id {:a a :b b})
                            (d/transact @conn [{:id id :a a :b b}]))
                  (and (= r 1) (contains? @model id))      ; value-upsert (change :a only)
                  (let [a (long (.nextInt rng 1000))]
                    (swap! model update id assoc :a a)
                    (d/transact @conn [{:id id :a a}]))
                  (and (= r 2) (contains? @model id))      ; retract whole entity
                  (do (swap! model dissoc id)
                      (d/transact @conn [[:db/retractEntity [:id id]]])))))
            ;; reopen — forces a cold fressian reload from the store
            (d/release @conn)
            (reset! conn (d/connect cfg))
            (let [db  @@conn
                  got (into {} (map (fn [[id a b]] [id {:a a :b b}]))
                            (d/q '[:find ?id ?a ?b :where [?e :id ?id] [?e :a ?a] [?e :b ?b]] db))]
              (when (not= @model got)
                (reset! fail {:seed seed :cycle c :params {:crypto? crypto? :diff-buf diff-buf}
                              :model-n (count @model) :got-n (count got)})))))
        @fail
        (finally
          (try (d/release @conn) (catch Throwable _))
          (try (d/delete-database cfg) (catch Throwable _)))))))

(defn run
  "Sweep grid × seeds; returns the seq of failures (empty = all good)."
  [grid seeds]
  (->> (for [params grid seed (range seeds)] (run-trial seed params))
       (remove nil?)
       vec))

(deftest diff-buf-generative
  (let [grid   (for [crypto? [false true] diff-buf [0 256]]
                 {:idrange 250 :cycles 6 :ops 35 :crypto? crypto? :diff-buf diff-buf})
        fails  (run grid 3)]
    (is (empty? fails)
        (str (count fails) " generative trial(s) diverged from model: " (pr-str (vec (take 6 fails)))))))

(deftest diff-buf-crypto-audit
  ;; Under diff-buf a Branch's content UUID folds its buffered slot diffs
  ;; (branch-content-uuid): the durable representation is (anchors + diffs), so
  ;; the hash must cover both. This guards the two properties that make that
  ;; sound: (1) a live node and its cold-deserialized twin hash identically
  ;; (verify-chain :deep? recomputes every node hash from stored bytes and
  ;; compares to the write-time address), warm and cold; (2) substituting a
  ;; slot-bearing branch blob is detected as a merkle mismatch.
  (testing "verify-chain :deep? is :ok over a diff-buf store, warm and cold; tampering detected"
    (let [cfg {:store {:backend :file
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-diffbuf-crypto-audit")
                       :id #uuid "d1ffb000-0000-0000-0000-00000000c0de"}
               :schema-flexibility :read :keep-history? true :crypto-hash? true
               :index-config {:diff-buf-size 64 :branching-factor 16}}]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn (vec (for [i (range 400)] {:db/id (inc i) :n i})))
        (dotimes [i 30] (d/transact conn [{:db/id (inc i) :n (+ 1000 i)}]))
        (is (= :ok (:status (audit/verify-chain @conn nil {:deep? true}))) "warm deep verify")
        (d/release conn))
      (let [conn  (d/connect cfg)
            db    @conn
            store (:store db)
            ;; deep verify only walks nodes reachable from the HEAD's indexes;
            ;; with :keep-history? the store also holds slot-bearing branches of
            ;; older commits — the tamper victim must be head-reachable.
            reachable (into #{} (mapcat di/-mark) [(:eavt db) (:aevt db) (:avet db)])
            slotted (vec (for [kk (k/keys store {:sync? true})
                               :let [addr (:key kk)
                                     v    (k/get store addr nil {:sync? true})]
                               :when (and (instance? Branch v)
                                          (.slotsForStorage ^Branch v))]
                           addr))
            victim (first (filter reachable slotted))
            donor  (first (remove #{victim} slotted))]
        (is (= :ok (:status (audit/verify-chain db nil {:deep? true}))) "cold deep verify")
        (is (some? victim) "a head-reachable slot-bearing branch exists (diff-buf engaged)")
        (is (some? donor) "a second slot-bearing branch exists to swap in")
        ;; swap the victim's blob for another slot-bearing node: same class, valid
        ;; blob, different (anchors + slots) content — only a slot-folding hash
        ;; catches the substitution
        (k/assoc store victim (k/get store donor nil {:sync? true}) {:sync? true})
        (let [res (audit/verify-chain db nil {:deep? true})]
          (is (= :mismatch (:status res)) "tampered store detected")
          (is (some #(= :audit/merkle-mismatch (:type %))
                    (->> res :deep :diffs (mapcat :errors)))))
        (d/release conn))
      (d/delete-database cfg))))
