(ns datahike.test.ivm-test
  "IVM prototype: shape tests for the delta class, fallback classification,
   and the seeded ratchet asserting the core invariant — the folded
   subscription state and the replayed change stream both equal a full
   re-query after every transaction."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.ivm :as ivm]
            [datahike.test.model.rng :as rng]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- watched
  "Subscribe with a callback that maintains a replayed result set from the
   change stream, so tests can check the emitted deltas — not just the
   internal weights — against re-query."
  [conn opts]
  (let [replay (atom #{})
        events (atom [])
        sub (ivm/subscribe! conn
                            (assoc opts
                                   :callback
                                   (fn [{:keys [reset result changes] :as m}]
                                     (swap! events conj m)
                                     (if reset
                                       (reset! replay result)
                                       (swap! replay
                                              (fn [s]
                                                (reduce (fn [s [t w]]
                                                          (if (pos? w) (conj s t) (disj s t)))
                                                        s changes)))))))]
    {:sub sub :replay replay :events events}))

(defn- consistent? [conn {:keys [sub replay]} query & args]
  ;; subscriptions operate at set-of-tuples level; aggregate finds come
  ;; back from q as vectors
  (let [expected (set (apply d/q query @conn args))]
    (and (= expected (ivm/result sub))
         (= expected @replay))))

;; ---------------------------------------------------------------------------
;; shape tests

(deftest entity-group-multiplicity
  (let [conn (fresh-conn)
        query '[:find ?n :where [?e :user/active? true] [?e :user/name ?n]]
        {:keys [sub events] :as w} (watched conn {:query query})]
    (is (= :delta (ivm/strategy sub)))
    (d/transact conn [{:db/id 1 :user/name "Ada" :user/active? true}
                      {:db/id 2 :user/name "Grace" :user/active? true}])
    (is (consistent? conn w query))
    (testing "duplicate name adds weight without emission"
      (let [n-events (count @events)]
        (d/transact conn [{:db/id 3 :user/name "Ada" :user/active? true}])
        (is (= n-events (count @events)))
        (is (consistent? conn w query))))
    (testing "retracting one of two duplicates emits nothing"
      (let [n-events (count @events)]
        (d/transact conn [[:db/retract 1 :user/active? true]])
        (is (= n-events (count @events)))
        (is (consistent? conn w query))))
    (testing "retracting the last emits -tuple"
      (d/transact conn [[:db/retract 3 :user/active? true]])
      (is (= [[["Ada"] -1]] (:changes (last @events))))
      (is (consistent? conn w query)))
    (testing "irrelevant attributes do not wake the view"
      (let [n-events (count @events)]
        (d/transact conn [[:db/add 2 :user/email "g@x"]])
        (is (= n-events (count @events)))))
    (ivm/unsubscribe! (:sub w))))

(deftest value-join-both-sides
  (let [conn (fresh-conn)
        query '[:find ?n :where [?e :user/friend ?f] [?f :user/name ?n]]
        w (watched conn {:query query})]
    (is (= :delta (ivm/strategy (:sub w))))
    (testing "producer side first: friend edge to an unnamed entity"
      (d/transact conn [[:db/add 1 :user/friend 7]])
      (is (consistent? conn w query)))
    (testing "consumer side later: naming the entity emits through the join"
      (d/transact conn [[:db/add 7 :user/name "Grace"]])
      (is (= [[["Grace"] 1]] (:changes (last @(:events w)))))
      (is (consistent? conn w query)))
    (testing "retracting the edge retracts through the join"
      (d/transact conn [[:db/retract 1 :user/friend 7]])
      (is (consistent? conn w query)))
    (ivm/unsubscribe! (:sub w))))

(deftest predicate-and-single-clause
  (let [conn (fresh-conn)
        pq '[:find ?n :where [?e :user/age ?a] [(< ?a 40)] [?e :user/name ?n]]
        sq '[:find ?e ?n :where [?e :user/name ?n]]
        wp (watched conn {:query pq})
        ws (watched conn {:query sq})]
    (is (= :delta (ivm/strategy (:sub wp))))
    (is (= :delta (ivm/strategy (:sub ws))))
    (d/transact conn [{:db/id 1 :user/name "Ada" :user/age 36}
                      {:db/id 2 :user/name "Charles" :user/age 44}])
    (is (consistent? conn wp pq))
    (is (consistent? conn ws sq))
    ;; card-one replacement arrives as retract+add in one tx
    (d/transact conn [[:db/add 1 :user/age 41]])
    (is (consistent? conn wp pq))
    (d/transact conn [[:db/add 2 :user/age 39]])
    (is (consistent? conn wp pq))
    (ivm/unsubscribe! (:sub wp))
    (ivm/unsubscribe! (:sub ws))))

(deftest wildcard-and-in-scalar
  (let [conn (fresh-conn)
        wq '[:find ?n :where [?e :user/alias _] [?e :user/name ?n]]
        iq '[:find ?n :in $ ?act :where [?e :user/active? ?act] [?e :user/name ?n]]
        ww (watched conn {:query wq})
        wi (watched conn {:query iq :args [true]})]
    (is (= :delta (ivm/strategy (:sub ww))))
    (is (= :delta (ivm/strategy (:sub wi))))
    (d/transact conn [{:db/id 1 :user/name "Ada" :user/alias "countess" :user/active? true}
                      {:db/id 2 :user/name "Alan" :user/active? false}])
    (is (consistent? conn ww wq))
    (is (consistent? conn wi iq true))
    (d/transact conn [[:db/retract 1 :user/alias "countess"]
                      [:db/add 2 :user/active? true]])
    (is (consistent? conn ww wq))
    (is (consistent? conn wi iq true))
    (ivm/unsubscribe! (:sub ww))
    (ivm/unsubscribe! (:sub wi))))

(deftest self-join
  (let [conn (fresh-conn)
        query '[:find ?a ?c :where [?a :edge ?b] [?b :edge ?c]]
        w (watched conn {:query query})]
    (is (= :delta (ivm/strategy (:sub w))))
    (d/transact conn [[:db/add 1 :edge 2]])
    (is (consistent? conn w query))
    ;; both clauses touched in one tx: δ⋈δ cross term
    (d/transact conn [[:db/add 2 :edge 3] [:db/add 3 :edge 1]])
    (is (consistent? conn w query))
    (d/transact conn [[:db/retract 2 :edge 3]])
    (is (consistent? conn w query))
    (ivm/unsubscribe! (:sub w))))

(deftest fallback-classification-and-correctness
  (let [conn (fresh-conn)
        agg '[:find (count ?e) :where [?e :user/active? true]]
        neg '[:find ?n :where [?e :user/name ?n] (not [?e :user/banned? true])]
        gel '[:find ?n ?nick :where [?e :user/name ?n] [(get-else $ ?e :user/nickname "-") ?nick]]
        subs (mapv (fn [q] (watched conn {:query q})) [agg neg gel])]
    (is (every? #(= :reeval (ivm/strategy (:sub %))) subs))
    (d/transact conn [{:db/id 1 :user/name "Ada" :user/active? true}
                      {:db/id 2 :user/name "Grace" :user/active? true :user/banned? true}])
    (is (consistent? conn (nth subs 0) agg))
    (is (consistent? conn (nth subs 1) neg))
    (is (consistent? conn (nth subs 2) gel))
    (d/transact conn [[:db/retract 2 :user/banned? true]
                      [:db/add 1 :user/nickname "countess"]])
    (is (consistent? conn (nth subs 0) agg))
    (is (consistent? conn (nth subs 1) neg))
    (is (consistent? conn (nth subs 2) gel))
    (run! #(ivm/unsubscribe! (:sub %)) subs)))

;; ---------------------------------------------------------------------------
;; the ratchet: seeded random workload, invariant checked after every tx

(def ^:private names ["ada" "grace" "alan" "edsger" "barbara" "donald" "leslie" "tony"])

(defn- gen-op [prng conn]
  (let [eid (rng/random-in-range prng 1 24)
        kind (rng/weighted-sample-rng
              prng
              [[:set-name 3] [:set-active 3] [:unset-active 2] [:set-age 3]
               [:set-friend 2] [:unset-friend 1] [:set-edge 2] [:unset-edge 1]
               [:set-alias 1] [:unset-alias 1] [:set-banned 1]])
        current (fn [attr] (ffirst (d/q '[:find ?v :in $ ?e ?a :where [?e ?a ?v]]
                                        @conn eid attr)))]
    (case kind
      :set-name [:db/add eid :user/name (rng/rand-nth-rng prng names)]
      :set-active [:db/add eid :user/active? true]
      :unset-active (when (current :user/active?) [:db/retract eid :user/active? true])
      :set-age [:db/add eid :user/age (rng/random-in-range prng 18 60)]
      :set-friend [:db/add eid :user/friend (rng/random-in-range prng 1 24)]
      :unset-friend (when-let [v (current :user/friend)] [:db/retract eid :user/friend v])
      :set-edge [:db/add eid :edge (rng/random-in-range prng 1 24)]
      :unset-edge (when-let [v (current :edge)] [:db/retract eid :edge v])
      :set-alias [:db/add eid :user/alias (str "alias" (rng/random-in-range prng 1 4))]
      :unset-alias (when-let [v (current :user/alias)] [:db/retract eid :user/alias v])
      :set-banned [:db/add eid :user/banned? true])))

(deftest ivm-ratchet
  (let [conn (fresh-conn)
        prng (rng/create-splitmix 42)
        queries {:active '[:find ?n :where [?e :user/active? true] [?e :user/name ?n]]
                 :friends '[:find ?n :where [?e :user/friend ?f] [?f :user/name ?n]]
                 :young '[:find ?n :where [?e :user/age ?a] [(< ?a 40)] [?e :user/name ?n]]
                 :all-names '[:find ?e ?n :where [?e :user/name ?n]]
                 :aliased '[:find ?n :where [?e :user/alias _] [?e :user/name ?n]]
                 :paths '[:find ?a ?c :where [?a :edge ?b] [?b :edge ?c]]
                 :count '[:find (count ?e) :where [?e :user/active? true]]
                 :unbanned '[:find ?n :where [?e :user/name ?n] (not [?e :user/banned? true])]}
        subs (into {} (map (fn [[k q]] [k (watched conn {:query q})])) queries)
        expected-strategy {:active :delta :friends :delta :young :delta :all-names :delta
                           :aliased :delta :paths :delta :count :reeval :unbanned :reeval}]
    (doseq [[k s] expected-strategy]
      (is (= s (ivm/strategy (:sub (subs k)))) (str "strategy of " k)))
    (dotimes [step 60]
      (let [n-ops (rng/random-in-range prng 1 3)
            tx (into [] (keep (fn [_] (gen-op prng conn))) (range n-ops))]
        (when (seq tx)
          (d/transact conn {:tx-data tx}))
        (doseq [[k q] queries]
          (is (consistent? conn (subs k) q)
              (str "step " step " view " k " tx " tx)))))
    (let [emissions (transduce (map (fn [[_ w]] (count @(:events w)))) + subs)]
      (is (> emissions 50) "workload exercised the change stream"))
    (run! (fn [[_ w]] (ivm/unsubscribe! (:sub w))) subs)))
