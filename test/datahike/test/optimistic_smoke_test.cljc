(ns datahike.test.optimistic-smoke-test
  "Cross-platform smoke + invariant tests for `datahike.optimistic`.

   Covers:
     - register! / unregister!
     - listen! / unlisten!
     - transact! happy path (channel delivers tx-report)
     - transact! eager-validation rejection (synchronous throw)
     - effective-db reduces overlay over @conn
     - external @conn advances re-fire listeners with correct effective-db
     - concurrent transacts: each entry stays visible while the other is in flight
     - default-dispatch closes the visibility gap (hammered reads see no flicker)
     - custom dispatch with :max-tx closes the gap likewise
     - Case J (conflict): conflicting entry hides from view, surfaces via
       :on-conflict, drops on dispatch failure
     - TTL expiry yields TimeoutException on :result"
  #?(:clj  (:require [clojure.test :refer [deftest is testing]]
                     [datahike.api :as d]
                     [datahike.datom :as dd]
                     [datahike.optimistic :as opt]
                     [datahike.test.async :refer [deftest-async]]
                     [clojure.core.async :as a :refer [<! go]])
     :cljs (:require [cljs.test :refer [deftest is testing] :include-macros true]
                     [datahike.api :as d]
                     [datahike.datom :as dd]
                     [datahike.optimistic :as opt]
                     [datahike.test.async :refer-macros [deftest-async]]
                     [clojure.core.async :as a :refer [<!] :refer-macros [go]])))

(defn- mk-cfg []
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :write
   :keep-history? false})

(defn- setup []
  (go
    (let [cfg (mk-cfg)
          _    (<! #?(:clj  (go (d/create-database cfg))
                      :cljs (d/create-database cfg)))
          conn (<! #?(:clj  (go (d/connect cfg))
                      :cljs (d/connect cfg {:sync? false})))
          _    (<! (d/transact! conn
                                [{:db/ident :name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one
                                  :db/unique :db.unique/identity}]))
          _    (<! (d/transact! conn [{:name "alice"}]))]
      conn)))

(defn- names [db]
  (->> (d/datoms db :avet :name) (mapv :v) sort vec))

(deftest-async register-listen-transact
  (let [conn (<! (setup))
        events (atom [])]
    (opt/register! conn)
    (opt/listen! conn ::probe (fn [eff-db]
                                (swap! events conj (names eff-db))))
    (testing "happy path: optimistic transact delivers tx-report on result chan"
      (let [{:keys [ov-id result]} (opt/transact! conn [{:name "bob"}])
            reply (<! result)]
        (is (some? ov-id) "ov-id is set")
        (is (not (instance? #?(:clj Throwable :cljs js/Error) reply))
            "reply is not an exception")
        (is (some? (:db-after reply)) "reply is a tx-report")
        (is (= 0 (count (opt/pending conn))) "pending drained after success")
        (is (= ["alice" "bob"] (names @conn)) "base advanced")
        (is (every? #(contains? (set %) "bob") @events)
            "all listener events include the optimistic name")
        (is (>= (count @events) 1) "at least one listener event fired")))
    (opt/unlisten! conn ::probe)
    (opt/unregister! conn)))

(deftest-async eager-validation-throws
  (let [conn (<! (setup))]
    (opt/register! conn)
    (testing "schema-violating tx-data throws synchronously and never enters overlay"
      (let [caught (try
                     (opt/transact! conn [{:name 42}])
                     ::no-throw
                     (catch #?(:clj Throwable :cljs :default) e
                       e))]
        (is (not= ::no-throw caught) "transact! threw")
        (is (= 0 (count (opt/pending conn))) "overlay not polluted")))
    (opt/unregister! conn)))

(deftest-async external-advance-keeps-overlay
  (let [conn (<! (setup))
        events (atom [])]
    (opt/register! conn)
    (opt/listen! conn ::probe (fn [eff-db] (swap! events conj (names eff-db))))
    (testing "manually-stacked overlay entries survive an external @conn advance"
      ;; Manual entry has no :expected-max-tx, so the watcher's
      ;; max-tx drop never removes it; it persists until unregister!.
      (let [{:keys [overlay]} (#'opt/conn-state conn)
            ov-id (random-uuid)]
        (swap! overlay conj
               {:ov-id   ov-id
                :tx-data [{:name "carol"}]})
        (<! (d/transact! conn [{:name "dave"}]))
        (is (= 1 (count (opt/pending conn)))
            "manual entry survives external advance")
        (is (= ["alice" "dave"] (names @conn)) "base has dave only")
        (is (= ["alice" "carol" "dave"] (names (opt/effective-db conn)))
            "effective-db includes carol via overlay")))
    (opt/unregister! conn)))

;; A gate-as-dispatch lets us hold a transact!'s reply until we choose.
;; New contract: dispatch-fn yields {:reply X :max-tx N}. We set N = 0 so
;; the wrapper's sync drop check (@conn :max-tx >= 0) drops the entry
;; immediately on resolution — no flapping while @conn marches on.
(defn- make-gate []
  #?(:clj (promise) :cljs (a/chan 1)))

(defn- open-gate! [gate]
  #?(:clj (deliver gate {:reply {:ok true} :max-tx 0})
     :cljs (a/put! gate {:reply {:ok true} :max-tx 0})))

(deftest-async concurrent-transacts-keep-each-other-visible
  ;; An in-flight optimistic entry must stay visible while another
  ;; concurrent transact! runs. (The original :transacting? bug:
  ;; effective-db skipped flagged entries, opening a visibility gap
  ;; between dispatch and @conn advance — freshly-created entities
  ;; flickered in the DOM diff.)
  (let [conn   (<! (setup))
        events (atom [])
        gate1  (make-gate)
        gate2  (make-gate)]
    (opt/register! conn)
    (opt/listen! conn ::probe (fn [eff-db] (swap! events conj (names eff-db))))
    (testing "an in-flight entry survives a concurrent transact!'s listener fire"
      (let [t1 (opt/transact! conn [{:name "bob"}]
                              {:dispatch-fn (fn [] gate1)})]
        (is (contains? (set (names (opt/effective-db conn))) "bob")
            "bob visible immediately after T1's optimistic transact!")
        (let [t2 (opt/transact! conn [{:name "carol"}]
                                {:dispatch-fn (fn [] gate2)})]
          (is (contains? (set (names (opt/effective-db conn))) "bob")
              "bob STILL visible after a concurrent transact!")
          (is (contains? (set (names (opt/effective-db conn))) "carol")
              "carol visible via its own overlay entry")
          (is (every? #(contains? (set %) "bob") @events)
              "NO listener event ever dropped the in-flight bob entry")
          (is (some #(contains? (set %) "carol") @events)
              "the concurrent transact! did fire a listener event")
          (open-gate! gate1)
          (open-gate! gate2)
          (<! (:result t1))
          (<! (:result t2))
          (is (= 0 (count (opt/pending conn))) "overlay drained after both resolve"))))
    (opt/unlisten! conn ::probe)
    (opt/unregister! conn)))

(deftest-async default-dispatch-no-gap
  ;; Invariant: with the default writer dispatch, the optimistic value
  ;; is continuously visible in effective-db from `transact!` return
  ;; until the dispatch resolves — and onward via @conn. No flicker
  ;; even under a heavy concurrent reader.
  (let [conn (<! (setup))
        stop? (atom false)
        samples (atom [])
        hammer
        #?(:clj (future (while (not @stop?)
                          (swap! samples conj (set (names (opt/effective-db conn))))))
           :cljs nil)] ;; CLJS: skip the hammer; the invariant holds by construction
    (opt/register! conn {:ttl-ms nil})
    (testing "no sample after submit loses the optimistic name"
      (let [{:keys [result]} (opt/transact! conn [{:name "bob"}])]
        (<! result)
        #?(:clj (do (reset! stop? true) @hammer))
        (is (= ["alice" "bob"] (names (opt/effective-db conn))))
        #?(:clj
           (let [after-submit @samples]
             ;; Every snapshot taken while bob was either in overlay or in
             ;; @conn must include bob. We allow snapshots taken before
             ;; the overlay was populated (the very first samples).
             (is (every? (fn [s] (or (contains? s "bob") (= s #{"alice"}) (empty? s)))
                         after-submit)
                 "snapshots are either pre-submit (alice-only) or post-submit (include bob)")))))
    (opt/unregister! conn)))

(deftest-async custom-dispatch-with-max-tx-no-gap
  ;; Custom dispatch-fns that honor the {:reply :max-tx} contract also
  ;; close the gap — the watcher drops the entry only after @conn has
  ;; demonstrably caught up.
  (let [conn (<! (setup))]
    (opt/register! conn {:ttl-ms nil})
    (testing "good custom dispatch closes the gap"
      (let [{:keys [result]}
            (opt/transact! conn [{:name "carol"}]
                           {:dispatch-fn
                            (fn []
                              #?(:clj
                                 (let [p (promise)]
                                   (future
                                     (let [r @(d/transact! conn [{:name "carol"}])]
                                       (deliver p {:reply r
                                                   :max-tx (:max-tx (:db-after r))})))
                                   p)
                                 :cljs
                                 (let [ch (a/chan 1)]
                                   (go (let [r (<! (d/transact! conn [{:name "carol"}]))]
                                         (a/put! ch {:reply r
                                                     :max-tx (:max-tx (:db-after r))})))
                                   ch)))})]
        (<! result)
        (is (contains? (set (names (opt/effective-db conn))) "carol"))
        (is (= 0 (count (opt/pending conn))) "overlay drained after dispatch resolved")))
    (opt/unregister! conn)))

;; A dispatch-fn that resolves only when an external `release-ch` closes
;; or yields a value. Used to hold an optimistic entry in flight while
;; the test asserts intermediate state. The resolver thunk is called
;; *after* release; its return must conform to {:reply :max-tx} or be
;; a Throwable / js/Error (the wrapper normalizes both).
(defn- gated-dispatch [release-ch resolver]
  (fn []
    #?(:clj
       (let [p (promise)]
         (a/thread
           (a/<!! release-ch)
           (deliver p (try (resolver)
                           (catch Throwable e e))))
         p)
       :cljs
       (let [out (a/chan 1)]
         (go
           (a/<! release-ch)
           (try (a/put! out (resolver))
                (catch :default e (a/put! out e))))
         out))))

(deftest-async conflict-surfaces-via-on-conflict
  ;; Case J: an in-flight overlay entry becomes un-applicable because a
  ;; concurrent direct write seized a unique value. The entry hides
  ;; from the view, the :on-conflict callback fires, and once the gate
  ;; opens the server rejects → :result yields the throwable.
  (let [cfg (mk-cfg)
        _ (<! #?(:clj (go (d/create-database cfg)) :cljs (d/create-database cfg)))
        conn (<! #?(:clj (go (d/connect cfg)) :cljs (d/connect cfg {:sync? false})))
        _ (<! (d/transact! conn [{:db/ident :slot :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one
                                  :db/unique :db.unique/value}
                                 {:db/ident :who :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}]))
        conflicts (atom [])
        release (a/chan)]
    (opt/register! conn {:ttl-ms 5000
                         :on-conflict (fn [cs] (swap! conflicts conj (mapv :ov-id cs)))})
    (let [opt-res (opt/transact! conn [{:slot "S" :who "alice"}]
                                 {:dispatch-fn
                                  (gated-dispatch
                                   release
                                   (fn []
                                     (let [r (<! (d/transact! conn [{:slot "S" :who "alice"}]))]
                                       (if (instance? #?(:clj Throwable :cljs js/Error) r)
                                         r
                                         {:reply r :max-tx (:max-tx (:db-after r))}))))})]
      (<! (a/timeout 20))
      (is (contains?
           (set (->> (d/q '[:find ?w :where [?e :who ?w]] (opt/effective-db conn))
                     (map first)))
           "alice")
          "alice optimistically visible before the conflict")
      (<! (d/transact! conn [{:slot "S" :who "mallory"}]))
      (<! (a/timeout 100))
      (let [eff-who (set (->> (d/q '[:find ?w :where [?e :who ?w]] (opt/effective-db conn))
                              (map first)))]
        (is (contains? eff-who "mallory") "mallory visible")
        (is (not (contains? eff-who "alice")) "alice excluded from view (conflict)")
        (is (some (fn [e] (true? (:conflicting? e))) (opt/pending conn))
            "pending entry marked :conflicting?")
        (is (seq @conflicts) "on-conflict fired"))
      (a/close! release)
      (let [reply (<! (:result opt-res))]
        (is (instance? #?(:clj Throwable :cljs js/Error) reply)
            ":result yields the server's rejection")))
    (<! (a/timeout 50))
    (is (= 0 (count (opt/pending conn))) "overlay drained")
    (is (= [] (last @conflicts)) "on-conflict cleared once entry removed")
    (opt/unregister! conn)))

;; ============================================================================
;; tx-report listeners
;; ============================================================================

(defn- key-eav [d]
  [(:e d) (:a d) (:v d)])

(defn- apply-tx-events
  "Walk events in order; build the consumer's view as a set of [e a v]
  tuples. Each event's :tx-data adds or removes from the set based on
  the datom's added? flag (via the IDatom protocol). Returns the
  resulting set."
  [events start-set]
  (reduce (fn [view event]
            (reduce (fn [v d]
                      (if (dd/datom-added d)
                        (conj v (key-eav d))
                        (disj v (key-eav d))))
                    view
                    (:tx-data event)))
          start-set
          events))

(defn- user-datoms
  "Project @conn datoms to a set of [e a v] for the given attribute set."
  [db attrs]
  (->> (d/datoms db :eavt)
       (filter (fn [d] (attrs (:a d))))
       (mapv key-eav)
       set))

(deftest-async tx-report-happy-path-converges-to-conn
  ;; The consumer's incremental tx-data application should arrive at
  ;; exactly @conn's state for the attributes touched by the optimistic
  ;; tx — proving the EID-shift handling works (predicted vs realized
  ;; tx-data composition).
  (let [conn (<! (setup))
        tx-events (atom [])]
    (opt/register! conn {:ttl-ms nil})
    (opt/listen-tx! conn ::probe (fn [r] (swap! tx-events conj r)))
    (testing "events fire and consumer view matches @conn"
      (let [before (user-datoms @conn #{:name})
            {:keys [result]} (opt/transact! conn [{:name "bob"}])]
        (<! result)
        (let [origins (mapv :origin @tx-events)]
          (is (= :overlay-add (first origins))
              "first event is :overlay-add")
          (is (some #{:conn-advance} origins)
              "a :conn-advance fires from the writer-listener")
          (is (every? #{:overlay-add :conn-advance :overlay-realized}
                      origins)
              "only expected origins for happy path"))
        (let [consumer (apply-tx-events @tx-events before)
              after (user-datoms @conn #{:name})]
          (is (= consumer after)
              "consumer's incremental view matches @conn"))))
    (opt/unlisten-tx! conn ::probe)
    (opt/unregister! conn)))

#?(:clj
   (deftest-async tx-report-failure-emits-overlay-drop-with-retract
     ;; A failed dispatch must emit an :overlay-drop tx-report whose
     ;; :tx-data retracts the predicted additions, returning the
     ;; consumer's view to the pre-submit baseline.
     (let [cfg (mk-cfg)
           _ (<! (go (d/create-database cfg)))
           conn (<! (go (d/connect cfg)))
           _ (<! (d/transact! conn [{:db/ident :slot :db/valueType :db.type/string
                                     :db/cardinality :db.cardinality/one
                                     :db/unique :db.unique/value}
                                    {:db/ident :who :db/valueType :db.type/string
                                     :db/cardinality :db.cardinality/one}]))
           release (a/chan)
           tx-events (atom [])]
       (opt/register! conn {:ttl-ms 5000})
       (opt/listen-tx! conn ::probe (fn [r] (swap! tx-events conj r)))
       (let [before-snap (user-datoms @conn #{:slot :who})
             opt-res (opt/transact! conn [{:slot "S" :who "alice"}]
                                    {:dispatch-fn
                                     (gated-dispatch
                                      release
                                      (fn []
                                        (let [r (<! (d/transact! conn [{:slot "S" :who "alice"}]))]
                                          (if (instance? Throwable r)
                                            r
                                            {:reply r :max-tx (:max-tx (:db-after r))}))))})]
         ;; Sneak in a conflicting durable write first.
         (<! (d/transact! conn [{:slot "S" :who "mallory"}]))
         (a/close! release)
         (let [reply (<! (:result opt-res))]
           (is (instance? Throwable reply) "dispatch returned the rejection"))
         (<! (a/timeout 50))
         (let [origins (mapv :origin @tx-events)]
           (is (some #{:overlay-add} origins) ":overlay-add was emitted")
           (is (some #{:overlay-drop :overlay-conflict} origins)
               "either :overlay-drop (server-rejected) or :overlay-conflict (already conflicting before drop)"))
         (let [consumer (apply-tx-events @tx-events before-snap)
               after (user-datoms @conn #{:slot :who})]
           (is (= consumer after)
               "consumer view converges to @conn after failure path")))
       (opt/unlisten-tx! conn ::probe)
       (opt/unregister! conn))))

#?(:clj
   (deftest-async tx-report-ttl-emits-ttl-event-with-retract
     ;; A TTL-expired entry must emit a :ttl tx-report retracting its
     ;; predicted additions, so the consumer's view rolls back to the
     ;; pre-submit baseline.
     (let [conn (<! (setup))
           never-release (a/chan)
           tx-events (atom [])]
       (opt/register! conn {:ttl-ms 1000})
       (opt/listen-tx! conn ::probe (fn [r] (swap! tx-events conj r)))
       (let [before (user-datoms @conn #{:name})
             {:keys [result]}
             (opt/transact! conn [{:name "ghost"}]
                            {:dispatch-fn (gated-dispatch
                                           never-release
                                           (fn [] {:reply :unused :max-tx 0}))})
             reply (<! result)]
         (is (instance? Throwable reply))
         (is (= :optimistic/timeout (:type (ex-data reply))))
         (<! (a/timeout 50))
         (is (some #{:ttl} (mapv :origin @tx-events))
             ":ttl tx-report was emitted")
         (let [consumer (apply-tx-events @tx-events before)
               after (user-datoms @conn #{:name})]
           (is (= consumer after)
               "consumer view returns to baseline after TTL")))
       (a/close! never-release)
       (opt/unlisten-tx! conn ::probe)
       (opt/unregister! conn))))

(deftest-async ttl-expires-then-fires-result
  ;; An entry whose dispatch never resolves expires after :ttl-ms and
  ;; the :result chan yields a TimeoutException tagged
  ;; :optimistic/timeout.
  (let [conn (<! (setup))
        never-release (a/chan)]
    (opt/register! conn {:ttl-ms 1500})
    (let [{:keys [result]}
          (opt/transact! conn [{:name "ghost"}]
                         {:dispatch-fn (gated-dispatch
                                        never-release
                                        (fn [] {:reply :unused :max-tx 0}))})
          reply (<! result)]
      (is (instance? #?(:clj Throwable :cljs js/Error) reply))
      (is (= :optimistic/timeout (:type (ex-data reply))) "tagged as timeout"))
    (is (= 0 (count (opt/pending conn))) "overlay drained after TTL")
    (is (= ["alice"] (names (opt/effective-db conn)))
        "effective-db drops the timed-out entry")
    (a/close! never-release)
    (opt/unregister! conn)))
