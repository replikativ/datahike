(ns datahike.test.sort-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.sort :as sort]))

(defn- instrumented-backend
  ([] (instrumented-backend {}))
  ([{:keys [runs fault reader-limit] :or {runs {} reader-limit 128}}]
   (let [state (atom {:runs runs :events [] :counts {} :prepared 0 :read-values 0
                      :readers 0 :writers 0 :peak-readers 0 :peak-writers 0})
         event! (fn [op id]
                  (let [after (swap! state #(-> % (update :events conj [op id])
                                                (update-in [:counts [op id]] (fnil inc 0))))]
                    (when fault (fault op id (get-in after [:counts [op id]])))))
         acquire! (fn [kind]
                    (let [after (swap! state update kind inc)]
                      (swap! state update (if (= kind :readers) :peak-readers :peak-writers)
                             max (get after kind))))]
     {:state state
      :backend
      {:prepare (fn [record]
                  (event! :prepare nil)
                  (swap! state update :prepared inc)
                  {:record record :charge (or (:charge record) 1)})
       :open-reader
       (fn [id]
         (event! :open-reader id)
         (when (>= (:readers @state) reader-limit)
           (throw (ex-info "Reader limit exceeded" {:test/error :reader-limit})))
         (when-not (contains? (:runs @state) id)
           (throw (ex-info "Unknown run" {:run id})))
         (acquire! :readers)
         (let [remaining (atom (seq (get-in @state [:runs id])))
               closed? (atom false)]
           {:next! (fn []
                     (event! :next id)
                     (let [entry (first @remaining)]
                       (swap! remaining next)
                       (when entry (swap! state update :read-values inc))
                       entry))
            :close! (fn []
                      (when (compare-and-set! closed? false true)
                        (reset! remaining nil)
                        (swap! state update :readers dec)
                        (event! :close-reader id)))}))
       :open-writer
       (fn [id]
         (event! :open-writer id)
         (acquire! :writers)
         (let [entries (atom []) closed? (atom false)]
           {:write! (fn [entry]
                      (event! :write id)
                      ;; Model a codec: cached engine keys are not serialized.
                      (swap! entries conj (select-keys entry [:record :charge])))
            :close! (fn []
                      (when (compare-and-set! closed? false true)
                        (swap! state #(-> % (update :writers dec) (assoc-in [:runs id] @entries)))
                        (reset! entries [])
                        (event! :close-writer id)))}))
       :delete! (fn [id] (event! :delete id) (swap! state update :runs dissoc id))
       :move! (fn [from to]
                (event! :move from)
                (swap! state update :runs #(assoc (dissoc % from) to (get % from))))}})))

(defn- thrown [f]
  (try (f) nil (catch #?(:clj Throwable :cljs :default) error error)))

(defn- no-handles? [state]
  (and (zero? (:readers @state)) (zero? (:writers @state))))

(deftest window-limits-and-in-memory-key-decoration
  (let [calls (atom 0) records (vec (reverse (range 40)))]
    (is (= (range 40) (sort/sort-window records {:key-fn #(do (swap! calls inc) %)})))
    (is (= 40 @calls)))
  (doseq [[record-cap byte-cap expected] [[3 nil [3 3 1]] [20 5 [2 2 2 1]]]]
    (let [{:keys [backend state]} (instrumented-backend)
          records (mapv #(hash-map :key % :charge 2) (range 7))
          runs (sort/initial-runs! records {:key-fn :key} backend
                                   {:window-records record-cap :window-bytes byte-cap})]
      (is (= expected (mapv #(count (get-in @state [:runs [0 %]])) (range runs))))
      (is (= 7 (:prepared @state)))
      (is (no-handles? state)))))

(deftest stable-ties-and-key-counts-through-balanced-passes
  (doseq [n [0 1 65 257 2053] fan [2 7] keyed? [false true]]
    (let [{:keys [backend state]} (instrumented-backend {:reader-limit fan})
          calls (atom 0)
          records (mapv #(vector (mod % 5) %) (range n))
          order (if keyed? {:key-fn #(do (swap! calls inc) (first %))}
                    {:cmp #(compare (first %1) (first %2))})
          id (sort/sorted-file! records order backend
                                {:window-records 3 :window-bytes nil :fan-in fan})]
      (is (= (vec (sort-by first records)) (mapv :record (get-in @state [:runs id])))
          (str n " records; fan-in " fan "; keyed " keyed?))
      (is (= n (:prepared @state)))
      (when keyed?
        (is (= (+ n (:read-values @state)) @calls)
            "one key per prepared or decoded entry, never per comparison"))
      (is (<= (:peak-readers @state) fan))
      (is (<= (:peak-writers @state) 1))
      (is (no-handles? state)))))

(deftest partial-final-pass-merges-only-two-of-sixty-five-runs
  (let [{:keys [backend state]} (instrumented-backend {:reader-limit 64})
        opts {:window-records 1 :window-bytes nil :fan-in 64}
        count-runs (sort/initial-runs! (range 65) {:cmp compare} backend opts)
        runs (sort/reduce-runs! count-runs {:cmp compare} backend opts)]
    (is (= 64 (count runs)))
    (is (= 2 (:read-values @state)))
    (is (= 63 (count (filter #(= :move (first %)) (:events @state)))))
    (is (= 67 (count (filter #(= :write (first %)) (:events @state)))))
    (let [{:keys [records close!]} (sort/merge-runs-resource! runs {:cmp compare} backend true)]
      (try (is (= (vec (range 65)) (vec records))) (finally (close!))))
    (is (empty? (:runs @state)))
    (is (no-handles? state))))

(deftest empty-and-single-run-avoid-unnecessary-io
  (doseq [input [[] [3 2 1]]]
    (let [{:keys [backend state]} (instrumented-backend)
          id (sort/sorted-file! input {:cmp compare} backend
                                {:window-records 5 :window-bytes nil :fan-in 2})]
      (is (= (sort input) (map :record (get-in @state [:runs id]))))
      (is (zero? (:read-values @state)))
      (is (not-any? #(contains? #{:open-reader :move} (first %)) (:events @state)))
      (is (no-handles? state)))))

(deftest explicit-early-close-is-idempotent
  (let [{:keys [backend state]}
        (instrumented-backend {:runs {[0 0] [{:record 1} {:record 3}]
                                      [0 1] [{:record 2} {:record 4}]}})
        {:keys [records close!]} (sort/merge-runs-resource! [[0 0] [0 1]] {:cmp compare} backend false)]
    (is (= 1 (first records)))
    (close!)
    (close!)
    (is (no-handles? state))
    (is (= 2 (count (filter #(= :close-reader (first %)) (:events @state)))))
    (is (= 2 (count (:runs @state))))))

(deftest acquisition-next-and-comparator-failures-close-acquired-readers
  (doseq [op [:open-reader :next :compare]]
    (let [failure (ex-info "primary" {:test/error op})
          {:keys [backend state]}
          (instrumented-backend
           {:runs {[0 0] [{:record 1}] [0 1] [{:record 2}]}
            :fault (fn [event id _]
                     (when (and (= event op) (= id [0 1])) (throw failure)))})
          order {:cmp (fn [a b] (if (= op :compare) (throw failure) (compare a b)))}]
      (is (identical? failure
                      (thrown #(sort/merge-runs-resource! [[0 0] [0 1]] order backend true))))
      (is (no-handles? state)))))

(deftest materialized-merge-failures-preserve-primary-and-close-all-handles
  (doseq [op [:open-writer :write :close-writer :next :close-reader :compare]]
    (let [failure (ex-info "primary" {:test/error op})
          cleanup (ex-info "cleanup" {:test/error :cleanup})
          failed? (atom false)
          {:keys [backend state]}
          (instrumented-backend
           {:fault (fn [event id occurrence]
                     (when (or (and (= event op) (= id [1 0]))
                               (and (contains? #{:next :close-reader} op)
                                    (= event op) (= id [0 0])
                                    (= occurrence (if (= op :next) 2 1))))
                       (reset! failed? true)
                       (throw failure))
                     (when (and @failed? (= event :close-reader) (= id [0 1])
                                (contains? #{:open-writer :write :close-writer :compare} op))
                       (throw cleanup)))})
          cmp-calls (atom 0)
          order {:cmp (fn [a b]
                        (swap! cmp-calls inc)
                        (if (= op :compare)
                          (do (reset! failed? true) (throw failure))
                          (compare a b)))}
          error (thrown #(sort/sorted-file! [1 2] order backend
                                            {:window-records 1 :window-bytes nil :fan-in 2}))]
      (is (identical? failure error) (str op))
      #?(:clj (when (contains? #{:open-writer :write :compare} op)
                (is (some #(identical? cleanup %) (.getSuppressed ^Throwable failure))
                    "cleanup does not replace the primary failure")))
      (is (no-handles? state) (str op)))))

(deftest failures-after-spilling-and-during-lazy-consumption-close-resources
  (let [failure (ex-info "input" {})
        {:keys [backend state]} (instrumented-backend)
        records (concat [3 2 1] (lazy-seq (throw failure)))]
    (is (identical? failure
                    (thrown #(sort/initial-runs! records {:cmp compare} backend
                                                 {:window-records 1 :window-bytes nil}))))
    (is (pos? (count (:runs @state))) "the failure occurred after spilling")
    (is (no-handles? state)))
  (let [failure (ex-info "later read" {})
        {:keys [backend state]}
        (instrumented-backend
         {:runs {[0 0] [{:record 1} {:record 3}] [0 1] [{:record 2} {:record 4}]}
          :fault (fn [event id occurrence]
                   (when (and (= event :next) (= id [0 0]) (= occurrence 2))
                     (throw failure)))})
        {:keys [records close!]} (sort/merge-runs-resource! [[0 0] [0 1]] {:cmp compare} backend false)]
    (is (identical? failure (thrown #(first records))))
    (is (no-handles? state))
    (close!)
    (is (no-handles? state))))
