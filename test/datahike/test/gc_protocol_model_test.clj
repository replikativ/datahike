(ns datahike.test.gc-protocol-model-test
  "Executable specification only; this namespace is not production coordination."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.set :as set]))

(def initial {:objects #{:old :live} :roots #{:live} :holder nil
              :collector :claim :publisher :claim})

(defn safe? [{:keys [objects roots]}] (set/subset? roots objects))

(defn advance [s actor validate?]
  (case (get s actor)
    :claim (when (nil? (:holder s)) (assoc s :holder actor actor :work))
    :work (when (= actor (:holder s))
            (if (= actor :collector)
              (assoc s :candidates (set/difference (:objects s) (:roots s)) actor :delete)
              (cond-> (assoc s actor :release)
                (or (not validate?) (contains? (:objects s) :old)) (update :roots conj :old))))
    :delete (when (= actor (:holder s))
              (-> s (update :objects set/difference (:candidates s)) (assoc actor :release)))
    :release (when (= actor (:holder s)) (assoc s :holder nil actor :done))
    :done nil))

(defn schedules [start validate?]
  (loop [pending [[start []]] results []]
    (if-let [[s path] (peek pending)]
      (let [nexts (keep (fn [actor]
                          (when-let [n (advance s actor validate?)] [n (conj path actor)]))
                        [:collector :publisher])]
        (recur (into (pop pending) nexts) (conj results [s path])))
      results)))

(deftest non-expiring-gate-and-validation-preserve-root-closure
  (let [states (schedules initial true)]
    (is (> (count states) 10))
    (is (every? (comp safe? first) states))
    (is (= #{#{:live} #{:live :old}}
           (into #{} (keep (fn [[s _]]
                             (when (and (= :done (:collector s)) (= :done (:publisher s))) (:roots s))))
                 states)))))

(deftest serialization-alone-cannot-promote-a-deleted-snapshot
  (is (some (comp not safe? first) (schedules initial false))))

(deftest frozen-owner-cannot-expire-or-be-stolen
  (let [frozen (-> initial (advance :collector true) (advance :collector true))]
    (is (= :delete (:collector frozen)))
    (is (nil? (advance frozen :publisher true)))
    (testing "A crashed holder intentionally prevents progress; time is not an action."
      (is (= :collector (:holder frozen))))
    (testing "Unsafe TTL takeover admits publication, followed by the stale delete."
      (let [taken (assoc frozen :holder nil)
            published (-> taken (advance :publisher true) (advance :publisher true))
            resumed (update published :objects set/difference (:candidates frozen))]
        (is (safe? published))
        (is (not (safe? resumed)))))))

(defn graph-walk
  "Model immutable adjacency reuse. Cache namespace includes store incarnation.
   The authoritative graph supplies cache misses; malformed entries are misses."
  [graph roots incarnation cache]
  (loop [pending (seq roots) live #{} cache cache reads 0 hits 0]
    (if-let [node (first pending)]
      (if (contains? live node)
        (recur (next pending) live cache reads hits)
        (let [key [incarnation node]
              cached (get cache key)
              hit? (and (= :complete (:status cached)) (set? (:children cached)))
              children (if hit? (:children cached)
                           (if (contains? graph node) (get graph node)
                               (throw (ex-info "Missing graph node" {:node node}))))]
          (recur (concat (next pending) children) (conj live node)
                 (assoc cache key {:status :complete :children children})
                 (+ reads (if hit? 0 1)) (+ hits (if hit? 1 0)))))
      {:live live :cache cache :reads reads :hits hits})))

(deftest remembered-edges-are-not-sticky-live-marks
  (let [graph {:a #{:shared :only-a} :b #{:shared} :shared #{:payload}
               :only-a #{} :payload #{}}
        first-pass (graph-walk graph [:a :b] :store-1 {})
        next-pass (graph-walk graph [:b] :store-1 (:cache first-pass))
        oracle (graph-walk graph [:b] :store-1 {})
        reincarnated (graph-walk {:b #{:new} :new #{}} [:b] :store-2 (:cache first-pass))]
    (is (= (:live oracle) (:live next-pass)))
    (is (= #{:b :shared :payload} (:live next-pass)))
    (is (= 0 (:reads next-pass)))
    (is (= 3 (:hits next-pass)))
    (is (= #{:b :new} (:live reincarnated)))
    (is (= 2 (:reads reincarnated)))))

(deftest incomplete-model-cache-is-a-miss
  (doseq [entry [{:status :partial :children #{}} {:status :complete :children nil}]]
    (let [r (graph-walk {:root #{:child} :child #{}} [:root] :store {[:store :root] entry})]
      (is (= #{:root :child} (:live r)))
      (is (= 2 (:reads r))))))
