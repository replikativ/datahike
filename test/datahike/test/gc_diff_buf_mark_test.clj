(ns datahike.test.gc-diff-buf-mark-test
  "#1112: under :diff-buf-size the GC mark walked the connection's node cache,
   whose resident children hide the durable anchors a cold read projects from."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.gc-roots :as roots]
            [datahike.index.interface :as idx]
            [datahike.index.audit :as audit]
            [datahike.versioning :as v]
            [konserve.core :as k]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]
           [org.replikativ.persistent_sorted_set Branch Leaf]))

(def families [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn raw-closure
  "Oracle: reads persisted node fields, without the marker or PSS descent."
  [store root-address inline]
  (loop [pending [[root-address inline]] result #{}]
    (if-let [[address inline] (peek pending)]
      (if (contains? result address)
        (recur (pop pending) result)
        (let [node (or inline (k/get store address nil {:sync? true}))]
          (when-not (or (instance? Branch node) (instance? Leaf node))
            (throw (ex-info "Oracle encountered a missing node" {:address address})))
          (recur (into (pop pending)
                       (when (instance? Branch node) (map #(vector % nil) (.addresses ^Branch node))))
                 (conj result address))))
      result)))

(defn record-marks [store record]
  (mapv (fn [family]
          (let [index (idx/with-storage :datahike.index/persistent-set
                        (get record (keyword (str (name family) "-key"))) (:storage store))
                inline (get record (keyword (str (name family) "-root")))
                index (cond-> index inline (idx/-seed-root! inline))]
            {:oracle (raw-closure store (audit/-merkle-root index) inline)
             :marked (idx/-mark index)}))
        families))

(deftest warm-mark-covers-cold-closure-and-survives-sweep
  (doseq [budget [1 4 64] fused? [false true]]
    (testing (str "diff-buf-size " budget " fused " fused?)
      (let [id (random-uuid)
            cfg {:store {:backend :file :id id :path (str "/tmp/dh-diff-buf-mark-" id)}
                 :schema-flexibility :write :keep-history? true :crypto-hash? true
                 :fuse-index-roots? fused? :index-config {:branching-factor 8 :diff-buf-size budget}}]
        (d/create-database cfg)
        (try
          (let [conn (d/connect cfg) released? (atom false)]
            (try
              (d/transact conn [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}])
              (d/transact conn (mapv #(hash-map :n %) (range 600)))
              (let [ids (mapv :e (d/datoms @conn :aevt :n))
                    store (:store @conn)
                    old @conn
                    pin (roots/pin! old {:ttl-ms nil} {:sync? true})
                    old-values (d/q '[:find ?e ?n :where [?e :n ?n]] old)
                    old-t (:max-tx old)]
                (v/branch! conn :db :retained)
                (doseq [round (range 8)]
                  (d/transact conn (mapv (fn [j] {:db/id (nth ids (mod (+ (* round 17) (* j 7)) 600))
                                                  :n (+ 10000 (* round 32) j)}) (range 32))))
                (d/transact conn (mapv #(vector :db/retractEntity %) (take 20 (drop 10 ids))))
                (let [expected (d/q '[:find ?e ?n :where [?e :n ?n]] @conn)]
                  (doseq [branch [:db :retained]
                          {:keys [oracle marked]} (record-marks store (k/get store branch nil {:sync? true}))]
                    (is (= oracle marked)))
                  (is (= 580 (count expected)))
                  ;; Collapse history: only the current head and the :retained head stay roots.
                  (<?? S (gc/gc-storage! @conn (Date.) {:min-age-ms 0}))
                  (roots/release! old pin {:sync? true})
                  (d/release conn)
                  (reset! released? true)
                  (let [reopened (d/connect cfg)
                        retained-conn (d/connect (assoc cfg :branch :retained))]
                    (try
                      (is (= expected (d/q '[:find ?e ?n :where [?e :n ?n]] @reopened)))
                      (doseq [i [:eavt :aevt :avet]]
                        (is (pos? (count (d/datoms @reopened i)))))
                      (is (= old-values (d/q '[:find ?e ?n :where [?e :n ?n]] @retained-conn)))
                      (finally (d/release reopened) (d/release retained-conn))))))
              (finally (when-not @released? (d/release conn)))))
          (finally (d/delete-database cfg)))))))
