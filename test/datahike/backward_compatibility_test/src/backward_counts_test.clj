(ns backward-counts-test
  "A store written by the latest RELEASE, across reconnects, read by the current
   code: the release stored branches without their element counts (#1108), and
   `datahike.index.recount/recount!` must find and repair exactly that.

   `write` runs under the released Datahike and uses nothing newer than its API;
   `verify-current` runs under the current code."
  (:require [datahike.api :as d]))

(def cfg {:store {:backend :file
                  :path (str (or (System/getenv "BACK_COMPAT_ROOT")
                                 (System/getProperty "java.io.tmpdir"))
                             "/datahike-backward-counts")
                  :id #uuid "550e8400-e29b-41d4-a716-446655441108"}
          :keep-history? true
          :schema-flexibility :write
          ;; small nodes, so that a few thousand datoms make trees three levels
          ;; deep and every index, AVET included, has branches below its root
          :index-config {:branching-factor 16}
          :initial-tx [{:db/ident :k :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                       {:db/ident :v :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one :db/index true}]})

(def batches 12)
(def batch-size 300)

(defn write [_]
  (d/delete-database cfg)
  (d/create-database cfg)
  ;; A fresh connection per batch: the branches a write splits or borrows from are
  ;; then on disk, which is when the release stored them without a count.
  (dotimes [i batches]
    (let [conn (d/connect cfg)]
      (d/transact conn (vec (for [j (range batch-size)]
                              {:k (+ (* i batch-size) j) :v (str "x" (mod j 50))})))
      (when (pos? i)
        (d/transact conn (vec (for [j (range 0 batch-size 7)]
                                [:db/retract [:k (+ (* (dec i) batch-size) j)] :v (str "x" (mod j 50))]))))
      (d/release conn)))
  (println "backward-counts-test: wrote" (* batches batch-size) "entities"))

(defn- total [f census]
  (reduce + (map f (vals (:indices census)))))

(defn verify-current [_]
  (let [census (requiring-resolve 'datahike.index.recount/census)
        recount! (requiring-resolve 'datahike.index.recount/recount!)
        q '[:find (count ?e) . :where [?e :k ?k] [(< ?k 1000)] [?e :v "x7"]]
        conn (d/connect cfg)
        before (census conn)
        datoms (vec (d/datoms @conn :eavt))
        history (count (d/datoms (d/history @conn) :eavt))
        answer (d/q q @conn)
        max-tx (:max-tx @conn)]
    (println "backward-counts-test: released store" before)
    (assert (every? pos? (map :branches (vals (:indices before))))
            "precondition: every index, AVET and the history ones included, has branches")
    ;; A release from before every branch kept its count writes no mark and leaves
    ;; branches uncounted; a later one writes the mark and none. Either way the
    ;; two must agree, and after a recount the store must be complete.
    (if (:complete? before)
      (assert (zero? (total :unknown before)) "a marked store has every count")
      (println "backward-counts-test: unmarked store with" (total :unknown before) "uncounted branches"))
    (assert (= before (recount! conn)))
    (assert (= max-tx (:max-tx @conn)))
    (d/transact conn [{:k 999999 :v "after"}])
    (d/release conn)
    (let [conn (d/connect cfg)
          after (census conn)]
      (println "backward-counts-test: after recount" after)
      (assert (:complete? after))
      (assert (zero? (total :unknown after)) "every branch is stored with its count")
      (assert (every? (set (d/datoms @conn :eavt)) datoms) "every datom is still there")
      (assert (<= history (count (d/datoms (d/history @conn) :eavt))))
      (assert (= answer (d/q q @conn)))
      (assert (= (count (d/datoms @conn :eavt)) (count @conn)))
      (d/release conn))
    (println "backward-counts-test: ok")))
