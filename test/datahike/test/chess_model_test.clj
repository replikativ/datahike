(ns datahike.test.chess-model-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.test.model.rng :as rng])
  (:import [java.util UUID]))

(def X ["A" "B" "C" "D" "E" "F" "G" "H"])
(def Y (vec (range 1 9)))

(def xmap (into {} (map-indexed (fn [i s] [s i])) X))

(def empty-board (into {} (for [x X
                                y Y]
                            [[x y] nil])))

(defn put-piece [board x y piece]
  (let [id (:piece/id piece)]
    (-> empty-board

        ;; Reset any occurrence with this id.
        (into (filter (fn [[_ piece]] (not= id (:piece/id piece)))) board)

        ;; Then put the piece.
        (assoc [x y] piece))))

(defn threatens? [type xd yd]
  (let [same-line? (or (zero? xd) (zero? yd))
        same-diagonal? (or (= xd yd) (= (- xd) yd))
        squared-distance (+ (* xd xd) (* yd yd))]
    (case type
      :rook same-line?
      :knight (= 5 squared-distance)
      :bishop same-diagonal?
      :pawn (and (contains? {-1 1} xd)
                 (= 1 yd))
      :queen (or same-line? same-diagonal?)
      :king (contains? #{1 2} squared-distance))))

(defn board-datoms [board]
  (-> []
      (into (for [[[x0 y0] piece0] board
                  [[x1 y1] piece1] board
                  :when piece0
                  :when piece1
                  :when (not= [x0 y0] [x1 y1])
                  :when (threatens? (:piece/type piece0) (- (xmap x1) (xmap x0)) (- y1 y0))]
              [(:piece/id piece0) :piece/threatens (:piece/id piece1)]))
      (into (mapcat (fn [[[x y] {:keys [piece/type piece/id]}]]
                      (when type
                        [[id :piece/type type]
                         [id :piece/x x]
                         [id :piece/y y]]))) board)))

(def entity-id-start 10000)

(defn step-board-1 [board rng]
  (put-piece board
             (rng/rand-nth-rng rng X)
             (rng/rand-nth-rng rng Y)
             (rng/random-branch rng
               nil
               (let [all-ids (vec (into #{} (keep (comp :piece/id val)) board))
                     default-next-id (inc (apply max entity-id-start all-ids))]
                 {:piece/type (rng/rand-nth-rng rng [:rook :knight :bishop :pawn :queen :king])
                  :piece/id (if (seq all-ids)
                              (rng/random-branch rng
                                default-next-id
                                (rng/rand-nth-rng rng all-ids))
                              default-next-id)}))))

(defn step-board-n [board rng n]
  (if (<= n 0)
    board
    (recur (step-board-1 board rng) rng (dec n))))

(def chess-schema
  [{:db/ident :piece/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :piece/x
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :piece/y
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :piece/threatens
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}])

(def user-attrs (into #{} (map :db/ident) chess-schema))

(defn create-chess-db []
  (let [cfg {:store {:backend :memory :id (UUID/randomUUID)}
             :keep-history? true
             :schema-flexibility :write}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn chess-schema)
      conn)))

(defn tx-data [old-datoms new-datoms]
  (let [old-set (set old-datoms)
        new-set (set new-datoms)]
    (for [[op to-add to-remove] [[:db/add new-set old-set]
                                 [:db/retract old-set new-set]]
          [e a v :as datom] to-add
          :when (not (to-remove datom))]
      [op e a v])))

(defn db-datoms
  "Extract user datoms from database as [e a v] triples."
  [db]
  (into []
        (keep (fn [[e a v]] (when (user-attrs a) [e a v])))
        (d/datoms db :eavt)))

(defn check-history [history db]
  (doseq [[tx-id expected-datoms] (take-last 3 history)]
    (let [historic-db (d/as-of db tx-id)
          from-db (set (db-datoms historic-db))
          from-model (set expected-datoms)]
      (is (= from-model from-db)
          (str "History mismatch at tx " tx-id)))))

(deftest test-chess-board-sync
  (testing "database stays in sync with board model through random mutations"
    (let [rng (rng/create 42)
          conn (create-chess-db)
          iterations 20]
      (loop [i 0
             board empty-board
             prev-datoms []
             history []]
        (if (< i iterations)
          (let [board' (step-board-n board rng 4)
                new-datoms (board-datoms board')
                tx (tx-data prev-datoms new-datoms)
                tx-id (when (seq tx)
                        (:max-tx (:db-after (d/transact conn {:tx-data tx}))))
                db (d/db conn)
                from-db (set (db-datoms db))
                from-board (set new-datoms)]
            (is (= from-board from-db)
                (str "Mismatch at iteration " i))
            (let [history' (if tx-id
                             (conj history [tx-id new-datoms])
                             history)]
              (check-history history' db)
              (recur (inc i) board' new-datoms history')))
          history)))))
