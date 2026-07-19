(ns datahike.test.ivm-async-test
  "IVM subscriptions on cljs: sync-mode maintenance (runs inside the
   listener) and async-mode maintenance (:sync? false — every internal
   query flows through the partial-cps engine, serialized by the
   subscription's go-loop). Memory store, so async mode runs warm."
  (:require [cljs.test :refer [is testing] :include-macros true]
            [clojure.core.async :as a :refer [<!]]
            [datahike.api :as d]
            [datahike.ivm :as ivm]
            [datahike.test.async :refer-macros [deftest-async]]))

(def ^:private query
  '[:find ?n :where [?e :user/active? true] [?e :user/name ?n]])

(defn- fresh-conn-ch []
  (a/go
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read :keep-history? true}]
      (<! (d/create-database cfg))
      (<! (d/connect cfg)))))

(defn- wait-until
  "Poll pred every 20ms up to ~3s; yields the final pred value."
  [pred]
  (a/go-loop [tries 0]
    (cond
      (pred) true
      (> tries 150) (pred)
      :else (do (<! (a/timeout 20)) (recur (inc tries))))))

(deftest-async ivm-sync-mode
  (let [conn (<! (fresh-conn-ch))
        events (atom [])
        sub (ivm/subscribe! conn {:query query
                                  :callback (fn [m] (swap! events conj (dissoc m :db-after)))})]
    (is (= :delta (ivm/strategy sub)))
    (is (:reset (first @events)))
    (<! (d/transact! conn {:tx-data [{:db/id 1 :user/name "Ada" :user/active? true}
                                     {:db/id 2 :user/name "Grace" :user/active? true}]}))
    (is (= #{["Ada"] ["Grace"]} (ivm/result sub)))
    (is (= (d/q query @conn) (ivm/result sub)))
    (testing "duplicate name absorbs without emission"
      (let [n (count @events)]
        (<! (d/transact! conn {:tx-data [{:db/id 3 :user/name "Ada" :user/active? true}]}))
        (is (= n (count @events)))
        (is (= (d/q query @conn) (ivm/result sub)))))
    (testing "card-one upsert (no retraction in tx-data) is recovered by probing"
      (<! (d/transact! conn {:tx-data [[:db/add 2 :user/name "Barbara"]]}))
      (is (= (d/q query @conn) (ivm/result sub)))
      (is (= #{["Ada"] ["Barbara"]} (ivm/result sub))))
    (ivm/unsubscribe! sub)))

(deftest-async ivm-async-mode
  (let [conn (<! (fresh-conn-ch))
        events (atom [])
        sub (ivm/subscribe! conn {:query query
                                  :sync? false
                                  :callback (fn [m] (swap! events conj (dissoc m :db-after)))})]
    (is (= :delta (ivm/strategy sub)))
    (is (true? (<! (wait-until #(some :reset @events)))) "initial snapshot delivered")
    (<! (d/transact! conn {:tx-data [{:db/id 1 :user/name "Ada" :user/active? true}
                                     {:db/id 2 :user/name "Grace" :user/active? true}]}))
    (is (true? (<! (wait-until #(= 2 (count @events))))) "first change delivered")
    (is (= #{["Ada"] ["Grace"]} (ivm/result sub)))
    (is (= (d/q query @conn) (ivm/result sub)))
    (<! (d/transact! conn {:tx-data [[:db/retract 1 :user/active? true]
                                     [:db/add 2 :user/name "Barbara"]]}))
    (is (true? (<! (wait-until #(= 3 (count @events))))) "second change delivered")
    (is (= (d/q query @conn) (ivm/result sub)))
    (is (= #{["Barbara"]} (ivm/result sub)))
    (testing "change stream replays to the same set"
      (let [replayed (reduce (fn [s {:keys [reset result changes]}]
                               (if reset
                                 result
                                 (reduce (fn [s [t w]] (if (pos? w) (conj s t) (disj s t)))
                                         s changes)))
                             #{} @events)]
        (is (= (ivm/result sub) replayed))))
    (ivm/unsubscribe! sub)))
