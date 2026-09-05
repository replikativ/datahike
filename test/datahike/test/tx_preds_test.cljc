(ns datahike.test.tx-preds-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.tx-preds :as tx-preds]))

(defn- report [store-id]
  {:db-after {:config {:store {:id store-id}}}})

(deftest named-predicates-compose-with-the-legacy-slot
  (let [store-id ::composed
        calls (atom [])]
    (try
      (tx-preds/register-tx-pred! store-id (fn [_] (swap! calls conj :default)))
      (tx-preds/register-tx-pred! store-id :audit
                                  (fn [_] (swap! calls conj :audit)))
      (tx-preds/check-report (report store-id))
      (is (= #{:default :audit} (set @calls)))
      (is (= 2 (count (tx-preds/tx-preds-for store-id))))
      (is (ifn? (tx-preds/tx-pred-for store-id)))

      (reset! calls [])
      (tx-preds/unregister-tx-pred! store-id)
      (tx-preds/check-report (report store-id))
      (is (= [:audit] @calls))
      (is (nil? (tx-preds/tx-pred-for store-id)))
      (finally
        (tx-preds/unregister-tx-pred! store-id :audit)))))

(deftest named-unregister-is-store-local
  (let [left ::left
        right ::right]
    (try
      (tx-preds/register-tx-pred! left :guard identity)
      (tx-preds/register-tx-pred! right :guard identity)
      (tx-preds/unregister-tx-pred! left :guard)
      (is (empty? (tx-preds/tx-preds-for left)))
      (is (= #{:guard} (set (keys (tx-preds/tx-preds-for right)))))
      (finally
        (tx-preds/unregister-tx-pred! right :guard)))))

(deftest predicates-run-in-deterministic-name-order
  (let [store-id ::ordered
        calls (atom [])]
    (try
      (doseq [id [:z :a :m]]
        (tx-preds/register-tx-pred! store-id id
                                    (fn [_] (swap! calls conj id))))
      (tx-preds/check-report (report store-id))
      (is (= [:a :m :z] @calls))
      (finally
        (doseq [id [:z :a :m]]
          (tx-preds/unregister-tx-pred! store-id id))))))
