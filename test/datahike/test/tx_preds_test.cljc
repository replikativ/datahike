(ns datahike.test.tx-preds-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing]])
            [datahike.tx-preds :as tx-preds]))

(defn- report [store-id]
  {:db-after {:config {:store {:id store-id}}}})

(deftest ensure-predicate-does-not-replace-another-owner
  (let [id ::ensure-owner
        f identity]
    (try
      (is (= :installed (tx-preds/ensure-tx-pred! id :guard f)))
      (is (= :present (tx-preds/ensure-tx-pred! id :guard f)))
      (is (= :tx-pred/id-collision
             (try (tx-preds/ensure-tx-pred! id :guard (fn [x] x))
                  nil
                  (catch #?(:clj Exception :cljs js/Error) e
                    (:type (ex-data e))))))
      (is (identical? f (get (tx-preds/tx-preds-for id) :guard)))
      (finally (tx-preds/unregister-tx-pred! id :guard)))))

#?(:clj
   (deftest concurrent-ensure-installs-once
     (let [id ::concurrent-ensure
           f identity
           start (promise)]
       (try
         (let [attempts (vec (repeatedly 16
                                         #(future @start
                                                  (tx-preds/ensure-tx-pred! id :guard f))))]
           (deliver start true)
           (is (= {:installed 1 :present 15}
                  (frequencies (mapv deref attempts)))))
         (finally (tx-preds/unregister-tx-pred! id :guard))))))

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
