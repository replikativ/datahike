(ns datahike.test.query-resource-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.query.cancel :as cancel]
            [datahike.query.resource :as resource]))

(defn- failure-data [f]
  (try (f) nil
       (catch #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo) e
         (ex-data e))))

(deftest optional-limit-validation-and-inheritance
  (is (nil? (resource/normalize-limit nil)))
  (doseq [n [1 12 resource/max-slots]]
    (is (= n (resource/normalize-limit n))))
  (doseq [n [0 -1 0.5 "12" :unbounded true ##NaN ##Inf (inc resource/max-slots)]]
    (is (= :query/invalid-resource-limit
           (:error (failure-data #(resource/normalize-limit n))))))
  (is (nil? (resource/effective-limit nil)))
  (binding [resource/*max-intermediate-slots* 12]
    (is (= 12 (resource/effective-limit nil)))
    (is (= 12 (resource/effective-limit 20)))
    (is (= 4 (resource/effective-limit 4)))
    (binding [resource/*max-intermediate-slots* (resource/effective-limit 4)]
      (is (= 4 (resource/effective-limit 12)))
      (is (= 4 (resource/effective-limit nil))))))

(deftest rejection-precedes-allocation-and-leaves-account-usable
  (binding [resource/*max-intermediate-slots* 8]
    (let [account (resource/account :output)
          allocations (atom [])
          append! (fn [width]
                    (resource/reserve! account (resource/tuple-slots width))
                    (swap! allocations conj (vec (repeat width :value))))]
      (append! 2)
      (append! 3)
      (is (= [[:value :value] [:value :value :value]] @allocations))
      (is (= {:error :query/intermediate-limit
              :resource :intermediate-slots :operator :output
              :limit 8 :retained 7 :requested 2}
             (failure-data #(append! 1))))
      (is (= 2 (count @allocations)) "the rejected tuple was never constructed")
      (append! 0)
      (is (= 3 (count @allocations)) "rejection did not consume capacity")
      (is (= :query/intermediate-limit
             (:error (failure-data #(append! 0))))))))

(deftest width-and-fixed-capacity-checks
  (binding [resource/*max-intermediate-slots* 12]
    (is (nil? (resource/check-size! :buffer (resource/rows-slots 4 2))))
    (is (= :query/intermediate-limit
           (:error (failure-data #(resource/check-size! :buffer (resource/rows-slots 4 3))))))
    (let [allocated? (atom false)]
      (is (= :buffer
             (:operator
              (failure-data #(do (resource/check-size! :buffer 13)
                                 (reset! allocated? true))))))
      (is (false? @allocated?)))))

(deftest local-accounts-measure-retention-rather-than-work
  (binding [resource/*max-intermediate-slots* 4]
    (testing "many independently bounded structures do not consume a query quota"
      (dotimes [_ 1000]
        (let [a (resource/account :temporary)]
          (resource/reserve! a 4))))
    (testing "a deduplicating sink only charges new retained entries"
      (let [a (resource/account :distinct)
            retained (atom #{})
            insert! (fn [v]
                      (when-not (contains? @retained v)
                        (resource/reserve! a (resource/tuple-slots 1))
                        (swap! retained conj v)))]
        (dotimes [_ 1000] (insert! :same))
        (insert! :other)
        (is (= #{:same :other} @retained))
        (is (= :query/intermediate-limit
               (:error (failure-data #(insert! :third)))))
        (is (= #{:same :other} @retained))))
    (testing "all buckets in one index use the same account"
      (let [a (resource/account :index)
            buckets (atom {})
            add! (fn [k v]
                   (let [new-key? (not (contains? @buckets k))]
                     (resource/reserve! a (if new-key? 3 1))
                     (swap! buckets update k (fnil conj []) v)))]
        (add! :a 1)
        (add! :a 2)
        (is (= :query/intermediate-limit
               (:error (failure-data #(add! :b 3)))))
        (is (= {:a [1 2]} @buckets))))))

(deftest checked-size-arithmetic
  (is (= resource/max-slots (resource/checked-add (dec resource/max-slots) 1)))
  (is (= resource/max-slots (resource/checked-multiply resource/max-slots 1)))
  (is (zero? (resource/checked-multiply resource/max-slots 0)))
  (is (= 12 (resource/rows-slots 4 2)))
  (doseq [f [#(resource/checked-add resource/max-slots 1)
             #(resource/checked-multiply resource/max-slots 2)
             #(resource/tuple-slots resource/max-slots)]]
    (is (= :query/intermediate-limit (:error (failure-data f)))))
  (doseq [n [-1 0.5 ##NaN ##Inf (inc resource/max-slots)]]
    (is (= :query/invalid-resource-size
           (:error (failure-data #(resource/tuple-slots n)))))))

(deftest disabled-policy-and-cancellation-precedence
  (is (nil? (resource/account :output)))
  (is (nil? (resource/reserve! nil resource/max-slots)))
  (is (nil? (resource/check-size! :output resource/max-slots)))
  (binding [resource/*max-intermediate-slots* 1
            cancel/*cancel* (volatile! true)]
    (is (= {:datahike/canceled true}
           (failure-data #(resource/reserve! (resource/account :output) 2))))
    (is (= {:datahike/canceled true}
           (failure-data #(resource/check-size! :output 2)))))
  (let [timeout {:type :datahike/query-timeout :timeout 10}]
    (binding [resource/*max-intermediate-slots* 1
              cancel/*cancel* (volatile! timeout)]
      (is (= timeout (failure-data #(resource/check-size! :output 2)))))))

(deftest resource-errors-survive-wrapped-causes
  (let [failure (ex-info "resource failure" {:error :query/intermediate-limit})
        wrapped (ex-info "predicate failed" {:error :query/predicate} failure)
        twice-wrapped (ex-info "execution failed" {} wrapped)]
    (is (true? (resource/limit-exceeded? failure)))
    (is (true? (resource/limit-exceeded? wrapped)))
    (is (true? (resource/limit-exceeded? twice-wrapped))))
  (doseq [e [nil
             (ex-info "Query intermediate exceeds its structural size limit" {})
             (ex-info "not our marker" {:error :query/limit})
             (ex-info "query timeout" {:type :datahike/query-timeout})
             (ex-info "ordinary wrapper" {} (ex-info "ordinary cause" {}))]]
    (is (false? (resource/limit-exceeded? e)))))

#?(:clj
   (deftest bigint-inputs-obey-the-same-portable-size-bound
     (is (= 12N (resource/normalize-limit 12N)))
     (is (= resource/max-slots
            (resource/checked-add 9007199254740990N 1N)))
     (is (= 12N (resource/rows-slots 4N 2N)))
     (is (= :query/invalid-resource-limit
            (:error (failure-data #(resource/normalize-limit 9007199254740992N)))))
     (is (= :query/intermediate-limit
            (:error (failure-data #(resource/checked-multiply 9007199254740991N 2N)))))
     (binding [resource/*max-intermediate-slots* 4N]
       (let [a (resource/account :output)]
         (resource/reserve! a 4N)
         (is (= :query/intermediate-limit
                (:error (failure-data #(resource/reserve! a 1N)))))))))
