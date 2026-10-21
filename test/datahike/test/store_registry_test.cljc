(ns datahike.test.store-registry-test
  (:require [datahike.store-registry :as registry]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])))

(defn- fresh-store [] #?(:clj (Object.) :cljs (js-obj)))

(deftest last-owner-removes-registration-and-lease-is-idempotent
  (let [baseline @nodes/storage-registry]
    (dotimes [_ 100]
      (let [id (random-uuid) store (fresh-store) lease (registry/acquire! id store)]
        (is (identical? store (nodes/registered-storage id)))
        (registry/release! lease)
        (registry/release! lease)
        (is (nil? (nodes/registered-storage id)))))
    (is (= baseline @nodes/storage-registry))))

(deftest sibling-release-order-never-restores-a-released-store
  (doseq [newest-first? [false true]]
    (let [id (random-uuid) older (fresh-store) newer (fresh-store)
          a (registry/acquire! id older) b (registry/acquire! id newer)]
      (registry/release! (if newest-first? b a))
      (is (identical? (if newest-first? older newer) (nodes/registered-storage id)))
      (registry/release! (if newest-first? a b))
      (is (nil? (nodes/registered-storage id))))))

(deftest explicit-registrations-survive-stale-leases-even-for-the-same-object
  (doseq [same-object? [false true]]
    (let [id (random-uuid) original (fresh-store)
          replacement (if same-object? original (fresh-store))
          lease (registry/acquire! id original)]
      (registry/register! id replacement)
      (registry/release! lease)
      (is (identical? replacement (nodes/registered-storage id)))
      (registry/unregister! id)
      (is (nil? (nodes/registered-storage id))))))

(deftest prior-explicit-registration-and-its-removal-are-respected
  (doseq [remove? [false true]]
    (let [id (random-uuid) prior (fresh-store) current (fresh-store)]
      (registry/register! id prior)
      (let [lease (registry/acquire! id current)]
        (when remove? (registry/unregister! id))
        (is (identical? current (nodes/registered-storage id)))
        (registry/release! lease)
        (is (identical? (when-not remove? prior) (nodes/registered-storage id))))
      (registry/unregister! id))))

(deftest a-different-direct-pss-registration-is-not-erased
  (let [id (random-uuid) lease (registry/acquire! id (fresh-store)) replacement (fresh-store)]
    (nodes/register-storage! id replacement)
    (registry/release! lease)
    (is (identical? replacement (nodes/registered-storage id)))
    (nodes/unregister-storage! id)))

(deftest store-rewrapping-retains-the-same-lease-and-chronological-owner
  (let [id (random-uuid) a (registry/acquire! id (fresh-store))
        newer (fresh-store) b (registry/acquire! id newer) wrapped (fresh-store)]
    (registry/replace! a wrapped)
    (is (identical? newer (nodes/registered-storage id)))
    (registry/release! b)
    (is (identical? wrapped (nodes/registered-storage id)))
    (registry/release! a)
    (is (nil? (nodes/registered-storage id)))
    (is (try (registry/replace! a wrapped) false
             (catch #?(:clj Exception :cljs js/Error) _ true)))))

#?(:clj
   (deftest concurrent-lease-operations-retain-a-live-registration
     (let [id (random-uuid) anchor (registry/acquire! id (fresh-store))
           workers (mapv (fn [_]
                           (future
                             (dotimes [_ 20]
                               (let [lease (registry/acquire! id (fresh-store))]
                                 (try
                                   (registry/replace! lease (fresh-store))
                                   (is (some? (nodes/registered-storage id)))
                                   (finally (registry/release! lease)))))))
                         (range 8))]
       (doseq [worker workers] (is (not= ::timed-out (deref worker 10000 ::timed-out))))
       (is (some? (nodes/registered-storage id)))
       (registry/release! anchor)
       (is (nil? (nodes/registered-storage id))))))
