(ns datahike.kabel.cleanup-async-test
  (:require [cljs.test :refer-macros [deftest is async]]
            [clojure.core.async :as a :refer [<!] :refer-macros [go]]
            [datahike.kabel.cleanup :as cleanup]
            [datahike.store-registry :as registry]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]))

(defn- ready [value]
  (let [ch (a/promise-chan)]
    (if (some? value) (a/put! ch value) (a/close! ch))
    ch))

(deftest lease-retirement-awaits-the-native-drain
  (async done
         (let [id (random-uuid) object (js-obj) drain (a/promise-chan) owner (cleanup/owner)]
           (swap! (:state owner) assoc :registration (registry/acquire! id object) :drain! (constantly drain))
           (let [result (cleanup/cleanup! owner)]
             (is (identical? result (cleanup/cleanup! owner)))
             (is (nil? (a/poll! result)))
             (is (identical? object (nodes/registered-storage id)))
             (a/put! drain true)
             (go
               (is (true? (<! result)))
               (is (nil? (nodes/registered-storage id)))
               (is (identical? result (cleanup/cleanup! owner)))
               (done))))))

(deftest invalid-drains-retain-leases-and-can-be-retried
  (async done
         (go
           (doseq [drain! [(constantly nil) (constantly false) #(ready nil) #(ready false)]]
             (let [id (random-uuid) object (js-obj) owner (cleanup/owner)]
               (swap! (:state owner) assoc :registration (registry/acquire! id object) :drain! drain!)
               (is (= :datahike.kabel/unconfirmed-drain
                      (:type (ex-data (<! (cleanup/cleanup! owner))))))
               (is (identical? object (nodes/registered-storage id)))
               (swap! (:state owner) assoc :drain! #(ready true))
               (is (true? (<! (cleanup/cleanup! owner))))
               (is (nil? (nodes/registered-storage id)))))
           (done))))

(deftest synchronous-callback-errors-do-not-strand-the-shared-promise
  (async done
         (go
           (doseq [callback [:retire-initial! :drain!]]
             (let [owner (cleanup/owner) attempts (atom 0) error (ex-info "Sync callback error" {})]
               (swap! (:state owner) assoc callback
                      #(if (= 1 (swap! attempts inc)) (throw error) (ready true)))
               (is (identical? error (<! (cleanup/cleanup! owner))))
               (is (true? (<! (cleanup/cleanup! owner))))
               (is (= 2 @attempts))))
           (done))))

(deftest a-strategy-without-its-captured-drain-cannot-claim-empty-cleanup
  (async done
         (go
           (let [id (random-uuid) object (js-obj) owner (cleanup/owner)]
             (swap! (:state owner) assoc :registration (registry/acquire! id object) :strategy :captured)
             (is (= :datahike.kabel/unconfirmed-drain
                    (:type (ex-data (<! (cleanup/cleanup! owner))))))
             (is (identical? object (nodes/registered-storage id)))
             (swap! (:state owner) assoc :drain! #(ready true))
             (is (true? (<! (cleanup/cleanup! owner))))
             (is (nil? (nodes/registered-storage id))))
           (done))))
