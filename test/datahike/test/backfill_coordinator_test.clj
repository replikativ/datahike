(ns datahike.test.backfill-coordinator-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.backfill.admission :as admission]
            [datahike.backfill.avet :as avet]
            [datahike.backfill.control :as control]
            [datahike.backfill.coordinator :as coordinator]
            [datahike.backfill.job :as job]
            [datahike.backfill.runtime :as runtime]
            [datahike.backfill.sort :as sort]
            [datahike.gc-guard :as guard]
            [datahike.gc-roots :as roots]
            [datahike.store :as ds])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit CountDownLatch]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- request! [^LinkedBlockingQueue requests]
  (or (.poll requests 5 TimeUnit/SECONDS)
      (throw (ex-info "Worker dispatch timed out" {}))))

(defn- fixture [options build f]
  (let [id (random-uuid)
        cursor {:generation id :journal-id (random-uuid) :offset 10 :sequence 1}
        source {:avet-build {:id id :patch {:value {:db/index true}}}}
        lease (atom {:cursor cursor :source-db source})
        requests (LinkedBlockingQueue.)
        events (atom [])
        owner (coordinator/create! {:options {:max-contexts 8}}
                                   (merge {:submit! #(.add requests %) :tail-bytes 100 :tail-transactions 2} options))]
    (with-redefs [runtime/acquire-prefix! (fn [_ generation]
                                            (swap! events conj [:acquire generation]) @lease)
                  runtime/release-prefix! (fn [& _] (swap! events conj :lease-release))
                  job/supported! (fn [_]) job/matches? (fn [& _] true)
                  ds/canonical-store-id (fn [& _] :store)
                  guard/writing! (fn [_] (swap! events conj :guard) :guard)
                  guard/done! (fn [& _] (swap! events conj :guard-release))
                  roots/pin! (fn [& _] (swap! events conj :pin) :pin)
                  roots/renew! (fn [& _] (swap! events conj :renew))
                  roots/assert-live! (fn [& _] (swap! events conj :assert-pin))
                  roots/release! (fn [& _] (swap! events conj :pin-release))
                  avet/build! (fn [& _] (swap! events conj :build) (build))
                  avet/close! (fn [_] (swap! events conj :close))
                  admission/mint! (fn [s candidate _ c] {:source s :candidate candidate :cursor c})
                  admission/flush! (fn [c] (swap! events conj :flush) c)
                  admission/cursor :cursor admission/candidate :candidate
                  admission/rebind-source (fn [c s] (assoc c :source s))
                  admission/advance-range (fn [c s end _]
                                            (swap! events conj :advance)
                                            (assoc c :source s :cursor end))]
      (try
        (f {:owner owner :id id :source source :cursor cursor :lease lease
            :requests requests :events events})
        (finally (coordinator/close! owner))))))

(defn- begin! [{:keys [owner id source cursor]}]
  (coordinator/after-commit! owner source {:generation id :source-db source :cursor cursor}))

(deftest candidate-owned-through-successful-commit
  (fixture {} (constantly {:current :tree})
           (fn [{:keys [owner source requests events] :as context}]
             (begin! context)
             (let [{:keys [token op]} (request! requests)]
               (is (= :install op))
               (is (= :ready (:status (coordinator/try-install! owner source token))))
               (is (= :activated (coordinator/with-install! owner token (constantly :activated))))
               (coordinator/accepted! owner token)
               (is (nil? (coordinator/check-install! owner token)))
               (is (not-any? #{:close :pin-release :guard-release} @events))
               (coordinator/after-commit! owner {} nil)
               (coordinator/close! owner)
               (is (= 1 (count (filter #{:close} @events))))
               (is (= [:close :pin-release :guard-release] (take-last 3 @events)))
               (is (empty? (:jobs @(:state owner))))))))

(deftest rejected-admission-reoffers-with-fresh-token
  (fixture {} (constantly {:current :tree})
           (fn [{:keys [owner source requests] :as context}]
             (begin! context)
             (let [first-token (:token (request! requests))]
               (is (= :ready (:status (coordinator/try-install! owner source first-token))))
               (coordinator/abort-install! owner first-token false)
               (let [next-token (:token (request! requests))]
                 (is (not (identical? first-token next-token)))
                 (is (thrown? clojure.lang.ExceptionInfo
                              (coordinator/try-install! owner source first-token)))
                 (is (= :ready (:status (coordinator/try-install! owner source next-token))))
                 (coordinator/abort-install! owner next-token true))))))

(deftest oversized-final-tail-retries-outside-writer
  (fixture {} (constantly {:current :tree})
           (fn [{:keys [owner source requests events lease] :as context}]
             (begin! context)
             (let [token (:token (request! requests))]
               (swap! lease update :cursor #(assoc % :sequence 10 :offset 1000))
               (is (= :retry (:status (coordinator/try-install! owner source token))))
        ;; Only the worker may perform this over-budget advancement. The next
        ;; offered candidate has already passed the full leased-range replay.
               (let [next-token (:token (request! requests))]
                 (is (= 1 (count (filter #{:advance} @events))))
                 (is (= :ready (:status (coordinator/try-install! owner source next-token))))
                 (coordinator/abort-install! owner next-token true))))))

(deftest cancellation-and-capacity-are-background-job-outcomes
  (let [entered (CountDownLatch. 1) resume (CountDownLatch. 1)]
    (fixture {:max-jobs 1}
             (fn [] (.countDown entered) (.await resume 5 TimeUnit/SECONDS)
               (control/check!) {:current :tree})
             (fn [{:keys [owner source requests events] :as context}]
               (try
                 (begin! context)
                 (is (.await entered 5 TimeUnit/SECONDS))
                 (let [replacement (random-uuid)
                       next-source (assoc-in source [:avet-build :id] replacement)]
                   (is (nil? (coordinator/after-commit!
                              owner next-source {:generation replacement :source-db next-source})))
                   (is (= {:op :cancel :generation replacement :reason :worker-capacity}
                          (request! requests)))
                   (is (= replacement (get-in @(:state owner) [:last-start-failure :generation]))))
                 (finally (.countDown resume)))
               (coordinator/close! owner)
               (is (not-any? #{:flush} @events))
               (is (= [:pin-release :guard-release] (take-last 2 @events)))))))

(deftest cancellation-interrupts-sort-and-cleans-owned-scratch
  (let [directory (Files/createTempDirectory "avet-cancel-sort-" (make-array FileAttribute 0))
        checks (atom 0)]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"cancelled"
           (binding [control/*check!* #(when (> (swap! checks inc) 12)
                                         (throw (ex-info "cancelled" {:type :cancelled})))]
             (sort/with-sorted-records! (map vector (range 100)) compare
               {:directory directory :window-records 2 :fan-in 2}
               doall))))
      (is (> @checks 12))
      (with-open [^java.util.stream.Stream files (Files/list directory)]
        (is (zero? (.count files))))
      (finally (Files/delete directory)))))

(deftest cancellation-between-admission-and-commit-prevents-publication
  (fixture {} (constantly {:current :tree})
           (fn [{:keys [owner source requests events] :as context}]
             (begin! context)
             (let [token (:token (request! requests))]
               (is (= :ready (:status (coordinator/try-install! owner source token))))
               (coordinator/accepted! owner token)
               (coordinator/invalidate! owner)
               (is (thrown? clojure.lang.ExceptionInfo (coordinator/check-install! owner token)))
               (is (not-any? #{:close :pin-release :guard-release} @events))
               (coordinator/abort-install! owner token true)
               (coordinator/close! owner)
               (is (= [:close :pin-release :guard-release] (take-last 3 @events)))))))
