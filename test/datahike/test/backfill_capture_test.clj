(ns datahike.test.backfill-capture-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.backfill.capture :as capture]
            [datahike.backfill.journal :as journal]))

(defn- build-db []
  {:schema {:a {:db.secondary/status :building :db.secondary/building-since-tx 1}
            :b {:db.secondary/status :building :db.secondary/building-since-tx 1}}
   :config {:writer {:backfill-journal {:max-frame-bytes 64 :max-bytes 1024}}}})

(defn- stage [owner before pending]
  (capture/prepare-report! owner
                           {:db-before before
                            :db-after (assoc before :secondary-index-build-deltas pending)}))

(deftest capture-uses-only-the-facade-for-opaque-descriptors
  (let [owner (atom {})
        prefixes (atom {})
        disposed (atom [])
        new-prefix (fn [id rows]
                     (let [handle (Object.)]
                       (swap! prefixes assoc handle {:id id :rows rows})
                       handle))
        descriptor-id (fn [handle] (:id (get @prefixes handle)))
        readable (fn [handle]
                   (let [prefix (get @prefixes handle)]
                     (when (or (nil? prefix) (some #{(:id prefix)} @disposed))
                       (throw (ex-info "Unknown or disposed fake journal" {})))
                     prefix))]
    (with-redefs [journal/create! (fn [_] (new-prefix (Object.) []))
                  journal/descriptor-id descriptor-id
                  journal/append! (fn [handle notifications]
                                    (let [{:keys [id rows]} (readable handle)]
                                      (new-prefix id (into rows notifications))))
                  journal/reduce-journal (fn [handle f init]
                                           (reduce f init (:rows (readable handle))))
                  journal/dispose! (fn [handle]
                                     (let [id (descriptor-id handle)]
                                       (when-not (some #{id} @disposed)
                                         (swap! disposed conj id)))
                                     nil)]
      (try
        (let [before (:db-after (stage owner (build-db) (array-map :a [:a0] :b [:b0])))
              a (get-in before [:secondary-index-build-journals :a])
              b (get-in before [:secondary-index-build-journals :b])
              after (:db-after (stage owner before {:a [:a1]}))
              next-a (get-in after [:secondary-index-build-journals :a])]
          (is (= Object (class a)))
          (is (not (identical? a next-a)))
          (is (identical? (descriptor-id a) (descriptor-id next-a)))
          (is (= #{(descriptor-id a) (descriptor-id b)} (set (keys @owner))))
          (is (= [:a0] (capture/reduce-deltas before :a conj [])))
          (is (= [:a0 :a1 :pending]
                 (capture/reduce-deltas
                  (assoc after :secondary-index-build-deltas {:a [:pending]}) :a conj [])))
          (let [retired (capture/prepare-report!
                         owner {:db-before after
                                :db-after (assoc-in after [:schema :a :db.secondary/status] :ready)})]
            (is (empty? @disposed))
            (is (= [:a0 :a1] (capture/reduce-deltas after :a conj [])))
            (capture/finish! owner retired)
            (is (= [(descriptor-id a)] @disposed))
            (is (= #{(descriptor-id b)} (set (keys @owner))))
            (is (= [:b0] (capture/reduce-deltas (:db-after retired) :b conj []))))
          (capture/dispose-owned! owner)
          (is (= [(descriptor-id a) (descriptor-id b)] @disposed))
          (is (empty? @owner))
          (is (= :writer-shut-down
                 (try (stage owner (build-db) {:a [:too-late]})
                      nil (catch Exception e (:type (ex-data e)))))))
        (finally (capture/dispose-owned! owner))))))

(deftest failed-multi-index-capture-preserves-all-accepted-prefixes
  (let [owner (atom {})]
    (try
      (let [before (:db-after (stage owner (build-db) (array-map :a [:a0] :b [:b0])))
            failure (try (stage owner before
                                (array-map :a [:discarded] :b [(apply str (repeat 100 "x"))]))
                         nil (catch Exception e (:type (ex-data e))))]
        (is (= :backfill.journal/frame-too-large failure))
        (is (= [:a0] (capture/reduce-deltas before :a conj [])))
        (is (= [:b0] (capture/reduce-deltas before :b conj [])))
        (let [after (:db-after (stage owner before (array-map :a [:a1] :b [:b1])))]
          (is (nil? (:secondary-index-build-deltas after)))
          (is (= [:a0 :a1] (capture/reduce-deltas after :a conj [])))
          (is (= [:b0 :b1] (capture/reduce-deltas after :b conj [])))
          (is (= [:a0] (capture/reduce-deltas before :a conj [])))))
      (finally (capture/dispose-owned! owner)))))

(deftest replay-stops-before-pure-pending-tail
  (let [owner (atom {})]
    (try
      (let [before (:db-after (stage owner (build-db) {:a [:captured]}))
            pure (assoc before :secondary-index-build-deltas {:a [:pending]})
            visited (atom [])]
        (is (= :stopped
               (capture/reduce-deltas pure :a
                                      (fn [_ x] (swap! visited conj x) (reduced :stopped)) nil)))
        (is (= [:captured] @visited)))
      (finally (capture/dispose-owned! owner)))))

(deftest retirement-waits-for-publication-and-owner-cannot-reopen
  (let [owner (atom {})]
    (try
      (let [before (:db-after (stage owner (build-db) {:a [:captured]}))
            path (get-in before [:secondary-index-build-journals :a :path])
            report (capture/prepare-report!
                    owner {:db-before before
                           :db-after (assoc-in before [:schema :a :db.secondary/status] :ready)})]
        (is (nil? (:secondary-index-build-journals (:db-after report))))
        (is (.exists (java.io.File. path)))
        (capture/finish! owner report)
        (is (not (.exists (java.io.File. path))))
        (is (empty? @owner)))
      (finally (capture/dispose-owned! owner)))
    (is (= :writer-shut-down
           (try (stage owner (build-db) {:a [:too-late]})
                nil (catch Exception e (:type (ex-data e))))))))
