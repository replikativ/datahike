(ns datahike.test.backfill-runtime-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.backfill.runtime :as runtime]
            [datahike.backfill.journal :as journal])
  (:import [java.nio.file Files Path]))

(defn- error-type [f]
  (try (f) nil (catch Exception e (:type (ex-data e)))))

(defn- report [before after]
  {:db-before before :db-after after :tx-data []})

(defn- begin [owner before id]
  (runtime/prepare-report! owner
                           (report before (assoc before :avet-build {:id id :attrs #{:score}}
                                                 :max-tx (inc (:max-tx before 0))))
                           'begin-avet-build!))

(defn- tx [owner before effects]
  (runtime/prepare-report! owner
                           (report before (assoc before :avet-build-effects effects
                                                 :max-tx (inc (:max-tx before))))
                           'transact!))

(defn- path-exists? [lease]
  (Files/exists (Path/of (get-in lease [:descriptor :path]) (make-array String 0))
                (make-array java.nio.file.LinkOption 0)))

(defn- origin [lease]
  (assoc (:cursor lease) :position (journal/start-cursor (:descriptor lease)) :sequence 0))

(deftest definite-conflict-discards-speculation-without-closing-owner
  (let [owner (runtime/create! nil) id (random-uuid)]
    (try
      (let [a (begin owner {} id)
            _ (runtime/committed! owner (:db-after a) [a])
            lease (runtime/acquire-prefix! owner id)
            b (tx owner (:db-after a) [[:current :insert :rejected nil]])]
        (try
          (is (= #{id} (runtime/invalidate! owner)))
          (is (= :datahike.backfill.runtime/unavailable
                 (error-type #(runtime/acquire-prefix! owner id))))
          (is (= :owner-lost
                 (get-in (tx owner (:db-after a) []) [:db-after :avet-build-result :reason])))
          (is (= :datahike.backfill.runtime/invalid-commit
                 (error-type #(runtime/committed! owner (:db-after b) [b]))))
          (is (= [[]] (runtime/reduce-prefix owner lease (origin lease)
                                             (fn [acc transaction _]
                                               (conj acc (:effects transaction))) [])))
          ;; The durable cancellation belongs to the writer. Simulate its
          ;; accepted report, then a wholly new generation on the fresh head.
          (let [canceled (runtime/prepare-report!
                          owner (report (:db-after a) (dissoc (:db-after a) :avet-build))
                          'cancel-avet-build!)
                fresh-id (random-uuid)
                fresh (begin owner (:db-after canceled) fresh-id)]
            (runtime/committed! owner (:db-after canceled) [canceled])
            (is (= fresh-id (:generation (runtime/committed! owner (:db-after fresh) [fresh])))))
          (finally (runtime/release-prefix! owner lease)))
        (is (not (path-exists? lease))))
      (finally (runtime/close! owner)))))

(deftest authoritative-reload-retires-lost-lineage-without-rejecting-data
  (let [owner (runtime/create! nil) id (random-uuid)
        before {:meta {:datahike/commit-id (random-uuid)}
                :datahike.writing/head-revision (random-uuid) :eavt (Object.)}]
    (try
      (let [a (begin owner before id)
            source (:db-after a)]
        (runtime/committed! owner source [a])
        (is (identical? source (runtime/reconcile-db! owner source)))
        ;; Same CID/root but a different storage revision is a lost lineage,
        ;; not permission to keep replaying an old speculative stream.
        (let [rewritten (assoc source :datahike.writing/head-revision (random-uuid))
              reconciled (runtime/reconcile-db! owner rewritten)
              accepted (tx owner reconciled [[:current :insert :valid-user-value nil]])]
          (is (nil? (:avet-build-invalidated? rewritten)))
          (is (:avet-build-invalidated? reconciled))
          (is (nil? (get-in accepted [:db-after :avet-build])))
          (is (= :owner-lost (get-in accepted [:db-after :avet-build-result :reason])))
          (is (= (inc (:max-tx source)) (get-in accepted [:db-after :max-tx])))
          (is (= :datahike.backfill.runtime/unavailable
                 (error-type #(runtime/acquire-prefix! owner id))))))
      (finally (runtime/close! owner))))
  (let [owner (runtime/create! nil)
        recovered {:avet-build {:id (random-uuid) :status :building} :max-tx 4}]
    (try
      (let [reconciled (runtime/reconcile-db! owner recovered)
            accepted (tx owner reconciled [])]
        (is (nil? (get-in accepted [:db-after :avet-build])))
        (is (= 5 (get-in accepted [:db-after :max-tx])))
        (is (empty? (:contexts @(:state owner)))))
      (finally (runtime/close! owner)))))

(deftest speculative-and-committed-prefixes-are-distinct
  (let [owner (runtime/create! nil) id (random-uuid)]
    (try
      (let [a (begin owner {} id)
            b (tx owner (:db-after a) [[:current :insert :value nil]])]
        (is (= :datahike.backfill.runtime/unavailable
               (error-type #(runtime/acquire-prefix! owner id))))
        (let [start (runtime/committed! owner (:db-after a) [a])
              lease (runtime/acquire-prefix! owner id)]
          (try
            (is (= (:db-after a) (:source-db start)))
            (is (identical? (:db-after a) (:source-db lease)))
            (is (= 1 (get-in start [:cursor :sequence])))
            (is (= [[]] (runtime/reduce-prefix owner lease (origin lease)
                                               (fn [acc transaction _] (conj acc (:effects transaction))) [])))
            (is (pos? (runtime/range-byte-size owner lease (origin lease))))
            (is (zero? (runtime/range-byte-size owner lease (:cursor lease))))
            (is (nil? (runtime/committed! owner (:db-after b) [b])))
            (is (identical? (:db-after a) (:source-db lease)))
            ;; An existing read lease remains the exact older immutable prefix.
            (is (= 1 (runtime/reduce-prefix owner lease (origin lease) (fn [n _ _] (inc n)) 0)))
            (finally (runtime/release-prefix! owner lease))))
        (let [empty-tx (tx owner (:db-after b) [])]
          (runtime/committed! owner (:db-after empty-tx) [empty-tx])
          (let [lease (runtime/acquire-prefix! owner id)]
            (try
              (is (= [1 2 3] (runtime/reduce-prefix owner lease (origin lease)
                                                    (fn [acc transaction _] (conj acc (:max-tx transaction))) [])))
              (is (= 3 (get-in lease [:cursor :sequence])))
              (is (= :stopped (runtime/reduce-prefix owner lease (origin lease)
                                                     (fn [_ _ _] (reduced :stopped)) nil)))
              (finally (runtime/release-prefix! owner lease))))))
      (finally (runtime/close! owner)))))

(deftest failed-disposal-retains-quota-and-can-be-retried
  (let [owner (runtime/create! {:max-contexts 1}) id (random-uuid)]
    (try
      (let [a (begin owner {} id)
            _ (runtime/committed! owner (:db-after a) [a])
            lease (runtime/acquire-prefix! owner id)
            cancel (runtime/prepare-report! owner
                                            (report (:db-after a) (dissoc (:db-after a) :avet-build))
                                            'cancel-avet-build!)]
        (runtime/release-prefix! owner lease)
        (with-redefs [journal/dispose! (fn [_] (throw (ex-info "delete failed" {:type :injected})))]
          (runtime/committed! owner (:db-after cancel) [cancel])
          (is (path-exists? lease))
          (is (= 1 (count (:contexts @(:state owner)))))
          (is (= :datahike.backfill.runtime/context-limit
                 (error-type #(begin owner (:db-after cancel) (random-uuid))))))
        ;; Another cleanup boundary retries the retained retired context.
        (runtime/committed! owner (:db-after cancel) [])
        (is (not (path-exists? lease)))
        (is (empty? (:contexts @(:state owner)))))
      (finally (runtime/close! owner)))))

(deftest recovered-context-retires-and-inconsistent-cursors-are-refused
  (let [owner (runtime/create! nil) id (random-uuid)]
    (try
      (is (= :owner-lost
             (get-in (tx owner {:avet-build {:id id :attrs #{:score}} :max-tx 1} [])
                     [:db-after :avet-build-result :reason])))
      (is (empty? (:contexts @(:state owner))))
      (let [a (begin owner {} id)]
        (runtime/committed! owner (:db-after a) [a])
        (let [lease (runtime/acquire-prefix! owner id)]
          (try
            (is (= :datahike.backfill.runtime/invalid-cursor
                   (error-type #(runtime/reduce-prefix owner lease
                                                       (assoc (:cursor lease) :sequence 0)
                                                       (fn [n _ _] (inc n)) 0))))
            (is (= :datahike.backfill.runtime/invalid-cursor
                   (error-type #(runtime/reduce-prefix owner lease
                                                       (assoc (origin lease) :sequence 1)
                                                       (fn [n _ _] (inc n)) 0))))
            (is (= :datahike.backfill.runtime/invalid-commit
                   (error-type #(runtime/committed! owner
                                                    (assoc-in (:db-after a) [:avet-build-journal :cursor :sequence] 9)
                                                    [a]))))
            (finally (runtime/release-prefix! owner lease)))))
      (finally (runtime/close! owner)))))

(deftest failed-new-append-and-disposal-still-consume-context-quota
  (let [owner (runtime/create! {:max-contexts 1}) id (random-uuid)]
    (try
      (with-redefs [journal/append! (fn [& _] (throw (ex-info "append failed" {:type :append-injected})))
                    journal/dispose! (fn [_] (throw (ex-info "delete failed" {:type :delete-injected})))]
        (is (= :append-injected (error-type #(begin owner {} id))))
        (is (= 1 (count (:contexts @(:state owner)))))
        (is (= :datahike.backfill.runtime/context-limit
               (error-type #(begin owner {} (random-uuid))))))
      (runtime/committed! owner {} [])
      (is (empty? (:contexts @(:state owner))))
      (finally (runtime/close! owner)))))

(deftest commit-prefix-must-be-the-exact-final-accepted-report-boundary
  (let [owner (runtime/create! nil) id (random-uuid)]
    (try
      (let [a (begin owner {} id)
            b (tx owner (:db-after a) [])
            sequence-mismatch (assoc-in (:db-after a) [:avet-build-journal :cursor :sequence] 2)
            invalid-position (assoc-in (:db-after a)
                                       [:avet-build-journal :cursor :position] (Object.))]
        (doseq [invalid [sequence-mismatch invalid-position (:db-after b)]]
          (is (= :datahike.backfill.runtime/invalid-commit
                 (error-type #(runtime/committed! owner invalid [a])))))
        (is (= :datahike.backfill.runtime/invalid-commit
               (error-type #(runtime/committed! owner (:db-after a) []))))
        (is (= :datahike.backfill.runtime/unavailable
               (error-type #(runtime/acquire-prefix! owner id))))
        (is (= 1 (get-in (runtime/committed! owner (:db-after a) [a]) [:cursor :sequence])))
        (is (nil? (runtime/committed! owner (:db-after a) [])))
        (runtime/committed! owner (:db-after b) [b]))
      (finally (runtime/close! owner)))))

(deftest invalid-publication-does-not-retire-an-existing-generation
  (let [owner (runtime/create! nil) first-id (random-uuid) second-id (random-uuid)]
    (try
      (let [a (begin owner {} first-id)
            _ (runtime/committed! owner (:db-after a) [a])
            b (begin owner (:db-after a) second-id)
            invalid (assoc-in (:db-after b) [:avet-build-journal :cursor :sequence] 0)]
        (is (= :datahike.backfill.runtime/invalid-commit
               (error-type #(runtime/committed! owner invalid [b]))))
        (let [lease (runtime/acquire-prefix! owner first-id)]
          (try
            (is (= 1 (runtime/reduce-prefix owner lease (origin lease) (fn [n _ _] (inc n)) 0)))
            (finally (runtime/release-prefix! owner lease))))
        (is (= second-id (:generation (runtime/committed! owner (:db-after b) [b])))))
      (finally (runtime/close! owner)))))

(deftest unknown-committed-generation-is-refused
  (let [owner (runtime/create! nil)
        foreign {:avet-build {:id (random-uuid) :attrs #{:score}}}]
    (try
      (is (= :datahike.backfill.runtime/invalid-commit
             (error-type #(runtime/committed! owner foreign [(report {} foreign)]))))
      (is (empty? (:contexts @(:state owner))))
      (finally (runtime/close! owner)))))

(deftest batched-begin-cancel-replacement-starts-only-final-generation
  (let [owner (runtime/create! nil) first-id (random-uuid) final-id (random-uuid)]
    (try
      (let [a (begin owner {} first-id)
            a-path (get-in a [:db-after :avet-build-journal :descriptor :path])
            cancel (runtime/prepare-report! owner
                                            (report (:db-after a) (dissoc (:db-after a) :avet-build))
                                            'cancel-avet-build!)
            b (begin owner (:db-after cancel) final-id)
            c (tx owner (:db-after b) [[:effect]])
            ;; Future speculative generation must not be retired by this commit.
            future-id (random-uuid)
            future-report (begin owner (:db-after c) future-id)
            start (runtime/committed! owner (:db-after c) [a cancel b c])]
        (is (= final-id (:generation start)))
        (is (= (:db-after c) (:source-db start)))
        (is (= 2 (get-in start [:cursor :sequence])))
        (is (not (Files/exists (Path/of a-path (make-array String 0)) (make-array java.nio.file.LinkOption 0))))
        (is (= :datahike.backfill.runtime/unavailable
               (error-type #(runtime/acquire-prefix! owner first-id))))
        (is (= future-id (:generation (runtime/committed! owner (:db-after future-report) [future-report])))))
      (finally (runtime/close! owner)))))

(deftest leases-delay-retirement-and-close-cannot-resurrect
  (let [owner (runtime/create! {:max-readers 1}) id (random-uuid)
        a (begin owner {} id)]
    (runtime/committed! owner (:db-after a) [a])
    (let [lease (runtime/acquire-prefix! owner id)]
      (try
        (is (= :datahike.backfill.runtime/reader-limit
               (error-type #(runtime/acquire-prefix! owner id))))
        (let [cancel (runtime/prepare-report! owner
                                              (report (:db-after a) (dissoc (:db-after a) :avet-build))
                                              'cancel-avet-build!)]
          (is (path-exists? lease))
          (runtime/committed! owner (:db-after cancel) [cancel])
          (is (path-exists? lease))
          (is (= :datahike.backfill.runtime/unavailable
                 (error-type #(runtime/acquire-prefix! owner id))))
          (runtime/close! owner)
          (is (path-exists? lease))
          (is (= 1 (runtime/reduce-prefix owner lease (origin lease) (fn [n _ _] (inc n)) 0)))
          (is (= :datahike.backfill.runtime/closed (error-type #(begin owner {} (random-uuid)))))
          (is (nil? (runtime/committed! owner (:db-after a) [a]))))
        (finally (runtime/release-prefix! owner lease) (runtime/close! owner)))
      (is (not (path-exists? lease)))
      (is (nil? (runtime/release-prefix! owner lease))))))

(deftest taint-and-unsupported-operations-fail-build-not-transaction
  (doseq [[op taint? reason] [['transact! true :schema-changed]
                              ['replace-roots! false :unsupported-operation]]]
    (let [owner (runtime/create! nil) id (random-uuid)]
      (try
        (let [a (begin owner {} id)
              _ (runtime/committed! owner (:db-after a) [a])
              changed (runtime/prepare-report! owner
                                               (report (:db-after a)
                                                       (assoc (:db-after a) :avet-build-invalidated? taint?
                                                              :user-change :kept)) op)]
          (is (= :kept (get-in changed [:db-after :user-change])))
          (is (= reason (get-in changed [:db-after :avet-build-result :reason])))
          (is (nil? (get-in changed [:db-after :avet-build])))
          ;; Retirement is not visible until the accepted report commits.
          (let [lease (runtime/acquire-prefix! owner id)] (runtime/release-prefix! owner lease))
          (runtime/committed! owner (:db-after changed) [changed])
          (is (= :datahike.backfill.runtime/unavailable
                 (error-type #(runtime/acquire-prefix! owner id)))))
        (finally (runtime/close! owner))))))

(deftest limits-and-append-failures-leave-accepted-prefix-intact
  (let [owner (runtime/create! {:max-contexts 1 :journal {:max-frame-bytes 512}})
        id (random-uuid)]
    (try
      (let [a (begin owner {} id)]
        (runtime/committed! owner (:db-after a) [a])
        (is (= :datahike.backfill.runtime/context-limit
               (error-type #(begin owner (:db-after a) (random-uuid)))))
        (is (= :backfill.journal/frame-too-large
               (error-type #(tx owner (:db-after a) [(apply str (repeat 2000 "x"))]))))
        (let [b (tx owner (:db-after a) [])]
          (runtime/committed! owner (:db-after b) [b])
          (let [lease (runtime/acquire-prefix! owner id)]
            (try
              (is (= 2 (runtime/reduce-prefix owner lease (origin lease) (fn [n _ _] (inc n)) 0)))
              (is (= :datahike.backfill.runtime/invalid-cursor
                     (error-type #(runtime/reduce-prefix owner lease
                                                         (assoc (origin lease) :generation (random-uuid))
                                                         (fn [n _ _] (inc n)) 0))))
              (finally (runtime/release-prefix! owner lease))))))
      (finally (runtime/close! owner))))
  (let [owner (runtime/create! nil) disposed (atom 0) original journal/dispose!]
    (try
      (with-redefs [journal/append! (fn [& _] (throw (ex-info "injected" {:type :injected})))
                    journal/dispose! (fn [descriptor] (swap! disposed inc) (original descriptor))]
        (is (= :injected (error-type #(begin owner {} (random-uuid)))))
        (is (= 1 @disposed)))
      (is (empty? (:contexts @(:state owner))))
      (finally (runtime/close! owner)))))
