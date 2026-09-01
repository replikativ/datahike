(ns datahike.index.secondary.publication
  "Shared ownership state machine for immutable secondary generations.

   A publication hold fences newly written CAS objects until the Datahike root
   either names their generation or definitively rejects the transaction. The
   hold is deliberately not a mutable visibility pointer: only the primary
   Datahike head publishes a generation."
  (:require
   [clojure.core.async :as async]
   [datahike.index.secondary :as sec]))

(defprotocol IUnpublishedGeneration
  "Internal capability for transferring one unpublished generation lineage."
  (-take-publication-holds! [index next-state])
  (-reserve-derivation! [index])
  (-release-derivation! [index]))

(defrecord PublicationOwner [family holds* state*])

(defn publication-owner
  "Create the exclusive publication owner for one immutable generation view."
  [family holds]
  (->PublicationOwner family (atom (vec holds))
                      (atom (if (seq holds) :unpublished :published))))

(defn publication-state [owner]
  @(:state* owner))

(defn set-publication-state!
  "Set a backend-validated terminal/intermediate state under the owner lock."
  [owner state]
  (locking (:state* owner)
    (reset! (:state* owner) state)))

(defn completed
  "Return an already-delivered awaitable carrying a value or failure."
  [value]
  (let [ch (async/promise-chan)]
    (async/put! ch value)
    ch))

(defn- failure-type [owner suffix]
  (keyword "secondary" (str (name (:family owner)) "-" suffix)))

(defn complete-holds!
  "Apply an idempotent backend completion to every hold, reporting all failures."
  [owner holds complete!]
  (let [failures
        (reduce (fn [errors hold]
                  (try
                    (complete! hold)
                    errors
                    (catch Throwable failure
                      (conj errors failure))))
                [] holds)]
    (when (seq failures)
      (throw (ex-info "One or more secondary publication holds failed to close."
                      {:type (failure-type owner "publication-cleanup-failed")
                       :index-family (:family owner)
                       :failure-count (count failures)}
                      (first failures))))))

(defn take-publication-holds!
  "Transfer the owner's holds exactly once to preparation or a linear child."
  [owner next-state]
  (locking (:state* owner)
    (case @(:state* owner)
      :published []

      :unpublished
      (let [holds @(:holds* owner)]
        (when-not (seq holds)
          (throw (ex-info "An unpublished secondary generation has no publication hold."
                          {:type (failure-type owner "missing-publication-hold")
                           :index-family (:family owner)})))
        (reset! (:holds* owner) [])
        (reset! (:state* owner) next-state)
        holds)

      :deriving
      (if (= :transferred next-state)
        (let [holds @(:holds* owner)]
          (when-not (seq holds)
            (throw (ex-info "A derived secondary generation has no publication hold."
                            {:type (failure-type owner "missing-publication-hold")
                             :index-family (:family owner)})))
          (reset! (:holds* owner) [])
          (reset! (:state* owner) next-state)
          holds)
        (throw
         (ex-info "A secondary derivation can only transfer its publication ownership."
                  {:type (failure-type owner "publication-owner-conflict")
                   :index-family (:family owner)
                   :state :deriving
                   :requested-state next-state})))

      (throw (ex-info "A secondary generation already has an exclusive publication owner."
                      {:type (failure-type owner "publication-owner-conflict")
                       :index-family (:family owner)
                       :state @(:state* owner)
                       :requested-state next-state})))))

(defn reserve-derivation!
  "Reserve an unpublished lineage for one transaction-local child."
  [owner]
  (locking (:state* owner)
    (case @(:state* owner)
      :published false
      :unpublished (do (reset! (:state* owner) :deriving) true)
      (throw (ex-info "This secondary generation already has an exclusive publication owner."
                      {:type (failure-type owner "publication-owner-conflict")
                       :index-family (:family owner)
                       :state @(:state* owner)})))))

(defn release-derivation!
  "Return an unused derivation reservation to its source generation."
  [owner]
  (locking (:state* owner)
    (when (= :deriving @(:state* owner))
      (reset! (:state* owner) :unpublished)
      true)))

(defn abort-unpublished!
  "Abort holds owned by a closing generation, unless a child owns derivation."
  [owner abort!]
  (locking (:state* owner)
    (case @(:state* owner)
      :deriving
      (throw
       (ex-info "Cannot close a secondary generation while a transient derivation owns it."
                {:type (failure-type owner "publication-owner-conflict")
                 :index-family (:family owner)
                 :state :deriving}))

      :unpublished
      (let [holds @(:holds* owner)]
        (complete-holds! owner holds abort!)
        (reset! (:holds* owner) [])
        (reset! (:state* owner) :aborted)
        true)

      nil)))

(defrecord PreparedGeneration
           [prepared-index publication-holds owns-prepared? owner release-state
            root! abort! close-prepared!]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] prepared-index)
  (-sec-release [_ outcome]
    (async/thread
      (try
        (locking release-state
          (locking (:state* owner)
            (let [previous @release-state
                  status (:status outcome)]
              (cond
                (#{:committed :aborted} previous)
                nil

                (and (= :unknown previous) (= :committed status))
                (do
                  (complete-holds! owner publication-holds root!)
                  (reset! (:state* owner) :published)
                  (reset! release-state :committed))

                (and (= :unknown previous) (= :unknown status))
                nil

                (= :committed status)
                (do
                  (complete-holds! owner publication-holds root!)
                  (reset! (:state* owner) :published)
                  (reset! release-state :committed))

                (and (= :aborted status) (nil? previous))
                (let [completed? (atom false)]
                  (try
                    (complete-holds! owner publication-holds abort!)
                    (reset! completed? true)
                    (finally
                      (when owns-prepared? (close-prepared! prepared-index))))
                  (when @completed?
                    (reset! (:state* owner) :aborted)
                    (reset! release-state :aborted)))

                (= :unknown status)
                (do
                  ;; The primary head may still land. Retain the lightweight
                  ;; storage fences for reconciliation, but release local views.
                  (when owns-prepared? (close-prepared! prepared-index))
                  (reset! (:state* owner) :unknown)
                  (reset! release-state :unknown))

                :else
                (throw
                 (ex-info "An ambiguous secondary generation cannot later be aborted."
                          {:type :secondary/ambiguous-generation-abort
                           :previous previous
                           :outcome outcome}))))))
        true
        (catch Throwable failure failure)))))

(defn prepared-generation
  "Create Datahike's common prepared-generation release/reconciliation handle."
  [prepared-index publication-holds owns-prepared? owner
   {:keys [root! abort! close-prepared!]}]
  (->PreparedGeneration prepared-index publication-holds owns-prepared? owner
                        (atom nil) root! abort! close-prepared!))
