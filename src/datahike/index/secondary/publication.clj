(ns datahike.index.secondary.publication
  "Shared ownership state machine for immutable secondary generations.

   A publication hold fences newly written CAS objects until the Datahike root
   either names their generation or definitively rejects the transaction. The
   hold is deliberately not a mutable visibility pointer: only the primary
   Datahike head publishes a generation.

   Two roles act on one unpublished generation, and they are independent:

   - DERIVATION is linear. At most one transaction-local child may reserve a
     generation at a time, and once a child has persisted, the generation's
     lineage belongs to that child. Fan-out of derivations is refused.
   - PREPARATION is the commit that may publish this exact generation. The
     writer pipelines: its transaction loop derives transaction n+1 from the
     db of transaction n while its commit loop prepares and publishes db n.
     Preparation therefore may overlap a derivation (in either order), but a
     generation is prepared at most once at a time.

   Both roles may need the same holds (a child carries every ancestor's holds
   because it can share their immutable segments). A hold is therefore a
   shared CLAIM: the first commit that names it roots it (idempotently); an
   abort only gives up the aborting claimant's claim, and the underlying hold
   is aborted once no claimant is left that could still publish it."
  (:require
   [clojure.core.async :as async]
   [datahike.index.secondary :as sec]))

(defprotocol IUnpublishedGeneration
  "Internal capability for transferring one unpublished generation lineage."
  (-take-publication-holds! [index next-state])
  (-reserve-derivation! [index])
  (-release-derivation! [index]))

;; ---------------------------------------------------------------------------
;; Shared holds

(defrecord SharedHold [hold claims* outcome*])

(defn- shared-hold [hold]
  (if (instance? SharedHold hold)
    hold
    (->SharedHold hold (atom 1) (atom nil))))

(defn- failure-type [owner suffix]
  (keyword "secondary" (str (name (:family owner)) "-" suffix)))

(defn- claim-hold!
  "Add one claimant to a shared hold. A hold that was already rooted needs no
   claim; one that was already aborted cannot be claimed any more."
  [owner ^SharedHold hold]
  (locking hold
    (case @(:outcome* hold)
      :committed nil
      :aborted
      (throw (ex-info "A secondary publication hold was already aborted."
                      {:type (failure-type owner "publication-owner-conflict")
                       :index-family (:family owner)
                       :state :aborted}))
      (swap! (:claims* hold) inc)))
  hold)

(defn- root-hold!
  "Complete a hold because a durable root names it. Idempotent."
  [root! hold]
  (if (instance? SharedHold hold)
    (locking hold
      (when-not @(:outcome* hold)
        (root! (:hold hold))
        (reset! (:outcome* hold) :committed)))
    (root! hold)))

(defn- abort-hold!
  "Give up one claim. The underlying hold is aborted when no claim is left."
  [abort! hold]
  (if (instance? SharedHold hold)
    (locking hold
      (when-not @(:outcome* hold)
        (if (> (long @(:claims* hold)) 1)
          (swap! (:claims* hold) dec)
          (do
            (abort! (:hold hold))
            (reset! (:claims* hold) 0)
            (reset! (:outcome* hold) :aborted)))))
    (abort! hold)))

;; ---------------------------------------------------------------------------
;; Owner

;; holds*      — the generation's shared holds (never cleared; claims live in
;;               the holds themselves)
;; state*      — publication state: :unpublished :transferred :preparing
;;               :published :aborted :unknown
;; derivation* — nil | :reserved | :transferred
;; claimed*    — does this owner itself hold a claim on every hold?
;; released*   — holds whose owner claim was already given up (retry-safe)
(defrecord PublicationOwner [family holds* state* derivation* claimed*
                             released*])

(defn publication-owner
  "Create the exclusive publication owner for one immutable generation view."
  [family holds]
  (let [holds (mapv shared-hold holds)]
    (->PublicationOwner family (atom holds)
                        (atom (if (seq holds) :unpublished :published))
                        (atom nil)
                        (atom (boolean (seq holds)))
                        (atom #{}))))

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

(defn root-holds!
  "Root every hold because a durable Datahike root names their generation."
  [owner holds root!]
  (complete-holds! owner holds (partial root-hold! root!)))

(defn abort-holds!
  "Give up one claimant's claim on every hold, aborting holds nobody else
   claims. Retrying after a partial failure must use `released*` so that a
   claim is never given up twice."
  ([owner holds abort!]
   (complete-holds! owner holds (partial abort-hold! abort!)))
  ([owner holds abort! released*]
   (complete-holds! owner holds
                    (fn [hold]
                      (when-not (contains? @released* hold)
                        (abort-hold! abort! hold)
                        (swap! released* conj hold))))))

(defn- conflict! [owner requested]
  (throw (ex-info "A secondary generation already has an exclusive publication owner."
                  {:type (failure-type owner "publication-owner-conflict")
                   :index-family (:family owner)
                   :state @(:state* owner)
                   :derivation @(:derivation* owner)
                   :requested-state requested})))

(defn- claim-all! [owner]
  (let [holds @(:holds* owner)]
    (when-not (seq holds)
      (throw (ex-info "An unpublished secondary generation has no publication hold."
                      {:type (failure-type owner "missing-publication-hold")
                       :index-family (:family owner)})))
    (doseq [hold holds] (claim-hold! owner hold))
    holds))

(defn- transfer-to-child!
  "A persisted child takes this generation's lineage."
  [owner]
  (case @(:state* owner)
    :published []

    (:unpublished :preparing :unknown)
    (do
      (when (= :transferred @(:derivation* owner))
        (conflict! owner :transferred))
      (let [holds (if @(:claimed* owner)
                    ;; The owner's own claim moves to the child.
                    (let [holds @(:holds* owner)]
                      (when-not (seq holds)
                        (throw (ex-info "An unpublished secondary generation has no publication hold."
                                        {:type (failure-type owner "missing-publication-hold")
                                         :index-family (:family owner)})))
                      (reset! (:claimed* owner) false)
                      holds)
                    ;; A preparation holds the owner's claim; the child needs
                    ;; its own in case that commit is rejected.
                    (claim-all! owner))]
        (reset! (:derivation* owner) :transferred)
        (when (= :unpublished @(:state* owner))
          (reset! (:state* owner) :transferred))
        holds))

    (conflict! owner :transferred)))

(defn begin-preparation!
  "Atomically claim this generation for one commit.

   Returns `{:status :published}` for an already published generation, or
   `{:status :prepare :holds holds}`. A preparation may overlap a derivation;
   a second concurrent preparation, or preparing an aborted or ambiguous
   generation, is refused."
  [owner]
  (locking (:state* owner)
    (case @(:state* owner)
      :published {:status :published}

      (:unpublished :transferred)
      (let [holds (if (and @(:claimed* owner) (nil? @(:derivation* owner)))
                    ;; Nobody else will derive from this lineage right now:
                    ;; the owner's claim moves to the preparation.
                    (let [holds @(:holds* owner)]
                      (when-not (seq holds)
                        (throw (ex-info "An unpublished secondary generation has no publication hold."
                                        {:type (failure-type owner "missing-publication-hold")
                                         :index-family (:family owner)})))
                      (reset! (:claimed* owner) false)
                      holds)
                    ;; A reserved or persisted child keeps its claim.
                    (claim-all! owner))]
        (reset! (:state* owner) :preparing)
        {:status :prepare :holds holds})

      (conflict! owner :preparing))))

(defn- lineage-state
  "The state an unpublished generation returns to after its preparation was
   rejected: it is still unpublished while its owner or a child claims it."
  [owner]
  (cond
    @(:claimed* owner) :unpublished
    (= :transferred @(:derivation* owner)) :transferred
    :else :aborted))

(defn abandon-preparation!
  "Undo `begin-preparation!` whose prepared view could not be built."
  [owner holds abort!]
  (locking (:state* owner)
    (abort-holds! owner holds abort!)
    (reset! (:state* owner) (lineage-state owner))))

(defn take-publication-holds!
  "Transfer the owner's holds to preparation or to a linear child."
  [owner next-state]
  (locking (:state* owner)
    (case next-state
      :transferred (transfer-to-child! owner)
      :preparing (let [{:keys [status holds]} (begin-preparation! owner)]
                   (if (= :published status) [] holds))
      (conflict! owner next-state))))

(defn reserve-derivation!
  "Reserve an unpublished lineage for one transaction-local child.

   Returns false for a published generation (nothing to own). The reservation
   may overlap a preparation of the same generation: the writer pipelines."
  [owner]
  (locking (:state* owner)
    (case @(:state* owner)
      :published false

      (:unpublished :preparing :unknown)
      (do
        (when @(:derivation* owner)
          (conflict! owner :deriving))
        (when-not @(:claimed* owner)
          ;; The preparation took the owner's claim. Re-acquire one for the
          ;; lineage now, so a rejected commit cannot release holds this child
          ;; may still publish.
          (claim-all! owner)
          (reset! (:claimed* owner) true)
          (reset! (:released* owner) #{}))
        (reset! (:derivation* owner) :reserved)
        true)

      (conflict! owner :deriving))))

(defn release-derivation!
  "Return an unused derivation reservation to its source generation."
  [owner]
  (locking (:state* owner)
    (when (= :reserved @(:derivation* owner))
      (reset! (:derivation* owner) nil)
      true)))

(defn abort-unpublished!
  "Abort holds owned by a closing generation, unless a child owns derivation."
  [owner abort!]
  (locking (:state* owner)
    (when (= :reserved @(:derivation* owner))
      (throw
       (ex-info "Cannot close a secondary generation while a transient derivation owns it."
                {:type (failure-type owner "publication-owner-conflict")
                 :index-family (:family owner)
                 :state :deriving})))
    (when (and @(:claimed* owner) (not= :published @(:state* owner)))
      (abort-holds! owner @(:holds* owner) abort! (:released* owner))
      (reset! (:claimed* owner) false)
      (when (= :unpublished @(:state* owner))
        (reset! (:state* owner) :aborted))
      true)))

(defrecord PreparedGeneration
           [prepared-index publication-holds owns-prepared? owner release-state
            root! abort! close-prepared! released*]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] prepared-index)
  (-sec-release [_ outcome]
    (async/thread
      (try
        (locking release-state
          (locking (:state* owner)
            (let [previous @release-state
                  status (:status outcome)
                  ;; An already-published generation prepared without holds
                  ;; does not own its owner's state: a rejected commit of it
                  ;; must not mark a published generation as aborted.
                  drives-owner? (or owns-prepared? (seq publication-holds))]
              (cond
                (#{:committed :aborted} previous)
                nil

                (and (= :unknown previous) (= :committed status))
                (do
                  (root-holds! owner publication-holds root!)
                  (reset! (:state* owner) :published)
                  (reset! release-state :committed))

                (and (= :unknown previous) (= :unknown status))
                nil

                (= :committed status)
                (do
                  (root-holds! owner publication-holds root!)
                  (reset! (:state* owner) :published)
                  (reset! release-state :committed))

                (and (= :aborted status) (nil? previous))
                (let [completed? (atom false)]
                  (try
                    (abort-holds! owner publication-holds abort! released*)
                    (reset! completed? true)
                    (finally
                      (when owns-prepared? (close-prepared! prepared-index))))
                  (when @completed?
                    (when drives-owner?
                      (reset! (:state* owner) (lineage-state owner)))
                    (reset! release-state :aborted)))

                (= :unknown status)
                (do
                  ;; The primary head may still land. Retain the lightweight
                  ;; storage fences for reconciliation, but release local views.
                  (when owns-prepared? (close-prepared! prepared-index))
                  (when drives-owner?
                    (reset! (:state* owner) :unknown))
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
                        (atom nil) root! abort! close-prepared! (atom #{})))
