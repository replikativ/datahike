(ns ^:no-doc datahike.backfill.coordinator
  "Owned JVM AVET workers. Writer supplies internal dispatch and successful
   commit notifications; no public transaction options or durable local tokens."
  (:require [datahike.backfill.admission :as admission]
            [datahike.backfill.avet :as avet]
            [datahike.backfill.control :as control]
            [datahike.backfill.job :as job]
            [datahike.backfill.runtime :as runtime]
            [datahike.gc-guard :as guard]
            [datahike.gc-roots :as roots]
            [datahike.store :as ds]
            [replikativ.logging :as log])
  (:import [java.util.concurrent ThreadPoolExecutor TimeUnit ArrayBlockingQueue
            ThreadFactory]))

(defn- fail! [kind message]
  (throw (ex-info message {:type kind})))

(defn- wake! [work f]
  (locking work (swap! work f) (.notifyAll ^Object work)))

(defn- submit! [owner request]
  ;; Dispatch must enqueue, never await writer completion or run admission here.
  ((get-in owner [:options :submit!]) request))

(defn- renew-work! [work]
  (when-let [root-id (:root-id @work)]
    (when (> (- (System/nanoTime) (:renewed-nanos @work)) 60000000000)
      ;; Renewal is owned by the worker/final-admission call, not a detached loop.
      (roots/renew! (:source @work) root-id {:sync? true})
      (swap! work assoc :renewed-nanos (System/nanoTime)))))

(defn- check-work! [work]
  (when (:cancel? @work) (fail! ::cancelled "AVET worker was cancelled."))
  (renew-work! work))

(defn- with-prefix [owner id f]
  (let [lease (runtime/acquire-prefix! (:runtime owner) id)]
    (try (f lease) (finally (runtime/release-prefix! (:runtime owner) lease)))))

(defn- advance! [owner certificate lease]
  (let [start (admission/cursor certificate)]
    (if (= start (:cursor lease))
      certificate
      (admission/advance-range
       certificate (:source-db lease) (:cursor lease)
       (fn [f init]
         (runtime/reduce-prefix (:runtime owner) lease start
                                (fn [acc frame cursor]
                                  (control/check!) (f acc frame cursor)) init))))))

(defn- cleanup! [owner id work]
  ;; Keep failed cleanup in the bounded registry so disk/pin failures cannot
  ;; bypass job capacity. close! retries it and reports remaining failures.
  (try
    (when-let [candidate (:candidate @work)]
      (avet/close! candidate)
      (swap! work dissoc :candidate :certificate))
    (when-let [root-id (:root-id @work)]
      (roots/release! (:source @work) root-id {:sync? true})
      (swap! work dissoc :root-id))
    (when-let [token (:guard @work)]
      (guard/done! (:store-id @work) token)
      (swap! work dissoc :guard))
    (swap! (:state owner) update :jobs dissoc id)
    (catch Throwable e
      (swap! work assoc :phase :cleanup-failed :cleanup-error e)
      (log/warn :datahike/avet-cleanup-failed {:generation id :message (ex-message e)}))))

(defn- await-offer! [work]
  (loop []
    (let [{:keys [phase cancel? terminal?]} @work]
      (cond
        terminal? :done
        ;; Never reclaim storage from an active writer admission/commit.
        (contains? #{:admitting :awaiting-commit} phase)
        (do (renew-work! work) (locking work (.wait ^Object work 100)) (recur))
        cancel? (fail! ::cancelled "AVET worker was cancelled.")
        (= :running phase) :again
        :else (do
                ;; Renewal I/O must not hold the monitor needed by commit's
                ;; nonblocking cancellation/notification callback.
                (check-work! work)
                (locking work (.wait ^Object work 100))
                (recur))))))

(defn- await-writer! [work]
  ;; A renewal/cancellation failure can race the writer taking an offer. Even
  ;; this exceptional path may not close private storage under an admission.
  (locking work
    (loop []
      (when (and (not (:terminal? @work))
                 (contains? #{:admitting :awaiting-commit} (:phase @work)))
        (.wait ^Object work 100)
        (recur)))))

(defn- run-work! [owner id work]
  (try
    (binding [control/*check!* #(check-work! work)]
      (control/check!)
      (with-prefix
        owner id
        (fn [lease]
          (let [source (:source-db lease)
                sid (ds/canonical-store-id (:store source) (get-in source [:config :store]))]
            (job/supported! source)
            (when-not (job/matches? source (:avet-build source))
              (fail! ::stale-source "Committed AVET source no longer matches its descriptor."))
            (swap! work assoc :source source :store-id sid :guard (guard/writing! sid))
            (let [root-id (roots/pin! source {:owner {:avet-build id}
                                              :note "AVET background build"} {:sync? true})]
              (swap! work assoc :root-id root-id :renewed-nanos (System/nanoTime)))
            (control/check!)
            (let [candidate (avet/build! source (keys (get-in source [:avet-build :patch]))
                                         (get-in owner [:options :build-options]))]
              (swap! work assoc :candidate candidate)
              (let [certificate (admission/mint! source candidate
                                                 (get-in source [:avet-build :patch]) (:cursor lease))]
                (swap! work assoc :certificate certificate :phase :running))))))
      (loop []
        (control/check!)
        (let [certificate (admission/flush! (with-prefix owner id #(advance! owner (:certificate @work) %)))]
          (wake! work #(assoc % :certificate certificate :phase :offered :token (Object.)))
          (submit! owner {:op :install :generation id :token (:token @work)})
          (when (= :again (await-offer! work)) (recur)))))
    (catch Throwable e
      (swap! work assoc :failure e)
      (when-not (or (:cancel? @work) (:terminal? @work))
        (try (submit! owner {:op :cancel :generation id :reason :worker-failed})
             (catch Throwable dispatch-error
               (.addSuppressed e dispatch-error)))))
    (finally
      (wake! work #(assoc % :cancel? true))
      (await-writer! work)
      (cleanup! owner id work))))

(defn create!
  "Create bounded owned worker capacity. :submit! must enqueue internal request
   maps without waiting for the writer. Call close! after writer admission and
   commit drain; close never abandons resources used by an in-flight install."
  [journal-runtime options]
  (let [options (merge {:max-jobs (get-in journal-runtime [:options :max-contexts])
                        :tail-bytes 1048576 :tail-transactions 128} options)]
    (when-not (and (map? options)
                   (every? #{:submit! :max-jobs :tail-bytes :tail-transactions :build-options} (keys options))
                   (ifn? (:submit! options))
                   (every? #(and (integer? %) (pos? %) (<= % Integer/MAX_VALUE))
                           (map options [:max-jobs :tail-bytes :tail-transactions]))
                   (<= (:max-jobs options) 1024))
      (fail! ::invalid-options "Invalid AVET coordinator options."))
    (let [factory (reify ThreadFactory
                    (newThread [_ runnable]
                      (doto (Thread. runnable "datahike-avet-worker") (.setDaemon true))))]
      {:runtime journal-runtime :options options
       :state (atom {:closed? false :jobs {}})
       :executor (ThreadPoolExecutor. 1 1 0 TimeUnit/MILLISECONDS
                                      (ArrayBlockingQueue. (int (:max-jobs options))) factory)})))

(defn after-commit!
  "Nonblocking successful-publication notification, AFTER runtime/committed!.
   No pin/storage I/O and no waiting for workers on the commit loop. A capacity
   failure fails only the background job, never the committed begin transaction."
  [owner final-db start]
  (let [active (get-in final-db [:avet-build :id])
        state (:state owner)
        created
        (locking state
          (doseq [[id work] (:jobs @state) :when (not= active id)]
            (wake! work #(assoc % :cancel? true
                                :terminal? (contains? #{:admitting :awaiting-commit} (:phase %)))))
          (when (and start (not (:closed? @state)))
            (let [id (:generation start)]
              (when-not (get-in @state [:jobs id])
                (if (>= (count (:jobs @state)) (get-in owner [:options :max-jobs]))
                  :full
                  (let [work (atom {:phase :queued :cancel? false})]
                    (swap! state assoc-in [:jobs id] work)
                    [id work]))))))]
    (if (= :full created)
      ;; Callback is required to be nonblocking. Do not turn failure to enqueue
      ;; a failed-start cancellation into a durable transaction failure.
      (do
        (swap! state assoc :last-start-failure {:generation (:generation start) :reason :worker-capacity})
        (try (submit! owner {:op :cancel :generation (:generation start) :reason :worker-capacity})
             (catch Throwable e
               (log/warn :datahike/avet-start-failed {:generation (:generation start) :message (ex-message e)}))))
      (when created
        (let [[id work] created]
          (try (.execute ^ThreadPoolExecutor (:executor owner) #(run-work! owner id work))
               (catch Throwable e
                 (swap! state update :jobs dissoc id)
                 (swap! state assoc :last-start-failure {:generation id :reason :worker-capacity})
                 (try (submit! owner {:op :cancel :generation id :reason :worker-capacity})
                      (catch Throwable nested (.addSuppressed e nested)))
                 (log/warn :datahike/avet-start-failed {:generation id :message (ex-message e)}))))))
    nil))

(defn- owned-work! [owner token]
  (or (some (fn [[id work]] (when (identical? token (:token @work)) [id work]))
            (:jobs @(:state owner)))
      (fail! ::stale-offer "Unknown or retired AVET install offer.")))

(defn try-install!
  "Take exclusive candidate ownership after a writer pending-batch boundary.
   Returns ready certificate or retry without a transaction. Caller MUST invoke
   accepted! or abort-install! after a ready result, including predicate errors."
  [owner actual-source token]
  (let [[id work] (owned-work! owner token)]
    (locking work
      (when-not (and (= :offered (:phase @work)) (not (:cancel? @work))
                     (= id (get-in actual-source [:avet-build :id])))
        (fail! ::stale-offer "AVET offer is no longer available for this generation."))
      (swap! work assoc :phase :admitting))
    (try
      (binding [control/*check!* #(check-work! work)]
        (job/supported! actual-source)
        (control/check!)
        (with-prefix
          owner id
          (fn [lease]
            (let [certificate (:certificate @work)
                  before (admission/cursor certificate) after (:cursor lease)]
              (if (or (> (runtime/range-byte-size (:runtime owner) lease before)
                         (get-in owner [:options :tail-bytes]))
                      (> (- (:sequence after) (:sequence before)) (get-in owner [:options :tail-transactions])))
                (do (wake! work #(assoc % :phase :running)) {:status :retry})
                (let [certificate (admission/flush! (advance! owner certificate lease))
                      certificate (admission/rebind-source certificate actual-source)]
                  (roots/assert-live! (:source @work) (:root-id @work) roots/DEFAULT_TTL_MS {:sync? true})
                  (swap! work assoc :certificate certificate)
                  {:status :ready :certificate certificate}))))))
      (catch Throwable e
        ;; Only the explicit bounded-tail outcome is automatically retried.
        ;; A deterministic admission failure must not race an endless reoffer.
        (wake! work #(assoc % :phase :running :cancel? true))
        (throw e)))))

(defn accepted! [owner token]
  (let [[_ work] (owned-work! owner token)]
    (locking work
      (when-not (= :admitting (:phase @work)) (fail! ::stale-offer "AVET admission is not owned."))
      (swap! work assoc :phase :awaiting-commit)
      (.notifyAll ^Object work)))
  nil)

(defn check-install!
  "Assert an owned admission/queued install remains protected. Writer invokes
   again immediately before conditional commit, not only before transaction
   evaluation. Throws before publication if renewal/cancellation invalidated it."
  [owner token]
  (let [[_ work] (owned-work! owner token)]
    (when-not (contains? #{:admitting :awaiting-commit} (:phase @work))
      (fail! ::stale-offer "Install is not owned by the writer."))
    (check-work! work)
    (roots/assert-live! (:source @work) (:root-id @work) roots/DEFAULT_TTL_MS {:sync? true}))
  nil)

(defn with-install!
  "Run activation and predicates with cooperative cancellation, before queue
   acceptance. f must not enqueue/commit or release the install token itself."
  [owner token f]
  (let [[_ work] (owned-work! owner token)]
    (binding [control/*check!* #(check-work! work)]
      (check-install! owner token)
      (let [result (f)]
        (check-install! owner token)
        result))))

(defn abort-install!
  "Return nonpublished candidate ownership. Definite conflicts retire it; a
   predicate/admission failure may retry only within the same runtime lineage."
  [owner token definite?]
  (when-let [[_ work] (some (fn [[id work]] (when (identical? token (:token @work)) [id work]))
                            (:jobs @(:state owner)))]
    (wake! work #(cond-> (assoc % :phase :running) definite? (assoc :cancel? true))))
  nil)

(defn invalidate! [owner]
  (doseq [[_ work] (:jobs @(:state owner))]
    (wake! work #(assoc % :cancel? true)))
  nil)

(defn close!
  "Cancel and JOIN all owned execution. Writer must first settle admissions and
   commits with accepted/abort/after-commit callbacks. Cleanup failures remain
   bounded and are reported; a second close retries them."
  [owner]
  (swap! (:state owner) assoc :closed? true)
  (invalidate! owner)
  (.shutdown ^ThreadPoolExecutor (:executor owner))
  (let [interrupted? (volatile! false)]
    (try
      (loop []
        (when-not (try (.awaitTermination ^ThreadPoolExecutor (:executor owner) 100 TimeUnit/MILLISECONDS)
                       (catch InterruptedException _ (vreset! interrupted? true) false))
          (recur)))
      (doseq [[id work] (:jobs @(:state owner))] (cleanup! owner id work))
      (when (seq (:jobs @(:state owner)))
        (fail! ::cleanup-failed "AVET resources could not all be released; retry close."))
      (finally (when @interrupted? (.interrupt (Thread/currentThread))))))
  nil)
