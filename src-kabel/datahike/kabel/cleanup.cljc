(ns ^:no-doc datahike.kabel.cleanup
  "Native ownership retained after a failed replica constructor. This capability
   is local-only; never serialize a failed-connect exception onto a transport."
  (:require [datahike.store-registry :as registry]
            [konserve.store :as store]
            [clojure.core.async.impl.protocols :as async-protocols]
            #?(:clj [clojure.core.async :refer [chan promise-chan put!]]
               :cljs [clojure.core.async :refer [chan promise-chan put!] :include-macros true])
            #?(:clj [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer [go-try- <?-] :include-macros true])))

(defn owner []
  (let [gate (chan 1)]
    (put! gate true)
    {:gate gate :state (atom {})}))

(defn- cleanup-resources!
  "Fence first, then join the exact strategy's drain before closing its store.
   Failure retains completed phases and resources. Release only the captured
   registry lease; never infer ownership from today's topic at retry time."
  [{:keys [gate state]}]
  (when-not (:drained? @state)
    (swap! state assoc :phase :retirement-fence)
    (when-let [retire! (:retire-initial! @state)] (retire!)))
  (let [drain! (when-not (:drained? @state) (:drain! @state))
        drain (when drain! (drain!))]
    (when (and drain! (not (satisfies? async-protocols/ReadPort drain)))
      (throw (ex-info "Replica drain did not return a completion channel"
                      {:type :datahike.kabel/unconfirmed-drain})))
    (go-try-
     (<?- gate)
     (try
       (when-not (:drained? @state)
         (swap! state assoc :phase :subscription-drain)
         (let [result (cond drain! (<?- drain)
                            (:strategy @state)
                            (throw (ex-info "Replica strategy has no captured drain capability"
                                            {:type :datahike.kabel/unconfirmed-drain}))
                            :else true)]
           (when-not (true? result)
             (throw (ex-info "Replica drain did not positively confirm cleanup"
                             {:type :datahike.kabel/unconfirmed-drain}))))
         (swap! state assoc :drained? true))
       (when-let [lease (:registration @state)]
         (registry/release! lease)
         (swap! state dissoc :registration))
       (when (and (:store @state) (not (:store-released? @state)))
         (swap! state assoc :phase :store-release)
         (let [{:keys [store store-config]} @state]
           (<?- (store/release-store store-config store {:sync? false})))
         (swap! state assoc :store-released? true))
       (swap! state assoc :phase :complete)
       (swap! state dissoc :store :store-config :strategy :peer :topic
              :drain! :retire-initial!)
       true
       (finally (put! gate true))))))

(defn cleanup!
  "Share pending/successful retirement; an explicit call after failure retries
   only unfinished phases. The local owner survives connection invalidation."
  [{:keys [state] :as owner}]
  (let [start! (fn []
                 (if (#{:pending :complete} (get-in @state [:cleanup :phase]))
                   (get-in @state [:cleanup :result])
                   (let [result (promise-chan)]
                     (swap! state assoc :cleanup {:phase :pending :result result})
                     (try
                       (let [operation (cleanup-resources! owner)]
                         (go-try-
                          (try
                            (<?- operation)
                            (swap! state assoc-in [:cleanup :phase] :complete)
                            (put! result true)
                            (catch #?(:clj Throwable :cljs :default) error
                              (swap! state assoc-in [:cleanup :phase] :error)
                              (put! result error)))))
                       (catch #?(:clj Throwable :cljs :default) error
                         (swap! state assoc-in [:cleanup :phase] :error)
                         (put! result error)))
                     result)))]
    #?(:clj (locking state (start!)) :cljs (start!))))

(defn confirmed? [error] (true? (::confirmed? (ex-data error))))

(defn failure!
  "Yield the original failure annotated with positive cleanup confirmation, or
   a retained local retry capability on uncertainty. Preserve the original type
   and message for diagnostics; do not suppress a failed drain/store cleanup."
  [owner cause]
  (go-try-
   (try
     (<?- (cleanup! owner))
     (ex-info (ex-message cause) (assoc (ex-data cause) ::confirmed? true ::phase :complete) cause)
     (catch #?(:clj Throwable :cljs :default) cleanup-error
       (ex-info (ex-message cause)
                (assoc (ex-data cause) ::confirmed? false
                       ::phase (:phase @(:state owner))
                       ::cleanup-error (ex-message cleanup-error)
                       ::retry! #(cleanup! owner))
                cause)))))

(defn retry!
  "Retry only a failed constructor's captured cleanup, never its construction.
   Returns a channel yielding true or an error. This is a native, local API."
  [error]
  (if-let [retry! (::retry! (ex-data error))]
    (retry!)
    (doto (promise-chan) (put! (ex-info "No retained constructor cleanup capability" {})))))
