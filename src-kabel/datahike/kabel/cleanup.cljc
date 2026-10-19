(ns ^:no-doc datahike.kabel.cleanup
  "Native ownership retained after a failed replica constructor. This capability
   is local-only; never serialize a failed-connect exception onto a transport."
  (:require [datahike.kabel.writer :as writer]
            [konserve.store :as store]
            #?(:clj [clojure.core.async :refer [chan promise-chan put!]]
               :cljs [clojure.core.async :refer [chan promise-chan put!] :include-macros true])
            #?(:clj [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer [go-try- <?-] :include-macros true])))

(defn owner []
  (let [gate (chan 1)]
    (put! gate true)
    {:gate gate :state (atom {})}))

(defn cleanup!
  "Fence first, then join the exact strategy's drain before closing its store.
   Failure retains completed phases and resources. Never unregister shared PSS
   storage and never infer ownership from today's topic at retry time."
  [{:keys [gate state]}]
  (when-let [retire! (:retire-initial! @state)] (retire!))
  (go-try-
   (<?- gate)
   (try
     (when-not (:drained? @state)
       (swap! state assoc :phase :subscription-drain)
       (let [{:keys [strategy peer topic]} @state]
         (when strategy (<?- (writer/await-topic-release! peer topic 5000 strategy))))
       (swap! state assoc :drained? true))
     (when (and (:store @state) (not (:store-released? @state)))
       (swap! state assoc :phase :store-release)
       (let [{:keys [store store-config]} @state]
         (<?- (store/release-store store-config store {:sync? false})))
       (swap! state assoc :store-released? true))
     (swap! state assoc :phase :complete)
     true
     (finally (put! gate true)))))

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
