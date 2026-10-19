(ns ^:no-doc datahike.kabel.subscription
  "Physical ownership of a replica subscription. No transactions or retries of
   remote writes: reconnect only refreshes the existing local store and DB."
  (:require [kabel.pubsub :as pubsub]
            [konserve-sync.pubsub :as sync]
            #?(:clj [clojure.core.async :refer [chan promise-chan put! alts! timeout]]
               :cljs [clojure.core.async :refer [chan promise-chan put! alts! timeout] :include-macros true])
            #?(:clj [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer [go-try- <?-] :include-macros true])))

(defn- locked [controller f]
  #?(:clj (locking (:lock controller) (f))
     :cljs (f)))

(defn controller
  "Own the existing initial subscription, then subsequent authenticated epochs.
   Caller supplies the exact sync-store, branch walker and publication callback
   already used to open the replica. A serialized physical operation gate avoids
   an older pending subscribe stealing a newer epoch's topic."
  [{:keys [peer topic strategy] :as opts}]
  (when-not (and strategy (identical? strategy (:strategy (pubsub/subscription peer topic))))
    (throw (ex-info "Initial replica subscription is no longer owned"
                    {:type :datahike.kabel/subscription-conflict :topic topic})))
  (let [gate (chan 1)
        state (atom {:owned strategy})]
    (put! gate true)
    (assoc opts :gate gate :state state :lock (or (:lock opts) state))))

(defn- current? [controller generation]
  (let [s @(:state controller)]
    (and (not (:closed? s)) (= generation (get-in s [:operation :generation])))))

(defn- cancelled []
  (ex-info "Replica synchronization was superseded or released"
           {:type :datahike.kabel/sync-cancelled}))

(defn- assert-current! [controller generation]
  (when-not (current? controller generation) (throw (cancelled)))
  (when-let [error (get-in @(:state controller) [:operation :error])] (throw error)))

(defn- release-owned! [{:keys [peer topic release! state]} timeout-ms]
  (go-try-
   (let [actual (:strategy (pubsub/subscription peer topic))]
     (when actual
       (when-not (identical? actual (:owned @state))
         (throw (ex-info "Replica topic belongs to another subscription"
                         {:type :datahike.kabel/subscription-conflict :topic topic})))
       (<?- (release! timeout-ms (:owned @state)))))))

(defn- bounded! [ch timeout-ms & [cancel]]
  (go-try-
   (let [[value port] (alts! (cond-> [ch (timeout timeout-ms)] cancel (conj cancel)))]
     (when (= port cancel) (throw (cancelled)))
     (when-not (= ch port)
       (throw (ex-info "Replica synchronization did not complete in time"
                       {:type :kabel-handshake-timeout :timeout-ms timeout-ms})))
     (when (instance? #?(:clj Throwable :cljs js/Error) value) (throw value))
     value)))

(defn subscribe!
  "Same epoch shares one result channel, including failures; a new explicit
   epoch may retry. Only a newly observed head after full handshake drain is
   published. Last known DB remains readable on error, never called current."
  [{:keys [state gate peer topic store branch walk-fn publish! retire-initial!] :as controller}
   {:keys [epoch handshake-timeout-ms] :or {handshake-timeout-ms 120000} :as opts}]
  (locked
   controller
   (fn []
     (let [prior (:operation @state)]
       (cond
         (:closed? @state) (doto (promise-chan) (put! (cancelled)))
         (not (and (map? opts) (every? #{:epoch :handshake-timeout-ms} (keys opts))
                   (integer? handshake-timeout-ms) (<= 1 handshake-timeout-ms 2147483647)))
         (doto (promise-chan) (put! (ex-info "Invalid replica synchronization options"
                                             {:type :datahike.kabel/invalid-sync-options})))
         (nil? epoch) (doto (promise-chan) (put! (ex-info "Replica sync requires an authenticated epoch"
                                                          {:type :datahike.kabel/missing-epoch})))
         (= epoch (:epoch prior)) (:result prior)
         :else
         (let [generation (random-uuid) result (promise-chan) drained (promise-chan)
               op {:generation generation :epoch epoch :result result :phase :pending}]
           (retire-initial!)
           (when-let [old-result (:result prior)] (put! old-result (cancelled)))
           (swap! state assoc :operation op)
           ;; Bound the caller, but never release the physical operation gate
           ;; while an asynchronous subscription constructor is still running.
           ;; Otherwise it could send its late topic-only request after a newer
           ;; owner subscribed, corrupting that owner's wire stream.
           (go-try-
            (let [[_ port] (alts! [result (timeout handshake-timeout-ms)])]
              (when-not (= port result)
                (locked controller
                        #(when (and (current? controller generation)
                                    (= :pending (get-in @state [:operation :phase])))
                           (let [error (ex-info "Replica synchronization did not complete in time"
                                                {:type :kabel-handshake-timeout
                                                 :timeout-ms handshake-timeout-ms})]
                             (swap! state update :operation assoc :phase :error :error error)
                             (put! result error)))))))
           (go-try-
            (try
              (<?- gate)
              (try
                (assert-current! controller generation)
                (<?- (release-owned! controller handshake-timeout-ms))
                (assert-current! controller generation)
                (let [strategy (sync/store-sync-strategy
                                store {:walk-fn walk-fn
                                       :on-key-update
                                       (fn [key head _op]
                                         (when (= branch key)
                                           (locked controller
                                                   #(when (current? controller generation)
                                                      (swap! state assoc-in [:operation :head] head)
                                                      (when (= :ready (get-in @state [:operation :phase]))
                                                        (publish! head))))))})
                      sub-ch (pubsub/subscribe!
                              peer #{topic} {:strategies {topic strategy}
                                             :on-handshake-complete
                                             #(when (= topic %) (put! drained true))})
                      actual (:strategy (pubsub/subscription peer topic))
                      _ (when (identical? actual strategy) (swap! state assoc :owned strategy))
                      sub (<?- sub-ch)]
                  ;; Even a superseded subscribe may have physically installed
                  ;; its strategy. Record ownership before the next gate holder
                  ;; attempts cleanup. Never unsubscribe an unrelated owner.
                  (when-let [error (:error sub)]
                    (throw (ex-info "Replica subscription refused"
                                    {:type :datahike.kabel/subscribe-failed :error error})))
                  (when-not sub
                    (throw (ex-info "Replica subscription did not start"
                                    {:type :datahike.kabel/subscribe-failed}))))
                (finally (put! gate true)))
              (<?- (bounded! drained handshake-timeout-ms result))
              (locked controller
                      #(do
                         (assert-current! controller generation)
                         (let [head (get-in @state [:operation :head])]
                           (when-not head
                             (throw (ex-info "Replica handshake supplied no branch head"
                                             {:type :kabel-no-head :topic topic :branch branch})))
                           (publish! head)
                           ;; DB watches can synchronously close or supersede
                           ;; this owner while publishing its canonical head.
                           (assert-current! controller generation)
                           (swap! state assoc-in [:operation :phase] :ready)
                           (put! result {:status :ready :epoch epoch}))))
              (catch #?(:clj Throwable :cljs :default) error
                (locked controller
                        #(when (current? controller generation)
                           (swap! state update :operation assoc :phase :error :error error)))
                (put! result error))))
           result))))))

(defn close!
  "Fence synchronously, share in-flight/successful cleanup, and retry failures.
   A completed old close cannot later retire a replacement topic owner."
  [{:keys [state gate retire-initial!] :as controller}]
  (locked
   controller
   (fn []
     (retire-initial!)
     (swap! state assoc :closed? true)
     (when-let [result (get-in @state [:operation :result])] (put! result (cancelled)))
     (if (#{:pending :closed} (get-in @state [:close :phase]))
       (get-in @state [:close :result])
       (let [result (promise-chan)]
         (swap! state assoc :close {:phase :pending :result result})
         (go-try-
          (<?- gate)
          (try
            (<?- (release-owned! controller 30000))
            (locked controller #(swap! state assoc-in [:close :phase] :closed))
            (put! result true)
            (catch #?(:clj Throwable :cljs :default) error
              (locked controller #(swap! state assoc-in [:close :phase] :error))
              (put! result error))
            (finally (put! gate true))))
         result)))))
