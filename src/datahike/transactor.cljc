(ns datahike.transactor
  (:require [superv.async :refer [<?- S go-loop-super go-try- >?]]
            [taoensso.timbre :as log]
            [clojure.core.async :refer [>! chan close! promise-chan put!]]))

(defprotocol PTransactor
  ; Send a transaction. Returns a channel that resolves when the transaction finalizes.
  (send-transaction! [_ tx-data tx-fn])
  ; Returns a channel that resolves when the transactor has shut down.
  (shutdown [_])
  (streaming? [_]))

(defrecord LocalTransactor
           [rx-queue rx-loop streaming?]
  PTransactor
  (send-transaction! [_ tx-data tx-fn]
    (let [p (promise-chan)]
      (go-try-
       (>? S rx-queue {:tx-data tx-data :report-ch p :tx-fn tx-fn})
       (<?- p))))

  (shutdown [_]
    (close! rx-queue)
    rx-loop)
  (streaming? [_] streaming?))

(defn create-rx-loop
  [connection rx-queue update-and-flush-db]
  (let [resolve-fn (memoize resolve)]
    (go-loop-super S []
                   (if-let [{:keys [tx-data report-ch tx-fn]} (<?- rx-queue)]
                     (do
                       (let [update-fn (resolve-fn tx-fn)
                             tx-report (try (<?- (update-and-flush-db connection tx-data update-fn))
                                 ; Only catch ExceptionInfo here (intentionally rejected transactions).
                                 ; Any other exceptions should crash the transactor and signal the supervisor.
                                            (catch clojure.lang.ExceptionInfo e
                                              (log/debug "Transaction rejected." tx-data)))]
                         (when (some? report-ch)
                           (put! report-ch tx-report)))
                       (recur))
                     (do
                       (log/debug "Transactor rx thread gracefully closed"))))))

(defmulti create-transactor
  (fn [transactor-config conn update-and-flush-db]
    (or (:backend transactor-config) :local)))

(defmethod create-transactor :local
  [{:keys [rx-buffer-size streaming?]} connection update-and-flush-db]
  (let [rx-queue (chan rx-buffer-size)
        rx-loop (create-rx-loop connection rx-queue update-and-flush-db)]
    (map->LocalTransactor
     {:rx-queue rx-queue
      :rx-loop rx-loop
      :streaming? (if (nil? streaming?) true streaming?)})))

