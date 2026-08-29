(ns datahike.http.writer
  "Remote writer implementation for datahike.http.server through datahike.http.client."
  (:require [datahike.writer :refer [PWriter create-writer create-database delete-database]]
            [datahike.connections :refer [invalidate-store-connections!]]
            [datahike.http.client :refer [request-cbor] :as client]
            [datahike.connector :as connector]
            [datahike.store :as ds]
            [datahike.tools :as dt :refer [throwable-promise]]
            [replikativ.logging :as log]
            [clojure.core.async :as async :refer [promise-chan put!]]))

(defrecord DatahikeServerWriter [remote-peer conn]
  PWriter
  (-dispatch! [_ arg-map]
    (let [{:keys [op args]} arg-map
          p (promise-chan)
          config (:config @(:wrapped-atom conn))]
      (log/debug :datahike/http-write-op {:op op})
      ;; On a real thread: `-dispatch!` is called from inside the writer's go
      ;; block, and a blocking HTTP call there occupies a core.async dispatch
      ;; thread for the round trip — enough concurrent transactions starve the
      ;; pool, and a server in the same JVM (tests, embedded hosts) deadlocks.
      (async/thread
        (put! p
              (try
              ;; The server implements one writer operation. Say so here, with
              ;; the operation named, rather than let it become a 404.
                (when-not (= "transact!" (name op))
                  (throw (ex-info (str op " is not available over the :datahike-server writer; "
                                       "only transact! is. Run it where the writer runs, or use a :self writer.")
                                  {:type :remote-writer-unsupported-op :op op})))
              ;; CBOR, not JSON. This channel is machine-to-machine only —
              ;; never a browser, never read by a human — so JSON's
              ;; readability buys nothing here while its costs all apply: the
              ;; `!kw`/`!date` tagged-string encoding, the re-entrant
              ;; `write-to-generator`, and the server-side schema lookup that
              ;; re-infers types JSON could not carry. CBOR carries them.
              ;; `:writer` holds the token; the server strips it anyway.
                (request-cbor :post
                              (str op "-writer")
                              remote-peer
                              (vec (concat [(dissoc config :writer)] args)))
                ;; Throwable: an Error on this thread must still deliver, or
                ;; the caller waits on the promise forever.
                (catch Throwable e
                  e))))
      p))
  (-shutdown [_])
  (-streaming? [_] false))

(defmethod create-writer :datahike-server
  [config connection]
  (log/debug :datahike/http-writer-create {:config (dissoc config :token)})
  (->DatahikeServerWriter config connection))

(defmethod create-database :datahike-server
  [& args]
  (let [p (throwable-promise)
        {:keys [writer] :as config} (first args)]
    ;; redirect call to remote-peer as writer config
    (deliver p (try (->
                     (request-cbor :post
                                   "create-database-writer"
                                   writer
                                   (vec (concat [(-> config
                                                     (assoc :remote-peer (dissoc writer :token))
                                                     (dissoc :writer))]
                                                (rest args))))
                     (dissoc :remote-peer))
                    (catch Exception e
                      e)))
    p))

(defmethod delete-database :datahike-server
  [& args]
  (let [p (throwable-promise)
        {:keys [writer] :as config} (first args)]
    ;; redirect call to remote-peer as writer config
    (let [result (try
                   (-> (request-cbor :post
                                     "delete-database-writer"
                                     writer
                                     (vec (concat [(-> config
                                                       (assoc :remote-peer (dissoc writer :token))
                                                       (dissoc :writer))]
                                                  (rest args))))
                       (dissoc :remote-peer))
                   (catch Exception e
                     e))]
      (when-not (instance? Throwable result)
        (invalidate-store-connections! (ds/store-identity (:store config))))
      (deliver p result))
    p))

;; =============================================================================
;; Connection Integration
;; =============================================================================

(defmethod connector/-connect* :datahike-server [config opts]
  ;; HTTP uses standard connection logic with async+sync behavior
  (connector/-connect-impl* config opts))
