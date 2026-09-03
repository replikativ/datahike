(ns datahike.js.kabel
  "JavaScript boundary for the opt-in Kabel browser bundle.

   Isomorphic to the ClojureScript API: a peer is constructed explicitly, the
   remote-invocation and sync middleware ride on it, and the same peer is what
   a `:kabel` writer sends through."
  (:require [datahike.js.api :as api]
            [datahike.kabel.cbor-handlers :refer [datahike-cbor-middleware]]
            [datahike.kabel.connector]
            [datahike.kabel.writer]
            [kabel.auth.websocket :as auth]
            [kabel.peer :as peer]
            [kabel.remote :as remote]
            [konserve-sync.core :as sync]
            [konserve.indexeddb]
            [clojure.core.async :as async :refer [promise-chan put! close! go]]
            [superv.async :refer [S] :refer-macros [<?]]))

(defn- peer-middleware [options]
  (let [token (:token options)
        ;; Accept idiomatic JavaScript camelCase while retaining the literal
        ;; CLJS option names for callers that construct config-shaped objects.
        on-auth (or (:onAuth options) (:on-auth options))
        on-error (or (:onError options) (:on-error options))
        auth-options (when token
                       ;; A function, or a promise-returning function, is read
                       ;; at every connection and for refreshes; see
                       ;; kabel.auth.websocket/authenticate-middleware.
                       {:authenticate {:token token
                                       :on-auth on-auth
                                       :on-error on-error}
                        ;; Browser clients authenticate to the server. They do
                        ;; not accept incoming transport connections themselves.
                        :permissive true})]
    (if auth-options
      (comp remote/middleware
            (sync/client-middleware)
            (auth/auth-middleware auth-options))
      (comp remote/middleware
            (sync/client-middleware)))))

(defn ^:export createKabelPeer
  "Create a Kabel client peer and start serving remote functions on it.

  clientId is a value returned by datahike.uuid() or datahike.randomUuid().
  options may contain token (a string, or a function returning a string or a
  promise of one), onAuth, and onError. The returned peer is an opaque value
  intended for writer.local-peer in a Datahike config."
  ([client-id]
   (createKabelPeer client-id nil))
  ([client-id js-options]
   (let [options (or (api/js->clj-recursive js-options) {})
         peer-atom (peer/client-peer S
                                     client-id
                                     (peer-middleware options)
                                     datahike-cbor-middleware)]
     (remote/serve peer-atom)
     peer-atom)))

(defn ^:export connectKabelPeer
  "Connect a Kabel client peer to a WebSocket URL once. Resolves with the
  server's peer id when remote invocations work."
  [peer-atom url]
  (api/maybe-chan->promise
   (remote/connect S peer-atom url)))

(defn ^:export maintainKabelPeer
  "Keep a Kabel client peer connected to a WebSocket URL: reconnect with
  backoff when the connection drops, and report every transition to
  options.onStatus as {status, attempt, error, ...}. Returns {stop, done}."
  ([peer-atom url]
   (maintainKabelPeer peer-atom url nil))
  ([peer-atom url js-options]
   (let [options (or (api/js->clj-recursive js-options) {})
         on-status (or (:onStatus options) (:on-status options))
         handle (peer/maintain S peer-atom url
                               {:on-status (when on-status
                                             (fn [status] (on-status (api/clj->js-recursive status))))
                                :backoff (:backoff options)
                                :max-attempts (or (:maxAttempts options) (:max-attempts options))})]
     #js {:stop (:stop! handle)
          :done (api/maybe-chan->promise (:done handle))})))

(defn- settle
  "A promise of the JavaScript-facing outcome of `ch`: a rejection for an
   exception, the converted value otherwise. Taken with `<?` inside a go
   block, so nil never travels through a channel and a yielded exception is
   released from the supervisor."
  [ch]
  (js/Promise.
   (fn [resolve reject]
     (go
       (try
         (resolve (api/clj->js-recursive (<? S ch)))
         (catch :default e (reject e)))))))

(defn ^:export refreshKabelToken
  "Send a new token on the peer's live connection. Without a token the
  configured token source is read again. Resolves with the accepted principal."
  ([peer-atom] (refreshKabelToken peer-atom nil))
  ([peer-atom token]
   ;; refresh-token! needs a live authenticated connection; without one it
   ;; throws right away, and a caller expects a rejection, not a throw
   (try
     (settle (auth/refresh-token! peer-atom token))
     (catch :default e (js/Promise.reject e)))))

(defn ^:export invokeRemote
  "Invoke the function fnName ('namespace/name') on the peer remoteId with an
  argument object, through this peer. Resolves with the result."
  ([peer-atom remote-id fn-name]
   (invokeRemote peer-atom remote-id fn-name nil))
  ([peer-atom remote-id fn-name js-args]
   (api/maybe-chan->promise
    (settle (remote/invoke peer-atom remote-id (symbol fn-name)
                           (or (api/js->clj-recursive js-args) {}))))))

(defn ^:export registerRemoteFn
  "Serve fn on this process under fnName ('namespace/name'). fn receives the
  argument object, with the caller's principal under 'kabel/principal' when
  the connection is authenticated, and may return a value or a promise."
  [fn-name f]
  (remote/register! (symbol fn-name)
                    (fn [arg-map]
                      (let [ch (promise-chan)
                            ;; nil cannot travel through a channel: a closed
                            ;; promise-chan yields nil to the taker instead
                            settle! (fn [v] (if (nil? v) (close! ch) (put! ch v)))
                            result (try (f (api/clj->js-recursive arg-map))
                                        (catch :default e e))]
                        (if (instance? js/Promise result)
                          (.then result
                                 (fn [v] (settle! (api/js->clj-recursive v)))
                                 (fn [e] (settle! (if (instance? js/Error e) e (ex-info (str e) {:error e})))))
                          (settle! (if (instance? js/Error result) result (api/js->clj-recursive result))))
                        ch)))
  fn-name)

(defn ^:export unregisterRemoteFn
  [fn-name]
  (remote/unregister! (symbol fn-name))
  nil)

(defn ^:export stopKabelPeer
  "Stop a Kabel client peer and release its transport resources."
  [peer-atom]
  (api/maybe-chan->promise (peer/stop peer-atom)))
