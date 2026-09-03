(ns datahike.js.kabel
  "JavaScript boundary for the opt-in Kabel browser bundle."
  (:require [datahike.js.api :as api]
            [datahike.kabel.cbor-handlers :refer [datahike-cbor-middleware]]
            [datahike.kabel.connector]
            [datahike.kabel.writer]
            [is.simm.distributed-scope :as ds]
            [kabel.auth.websocket :as auth]
            [kabel.peer :as peer]
            [konserve-sync.core :as sync]
            [konserve.indexeddb]
            [superv.async :refer [S]]))

(defn- peer-middleware [options]
  (let [token (:token options)
        ;; Accept idiomatic JavaScript camelCase while retaining the literal
        ;; CLJS option names for callers that construct config-shaped objects.
        on-auth (or (:onAuth options) (:on-auth options))
        on-error (or (:onError options) (:on-error options))
        auth-options (when token
                       {:authenticate {:token token
                                       :on-auth on-auth
                                       :on-error on-error}
                        ;; Browser clients authenticate to the server. They do
                        ;; not accept incoming transport connections themselves.
                        :permissive true})]
    (if auth-options
      (comp ds/remote-middleware
            (sync/client-middleware)
            (auth/auth-middleware auth-options))
      (comp ds/remote-middleware
            (sync/client-middleware)))))

(defn ^:export createKabelPeer
  "Create a Kabel client peer and start its remote invocation loop.

  clientId is a value returned by datahike.uuid() or datahike.randomUuid().
  options may contain token, onAuth, and onError. The returned peer is an
  opaque value intended for writer.local-peer in a Datahike config."
  ([client-id]
   (createKabelPeer client-id nil))
  ([client-id js-options]
   (let [options (or (api/js->clj-recursive js-options) {})
         peer-atom (peer/client-peer S
                                     client-id
                                     (peer-middleware options)
                                     datahike-cbor-middleware)]
     (ds/invoke-on-peer peer-atom)
     peer-atom)))

(defn ^:export connectKabelPeer
  "Connect a Kabel client peer to a WebSocket URL."
  [peer-atom url]
  (api/maybe-chan->promise
   (ds/connect-distributed-scope S peer-atom url)))

(defn ^:export stopKabelPeer
  "Stop a Kabel client peer and release its transport resources."
  [peer-atom]
  (api/maybe-chan->promise (peer/stop peer-atom)))
