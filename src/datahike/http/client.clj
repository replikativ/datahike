(ns datahike.http.client
  (:refer-clojure :exclude [filter])
  (:require [babashka.http-client :as http]
            [cognitect.transit :as transit]
            [jsonista.core :as j]
            [hasch.core :refer [uuid]]
            [datahike.api.specification :as api]
            [clojure.edn :as edn]
            [datahike.datom :as dd]
            [boring.core :as boring]
            [datahike.remote :as remote]
            [datahike.remote.cbor :as rcbor]
            [datahike.impl.entity :as de]
            [replikativ.logging :as log])
  (:import [java.io ByteArrayOutputStream]))

(def MEGABYTE (* 1024 1024))

(def MAX_OUTPUT_BUFFER_SIZE (* 4 MEGABYTE))

(defn request-edn [method end-point remote-peer data]
  (let [{:keys [url token]} remote-peer
        fmt                 "application/edn"
        url                 (str url "/" end-point)
        body                (remote/edn-replace-remote-literals (pr-str data))
        _                   (log/trace :datahike/http-request {:url url :end-point end-point :data data})
        response
        (try
          (http/request (merge
                         {:uri     url
                          :method  method
                          :headers (merge {:content-type fmt
                                           :accept       fmt}
                                          (when token
                                            {:authorization (str "token " token)}))
                          :body    body}
                         (when (= method :get)
                           {:query-params {"args-id" (uuid data)}})))
          (catch Exception e
            (let [msg  (ex-message e)
                  data (ex-data e)
                  new-data
                  (update data :body #(edn/read-string {:readers remote/edn-readers} %))]
              (throw (ex-info msg new-data)))))
        response            (:body response)]
    (log/trace :datahike/http-response {:response response})
    (edn/read-string {:readers remote/edn-readers} response)))

(defn request-transit
  ([method end-point remote-peer data]
   (request-transit method end-point remote-peer data
                    remote/transit-read-handlers
                    remote/transit-write-handlers))
  ([method end-point remote-peer data read-handlers write-handlers]
   (let [{:keys [url token max-output-buffer-size]}
         remote-peer
         fmt      "application/transit+json"
         url      (str url "/" end-point)
         out      (ByteArrayOutputStream. (or max-output-buffer-size MAX_OUTPUT_BUFFER_SIZE))
         writer   (transit/writer out :json {:handlers write-handlers})
         _        (transit/write writer data)
         _        (log/trace :datahike/http-request {:url url :end-point end-point :data data})
         response
         (try
           (http/request (merge
                          {:method method
                           :uri    url
                           :headers
                           (merge {:content-type fmt
                                   :accept       fmt}
                                  (when token
                                    {:authorization (str "token " token)}))
                           :as     :stream
                           :body   (.toByteArray out)}
                          (when (= method :get)
                            {:query-params {"args-id" (uuid data)}})))
           (catch Exception e
             ;; read exception
             (let [msg  (ex-message e)
                   data (ex-data e)
                   new-data
                   (update data :body
                           #(when %
                              (transit/read (transit/reader % :json {:handlers read-handlers}))))]
               (throw (ex-info msg new-data)))))
         response (:body response)
         response (transit/read (transit/reader response :json {:handlers read-handlers}))]
     (log/trace :datahike/http-response {:response response})
     response)))

;; One registry per process; immutable once built. Private: its server twin
;; is private, it is a delay whose deref-and-rebind would be a mutation of
;; process-wide wire behaviour, and publishing it would commit us to it staying
;; a delay.
(def ^:private cbor-registry (delay (rcbor/client-registry)))

(defn- decode-error-body
  "An error body is only CBOR if the error came from datahike. A proxy, a load
  balancer or a gateway answers with HTML or plain text, and feeding that to a
  decoder replaces the real HTTP failure with a parse failure — which is how a
  502 turns into an unreadable bug report. Anything that does not decode is
  handed back untouched."
  [body registry]
  (when body
    (try
      (if (bytes? body)
        (boring/decode body (rcbor/decode-opts registry))
        body)
      (catch Exception _ body))))

(defn request-cbor
  "`application/cbor`, via `datahike.remote.cbor`.

  Unlike the JSON path this needs no server-side help: CBOR carries keywords,
  symbols, sets, instants, UUIDs and ratios as tags, so the schema-driven
  re-inference in `datahike.json` (`xf-data-for-tx`) and the EDN-string
  smuggling in `support-embedded-edn-in-json` are simply not reached — that
  middleware is gated on `content-type = application/json`."
  ([method end-point remote-peer data]
   (request-cbor method end-point remote-peer data @cbor-registry))
  ([method end-point remote-peer data registry]
   (let [{:keys [url token]} remote-peer
         fmt      "application/cbor"
         url      (str url "/" end-point)
         out      (boring/encode data (rcbor/encode-opts registry))
         _        (log/trace :datahike/http-request {:url url :end-point end-point})
         response
         (try
           (http/request (merge
                          {:method  method
                           :uri     url
                           :headers (merge {:content-type fmt
                                            :accept       fmt}
                                           (when token
                                             {:authorization (str "token " token)}))
                           :as      :bytes
                           :body    out}
                          (when (= method :get)
                            {:query-params {"args-id" (uuid data)}})))
           (catch Exception e
             ;; A datahike error body is CBOR, decoded with the same registry so
             ;; ex-data carrying a Datom or a DB handle survives the trip.
             (let [msg  (if-let [m (ex-message e)] m "Nothing returned. Is the server reachable?")
                   data (ex-data e)
                   new-data (update data :body decode-error-body registry)]
               (throw (ex-info (or (:msg (:body new-data)) msg)
                               (or (:ex-data (:body new-data)) new-data))))))
         ;; Bound around decoding, as the generated client fns do: a database
         ;; handle in the response carries this peer, and without it the
         ;; writer's `:db-after` came back unable to make a remote call.
         response (binding [remote/*remote-peer* remote-peer]
                    (boring/decode (:body response) (rcbor/decode-opts registry)))]
     (log/trace :datahike/http-response {:response response})
     response)))

(defn request-json
  ([method end-point remote-peer data]
   (request-json method end-point remote-peer data remote/json-mapper))
  ([method end-point remote-peer data mapper]
   (let [{:keys [url token]}
         remote-peer
         fmt      "application/json"
         url      (str url "/" end-point)
         out      (j/write-value-as-bytes data mapper)
         _        (log/trace :datahike/http-request {:url url :end-point end-point :data data})
         response
         (try
           (http/request (merge
                          {:method method
                           :uri    url
                           :headers
                           (merge {:content-type fmt
                                   :accept       fmt}
                                  (when token
                                    {:authorization (str "token " token)}))
                           :as     :stream
                           :body   out}
                          (when (= method :get)
                            {:query-params {"args-id" (uuid data)}})))
           (catch Exception e
             ;; read exception
             (let [msg  (if-let [m (ex-message e)] m "Nothing returned. Is the server reachable?")
                   data (ex-data e)
                   new-data
                   (update data :body
                           #(when %
                              (j/read-value % mapper)))]
               (throw (ex-info (or (:msg (:body new-data)) msg)
                               (or (:ex-data (:body new-data)) new-data))))))
         response (:body response)
         response (j/read-value response mapper)]
     (log/trace :datahike/http-response {:response response})
     response)))

(defn request-json-raw [method end-point remote-peer data]
  (let [{:keys [url token]}
        remote-peer
        fmt      "application/json"
        url      (str url "/" end-point)
        out      data
        _        (log/trace :datahike/http-request {:url url :end-point end-point :data data})
        response
        (http/request (merge
                       {:method method
                        :uri    url
                        :headers
                        (merge {:content-type fmt
                                :accept       fmt}
                               (when token
                                 {:authorization (str "token " token)}))
                        :as     :stream
                        :body   out}
                       (when (= method :get)
                         {:query-params {"args-id" (uuid data)}})))
        response (slurp (:body response))]
    (log/trace :datahike/http-response {:response response})
    response))

(defn get-remote [args]
  (let [remotes (disj
                 (into
                  ;; first arguments can be config maps, e.g. for
                  ;; create-database; this code could be made explicit by
                  ;; explicitly dispatching on when the first argument is a
                  ;; config map
                  #{(:remote-peer (first args))}
                  ;; other arguments to API follow hygiene
                  (map remote/remote-peer (rest args)))
                 nil)]
    (if (> (count remotes) 1)
      (throw (ex-info "Arguments refer to more than one remote-peer." {:remotes remotes
                                                                       :args args}))
      (first remotes))))

(doseq [[n {:keys [args doc supports-remote? referentially-transparent?]}] api/api-specification]
  (eval
   `(def
      ~(with-meta n
         {:arglists `(api/malli-schema->argslist (quote ~args))
          :doc      doc})
      (fn [& ~'args]
        ~(if-not supports-remote?
           `(throw (ex-info (str ~(str n) " is not supported for remote connections.")
                            {:type     :remote-not-supported
                             :function ~(str n)}))
           `(binding [remote/*remote-peer* (get-remote ~'args)]
              (let [format# (:format remote/*remote-peer*)]
                (({:transit request-transit
                   :edn     request-edn
                   :json    request-json
                   :cbor    request-cbor} (or format# :transit))
                 ~(if referentially-transparent? :get :post)
                 ~(api/->url n)
                 remote/*remote-peer* (vec ~'args)))))))))

(defmethod remote/remote-deref :datahike-server [conn] (db conn))
