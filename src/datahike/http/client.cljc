(ns datahike.http.client
  "The thin client of the Datahike HTTP server: every API function as a call
   to the server's route of the same name, with connections, databases and
   entities travelling as handles (`datahike.remote`).

   On the JVM the functions are synchronous and speak EDN, transit, JSON or
   CBOR as the peer's `:format` asks. On ClojureScript they speak CBOR over
   `fetch` and each returns a promise-channel delivering the result or the
   exception. A read goes as a GET with its arguments in the URL while they
   fit (`url-args-limit`), which is what lets an HTTP cache serve it; larger
   arguments go by POST."
  (:refer-clojure :exclude [filter])
  (:require #?@(:clj [[babashka.http-client :as http]
                      [cognitect.transit :as transit]
                      [jsonista.core :as j]
                      [hasch.core :refer [uuid]]
                      [clojure.edn :as edn]
                      [datahike.datom :as dd]
                      [datahike.impl.entity :as de]]
                :cljs [[clojure.core.async :as async]])
            ;; The specification drives the JVM's generated functions at load
            ;; and ClojureScript's at macro time; a browser bundle carries
            ;; none of it.
            #?(:clj [datahike.api.specification :as api])
            [boring.core :as boring]
            [datahike.remote :as remote]
            [datahike.remote.cbor :as rcbor]
            [replikativ.logging :as log])
  #?(:clj (:import [java.io ByteArrayOutputStream])
     :cljs (:require-macros [datahike.http.client :refer [emit-remote-api]])))

(def MEGABYTE (* 1024 1024))

(def MAX_OUTPUT_BUFFER_SIZE (* 4 MEGABYTE))

(def ^:private cbor-registry (delay (rcbor/client-registry)))

#?(:clj
   (do
     (defn request-edn [method end-point remote-peer data]
       (let [{:keys [url token]} remote-peer
             fmt                 "application/edn"
             url                 (str url "/" end-point)
             body                (remote/edn-replace-remote-literals (pr-str data))
             _                   (log/trace :datahike/http-request {:url url :end-point end-point})
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
         (log/trace :datahike/http-response {:end-point end-point})
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
              _        (log/trace :datahike/http-request {:url url :end-point end-point})
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
          (log/trace :datahike/http-response {:end-point end-point})
          response)))

;; One registry per process; immutable once built. Private: its server twin
;; is private, it is a delay whose deref-and-rebind would be a mutation of
;; process-wide wire behaviour, and publishing it would commit us to it staying
;; a delay.
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
          (log/trace :datahike/http-response {:end-point end-point})
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
              _        (log/trace :datahike/http-request {:url url :end-point end-point})
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
          (log/trace :datahike/http-response {:end-point end-point})
          response)))

     (defn request-json-raw [method end-point remote-peer data]
       (let [{:keys [url token]}
             remote-peer
             fmt      "application/json"
             url      (str url "/" end-point)
             out      data
             _        (log/trace :datahike/http-request {:url url :end-point end-point})
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
         (log/trace :datahike/http-response {:end-point end-point})
         response))))

#?(:cljs
   (def url-args-limit
     "Encoded arguments up to this many bytes travel in a GET's URL; beyond it
      the call is a POST. Well under every intermediary's line limit."
     2048))

#?(:cljs
   (defn- base64url
     "base64url without padding, of a Uint8Array."
     [^js bytes]
     (let [chars (js/Array.)]
       (dotimes [i (.-length bytes)]
         (.push chars (js/String.fromCharCode (aget bytes i))))
       (-> (js/btoa (.join chars ""))
           (.replace (js/RegExp. "\\+" "g") "-")
           (.replace (js/RegExp. "/" "g") "_")
           (.replace (js/RegExp. "=+$") "")))))

#?(:cljs
   (defn- error-from-response
     "The exception a failed response stands for. A Datahike error body is
      CBOR carrying `:msg` and `:ex-data`; anything else (a proxy's HTML, an
      empty body) is reported as the HTTP failure it is."
     [status target bytes registry]
     (let [decoded (try (boring/decode bytes (rcbor/decode-opts registry))
                        (catch :default _ nil))]
       (if (and (map? decoded) (:msg decoded))
         (ex-info (:msg decoded) (assoc (or (:ex-data decoded) {}) :status status :url target))
         (ex-info (str "HTTP " status " from " target)
                  {:type :datahike.http/request-failed :status status :url target})))))

#?(:cljs
   (defn request-cbor
     "`application/cbor` over `fetch`. Yields a promise-channel that delivers
      the decoded result, closes for a nil result, or delivers the exception.
      A `:get` with arguments within `url-args-limit` is sent as a GET with
      `?args=<base64url>&f=cbor`, so an HTTP cache can answer it; otherwise,
      and for every `:post`, the arguments are the body."
     ([method end-point remote-peer data]
      (request-cbor method end-point remote-peer data @cbor-registry))
     ([method end-point remote-peer data registry]
      (let [{:keys [url token]} remote-peer
            fmt      "application/cbor"
            body     (boring/encode data (rcbor/encode-opts registry))
            as-get?  (and (= method :get) (<= (.-length body) url-args-limit))
            target   (if as-get?
                       (str url "/" end-point "?args=" (base64url body) "&f=cbor")
                       (str url "/" end-point))
            headers  (cond-> {"accept" fmt}
                       (not as-get?) (assoc "content-type" fmt)
                       token (assoc "authorization" (str "token " token)))
            init     (cond-> {:method (if as-get? "GET" "POST") :headers headers}
                       (not as-get?) (assoc :body body))
            out      (async/promise-chan)
            deliver! (fn [v] (if (nil? v) (async/close! out) (async/put! out v)))
            ;; Bound around decoding, as on the JVM: a database handle in the
            ;; response carries this peer, and without it the handle could not
            ;; make the next call.
            decode   (fn [bytes] (binding [remote/*remote-peer* remote-peer]
                                   (boring/decode bytes (rcbor/decode-opts registry))))]
        (log/trace :datahike/http-request {:url url :end-point end-point :method (:method init)})
        (-> (js/fetch target (clj->js init))
            (.then (fn [^js resp]
                     (.then (.arrayBuffer resp)
                            (fn [buf]
                              (let [bytes (js/Uint8Array. buf)]
                                (log/trace :datahike/http-response {:end-point end-point})
                                (deliver! (if (.-ok resp)
                                            (try (decode bytes)
                                                 (catch :default e
                                                   (ex-info (str "Undecodable response from " target ": " (ex-message e))
                                                            {:type :datahike.http/bad-response :url target})))
                                            (error-from-response (.-status resp) target bytes registry))))))))
            (.catch (fn [e]
                      (deliver! (ex-info (str "Request to " target " failed: " (or (some-> e .-message) (str e)))
                                         {:type :datahike.http/request-failed :url target})))))
        out))))

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

(defn- without-credentials
  "The arguments as sent: a config's `:remote-peer` and `:writer` name how to
   reach a server, and their tokens belong in the authorization header, not
   the body."
  [args]
  (mapv (fn [a]
          (if (map? a)
            (cond-> a
              (map? (:remote-peer a)) (update :remote-peer dissoc :token)
              (map? (:writer a))      (update :writer dissoc :token))
            a))
        args))

;; `db` and its siblings are defined by the `eval` below; the declaration is
;; for readers of the source, human and linter alike.
#?(:clj (declare db))

#?(:clj
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
                 (let [format# (:format remote/*remote-peer*)
                       ~'result (({:transit request-transit
                                   :edn     request-edn
                                   :json    request-json
                                   :cbor    request-cbor} (or format# :cbor))
                                 ~(if referentially-transparent? :get :post)
                                 ~(api/->url n)
                                 remote/*remote-peer* (without-credentials (vec ~'args)))]
                ;; The server echoes the config's :remote-peer as it received
                ;; it — without the token. The caller's own peer goes back on.
                   ~(if (= n 'create-database)
                      `(cond-> ~'result (map? ~'result) (assoc :remote-peer remote/*remote-peer*))
                      'result)))))))))

#?(:cljs
   (defn remote-call
     "One API call on ClojureScript: find the peer among `args`, send them to
      `end-point` (`method` is `:get` for a read), and for `create-database`
      put the caller's peer back on the configuration the server echoes
      without its token."
     [method end-point args create?]
     (let [remote-peer (get-remote args)
           result (request-cbor method end-point remote-peer (without-credentials (vec args)))]
       (if create?
         (let [out (async/promise-chan)]
           (async/take! result
                        (fn [v]
                          (cond (nil? v) (async/close! out)
                                (map? v) (async/put! out (assoc v :remote-peer remote-peer))
                                :else (async/put! out v))))
           out)
         result))))

#?(:clj
   (defmacro emit-remote-api
     "Define the API functions for ClojureScript, one per specification entry:
      the remote-capable ones call the server, the rest throw as on the JVM."
     []
     `(do
        ~@(for [[n {:keys [args doc supports-remote? referentially-transparent?]}] api/api-specification]
            `(defn ~(with-meta n {:arglists (list 'quote (api/malli-schema->argslist args)) :doc doc})
               [& ~'args]
               ~(if supports-remote?
                  `(remote-call ~(if referentially-transparent? :get :post) ~(api/->url n) ~'args ~(= n 'create-database))
                  `(throw (ex-info (str ~(str n) " is not supported for remote connections.")
                                   {:type :remote-not-supported :function ~(str n)}))))))))

#?(:cljs (emit-remote-api))

#?(:clj (defmethod remote/remote-deref :datahike-server [conn] (db conn)))
