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
  (:require [clojure.string :as str]
            #?@(:clj [[babashka.http-client :as http]
                      [cognitect.transit :as transit]
                      [jsonista.core :as j]
                      [hasch.core :refer [uuid]]
                      [clojure.edn :as edn]
                      [datahike.datom :as dd]
                      [datahike.impl.entity :as de]]
                :cljs [[clojure.core.async :as async]
                       [datahike.datom :as dd]])
            ;; The specification drives the JVM's generated functions at load
            ;; and ClojureScript's at macro time; a browser bundle carries
            ;; none of it.
            #?(:clj [datahike.api.specification :as api])
            [boring.core :as boring]
            [datahike.remote :as remote]
            [datahike.remote.cbor :as rcbor]
            ;; The logger and its printer stay out of the browser bundle; a
            ;; request is traced with the console there.
            #?(:clj [replikativ.logging :as log]))
  #?(:clj (:import [java.io BufferedReader ByteArrayOutputStream InputStreamReader]
                   [java.lang Thread])
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
   (defn- trace
     "`console.debug` when the page asked for it (`datahike.http.client.trace = true`)."
     [event data]
     (when (some-> js/globalThis .-datahike_http_client_trace)
       (js/console.debug (str event) (clj->js data)))))

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
        (trace :datahike/http-request {:url url :end-point end-point :method (:method init)})
        (-> (js/fetch target (clj->js init))
            (.then (fn [^js resp]
                     (.then (.arrayBuffer resp)
                            (fn [buf]
                              (let [bytes (js/Uint8Array. buf)]
                                (trace :datahike/http-response {:end-point end-point})
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
   (do
     (defonce ^:private listeners (atom {}))

     (defn- listener-id [conn key]
       [(get-in conn [:remote-peer :url]) (:store-id conn) key])

     (defn- active-listener? [{:keys [id stopped?] :as listener}]
       (and (not @stopped?) (identical? listener (get @listeners id))))

     (defn- close-listen-stream! [{:keys [stream]}]
       (when-let [body @stream]
         (reset! stream nil)
         (try
           (.close ^java.io.Closeable body)
           (catch Exception _))))

     (defn- stop-listener! [{:keys [id stopped? thread] :as listener}]
       (reset! stopped? true)
       (close-listen-stream! listener)
       (when-let [worker @thread]
         (when-not (identical? worker (Thread/currentThread))
           (.interrupt ^Thread worker)))
       (swap! listeners #(if (identical? listener (get % id)) (dissoc % id) %)))

     (defn- remote-db-from-head [conn head]
       (binding [remote/*remote-peer* (:remote-peer conn)]
         (remote/remote-db
          (assoc (select-keys head [:max-tx :max-eid :commit-id])
                 :store-id (if (sequential? (:store-id head))
                             (:store-id head)
                             [(:store-id head) (:branch head :db)])))))

     (defn- invoke-listener-callback! [{:keys [callback]} report]
       (try
         (callback report)
         (catch Throwable error
           (log/error :datahike/http-listen-callback-error
                      {:message (ex-message error)
                       :error-class (.getName (class error))}))))

     (defn- deliver-listen-event!
       [{:keys [conn last-commit] :as listener} event data]
       (when (active-listener? listener)
         (case event
           "report"
           (do
             (reset! last-commit (:commit-id data))
             (invoke-listener-callback!
              listener
              (cond-> {:db-after (remote-db-from-head conn data)
                       :db-before nil
                       :tempids (:tempids data)
                       :commit-id (:commit-id data)}
                (contains? data :tx-data) (assoc :tx-data (:tx-data data))
                (:truncated data) (assoc :truncated true))))

           ("resync" "coalesced")
           (do
             (reset! last-commit (:commit-id data))
             (invoke-listener-callback!
              listener {:resync true :db-after (remote-db-from-head conn data)}))

           "deleted"
           (do
             (invoke-listener-callback! listener {:deleted true})
             (stop-listener! listener))

           nil)))

     (defn- sse-field-value [line prefix]
       (let [value (subs line (count prefix))]
         (if (str/starts-with? value " ") (subs value 1) value)))

     (defn- read-listen-stream! [listener ^java.io.InputStream body]
       (with-open [reader (BufferedReader. (InputStreamReader. body "UTF-8"))]
         (loop [event nil data []]
           (when (active-listener? listener)
             (if-let [line (.readLine reader)]
               (cond
                 (empty? line)
                 (do
                   (when (and event (seq data))
                     (deliver-listen-event!
                      listener event
                      (j/read-value (str/join "\n" data) remote/json-mapper)))
                   (recur nil []))

                 (str/starts-with? line ":")
                 (recur event data)

                 (str/starts-with? line "event:")
                 (recur (sse-field-value line "event:") data)

                 (str/starts-with? line "data:")
                 (recur event (conj data (sse-field-value line "data:")))

                 :else
                 (recur event data))
               nil)))))

     (defn- listen-target [{:keys [conn last-commit]}]
       (let [{:keys [url]} (:remote-peer conn)
             [store-id branch] (if (sequential? (:store-id conn))
                                 (:store-id conn)
                                 [(:store-id conn) :db])]
         {:url (str url "/listen")
          :query-params (cond-> {"store" (str store-id)
                                 "branch" (name branch)}
                          @last-commit (assoc "since" (str @last-commit)))}))

     (defn- wait-to-reconnect [listener ^long delay]
       (try
         (Thread/sleep delay)
         (active-listener? listener)
         (catch InterruptedException _ false)))

     (defn- listen-once! [{:keys [conn stream] :as listener}]
       (let [{:keys [url query-params]} (listen-target listener)
             {:keys [token]} (:remote-peer conn)
             response (http/get url
                                {:query-params query-params
                                 :headers (cond-> {"accept" "text/event-stream"}
                                            token (assoc "authorization" (str "token " token)))
                                 :as :stream
                                 :throw false})
             status (:status response)
             body (:body response)]
         (cond
           (contains? #{401 403 404} status)
           (do
             (when body
               (try (.close ^java.io.Closeable body) (catch Exception _)))
             (invoke-listener-callback!
              listener {:error (ex-info (str "HTTP " status " from " url)
                                        {:status status :url url})
                        :status status})
             (stop-listener! listener)
             :terminal)

           (= 200 status)
           (do
             (reset! stream body)
             (try
               (when (active-listener? listener)
                 (read-listen-stream! listener body))
               (catch Throwable error
                 (when (active-listener? listener)
                   (log/error :datahike/http-listen-error
                              {:message (ex-message error)
                               :error-class (.getName (class error))})))
               (finally
                 (close-listen-stream! listener)))
             :success)

           :else
           (do
             (when body
               (try (.close ^java.io.Closeable body) (catch Exception _)))
             (throw (ex-info (str "HTTP " status " from " url)
                             {:status status :url url}))))))

     (defn- run-listener! [listener]
       (loop [backoff 500]
         (when (active-listener? listener)
           (let [outcome (try
                           (listen-once! listener)
                           (catch Throwable error
                             (when (active-listener? listener)
                               (log/error :datahike/http-listen-error
                                          {:message (ex-message error)
                                           :error-class (.getName (class error))}))
                             :failure))]
             (case outcome
               :success (when (wait-to-reconnect listener 500)
                          (recur 1000))
               :failure (when (wait-to-reconnect listener backoff)
                          (recur (min 30000 (* 2 backoff))))
               nil)))))

     (declare unlisten)

     (defn listen
       "Listen for remote changes so a thin client can refresh without polling."
       ([conn callback]
        (listen conn (str (random-uuid)) callback))
       ([conn key callback]
        (unlisten conn key)
        (let [listener {:id (listener-id conn key)
                        :conn conn
                        :callback callback
                        :last-commit (atom nil)
                        :stream (atom nil)
                        :thread (atom nil)
                        :stopped? (atom false)}
              worker (doto (Thread. #(run-listener! listener)
                                    (str "datahike-http-listen-" key))
                       (.setDaemon true))]
          (reset! (:thread listener) worker)
          (swap! listeners assoc (:id listener) listener)
          (.start worker)
          key)))

     (defn unlisten
       "Stop a remote change listener so its request and reconnect loop release resources."
       [conn key]
       (when-let [listener (get @listeners (listener-id conn key))]
         (stop-listener! listener))
       nil)))

#?(:clj
   (doseq [[n {:keys [args doc supports-remote? referentially-transparent?]}] api/api-specification
           :when (not (#{'listen 'unlisten} n))]
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

#?(:cljs
   (do
     (defonce ^:private listeners (atom {}))

     (def ^:private listen-event-keys
       #{"store-id" "branch" "commit-id" "max-tx" "max-eid"
         "tempids" "tx-data" "truncated"})

     (defn ^:no-doc decode-listen-json
       "Decode the tagged values present in change events into client values."
       [value]
       (cond
         (array? value)
         (let [items (mapv decode-listen-json (array-seq value))]
           (if (and (= 2 (count items)) (string? (first items)))
             (case (first items)
               "!kw" (keyword (second items))
               "!sym" (symbol (second items))
               "!set" (set (second items))
               "!uuid" (uuid (second items))
               "!date" (js/Date. (js/Number (second items)))
               "!datahike/Datom" (apply dd/datom (second items))
               items)
             items))

         (and (some? value) (object? value))
         (into {}
               (map (fn [key]
                      [key
                       (decode-listen-json (aget value key))]))
               (js-keys value))

         :else value))

     (defn- decode-listen-event [value]
       (let [decoded (decode-listen-json value)]
         (into {}
               (map (fn [[key value]]
                      [(if (contains? listen-event-keys key) (keyword key) key)
                       value]))
               decoded)))

     (defn- listener-id [conn key]
       [(get-in conn [:remote-peer :url]) (:store-id conn) key])

     (defn- active-listener? [{:keys [id stopped?] :as listener}]
       (and (not @stopped?) (identical? listener (get @listeners id))))

     (defn- stop-listener! [{:keys [id stopped? controller timer] :as listener}]
       (reset! stopped? true)
       (when-let [timeout @timer]
         (js/clearTimeout timeout)
         (reset! timer nil))
       (when-let [abort @controller]
         (.abort abort)
         (reset! controller nil))
       (swap! listeners #(if (identical? listener (get % id)) (dissoc % id) %)))

     (defn- remote-db-from-head [conn head]
       (binding [remote/*remote-peer* (:remote-peer conn)]
         (remote/remote-db
          (assoc (select-keys head [:max-tx :max-eid :commit-id])
                 :store-id (if (sequential? (:store-id head))
                             (:store-id head)
                             [(:store-id head) (:branch head :db)])))))

     (defn- invoke-listener-callback! [{:keys [callback]} report]
       (try
         (callback report)
         (catch :default error
           (trace :datahike/http-listen-callback-error
                  {:message (or (.-message error) (str error))}))))

     (defn- deliver-listen-event!
       [{:keys [conn last-commit] :as listener} event data]
       (when (active-listener? listener)
         (case event
           "report"
           (do
             (reset! last-commit (:commit-id data))
             (invoke-listener-callback!
              listener
              (cond-> {:db-after (remote-db-from-head conn data)
                       :db-before nil
                       :tempids (:tempids data)
                       :commit-id (:commit-id data)}
                (contains? data :tx-data) (assoc :tx-data (:tx-data data))
                (:truncated data) (assoc :truncated true))))

           ("resync" "coalesced")
           (do
             (reset! last-commit (:commit-id data))
             (invoke-listener-callback!
              listener {:resync true :db-after (remote-db-from-head conn data)}))

           "deleted"
           (do
             (invoke-listener-callback! listener {:deleted true})
             (stop-listener! listener))

           nil)))

     (defn- parse-sse-frame! [listener frame]
       (let [{:keys [event data]}
             (reduce (fn [parsed line]
                       (cond
                         (or (empty? line) (str/starts-with? line ":")) parsed
                         (str/starts-with? line "event:")
                         (assoc parsed :event (str/trim (subs line 6)))
                         (str/starts-with? line "data:")
                         (update parsed :data conj (str/triml (subs line 5)))
                         :else parsed))
                     {:data []}
                     (str/split-lines frame))]
         (when (and event (seq data))
           (deliver-listen-event! listener event
                                  (decode-listen-event
                                   (js/JSON.parse (str/join "\n" data)))))))

     (defn ^:no-doc split-listen-chunk
       "Split complete SSE frames while retaining an unfinished trailing CR."
       [pending chunk]
       (let [raw (str pending chunk)
             trailing-cr? (str/ends-with? raw "\r")
             complete (if trailing-cr? (subs raw 0 (dec (count raw))) raw)
             normalized (str/replace complete #"\r\n?" "\n")
             frames (.split normalized "\n\n")
             pending (str (.pop frames) (when trailing-cr? "\r"))]
         [(vec (array-seq frames)) pending]))

     (defn- read-listen-stream! [listener reader decoder pending]
       (-> (.read reader)
           (.then
            (fn [result]
              (if (.-done result)
                nil
                (let [chunk (.decode decoder (.-value result) #js {:stream true})
                      [frames pending] (split-listen-chunk pending chunk)]
                  (doseq [frame frames]
                    (when (seq frame)
                      (parse-sse-frame! listener frame)))
                  (if (active-listener? listener)
                    (read-listen-stream! listener reader decoder pending)
                    (.cancel reader))))))))

     (declare connect-listener! unlisten)

     (defn- reconnect-listener! [{:keys [backoff timer] :as listener}]
       (when (active-listener? listener)
         (let [delay @backoff]
           (reset! backoff (min 30000 (* 2 delay)))
           (reset! timer
                   (js/setTimeout
                    (fn []
                      (reset! timer nil)
                      (connect-listener! listener))
                    delay)))))

     (defn- listen-url [{:keys [conn last-commit]}]
       (let [{:keys [url]} (:remote-peer conn)
             [store-id branch] (if (sequential? (:store-id conn))
                                 (:store-id conn)
                                 [(:store-id conn) :db])
             since @last-commit]
         (str url "/listen?store=" (js/encodeURIComponent (str store-id))
              "&branch=" (js/encodeURIComponent (name branch))
              (when since (str "&since=" (js/encodeURIComponent (str since)))))))

     (defn- connect-listener!
       [{:keys [conn controller] :as listener}]
       (when (active-listener? listener)
         (let [{:keys [token]} (:remote-peer conn)
               abort (js/AbortController.)
               target (listen-url listener)
               headers (cond-> {"accept" "text/event-stream"}
                         token (assoc "authorization" (str "token " token)))]
           (reset! controller abort)
           (trace :datahike/http-listen {:url target})
           (-> (js/fetch target
                         #js {:method "GET"
                              :headers (clj->js headers)
                              :signal (.-signal abort)})
               (.then (fn [response]
                        (let [status (.-status response)]
                          (if-not (.-ok response)
                            (if (contains? #{401 403 404} status)
                              (let [error (ex-info (str "HTTP " status " from " target)
                                                   {:status status :url target})]
                                (invoke-listener-callback!
                                 listener {:error error :status status})
                                (stop-listener! listener)
                                nil)
                              (throw (js/Error. (str "HTTP " status " from " target))))
                            (do
                              (reset! (:backoff listener) 500)
                              (if-let [body (.-body response)]
                                (read-listen-stream! listener (.getReader body) (js/TextDecoder.) "")
                                (throw (js/Error. (str "No response body from " target)))))))))
               (.then (fn [_] (reconnect-listener! listener)))
               (.catch (fn [error]
                         (when (active-listener? listener)
                           (trace :datahike/http-listen-error {:url target
                                                               :message (or (.-message error) (str error))})
                           (reconnect-listener! listener))))))))

     (defn listen
       "Listen for remote changes so a thin client can refresh without polling."
       ([conn callback]
        (listen conn (str (random-uuid)) callback))
       ([conn key callback]
        (unlisten conn key)
        (let [listener {:id (listener-id conn key)
                        :conn conn
                        :callback callback
                        :last-commit (atom nil)
                        :backoff (atom 500)
                        :controller (atom nil)
                        :timer (atom nil)
                        :stopped? (atom false)}]
          (swap! listeners assoc (:id listener) listener)
          (connect-listener! listener)
          key)))

     (defn unlisten
       "Stop a remote change listener so its request and reconnect loop release resources."
       [conn key]
       (when-let [listener (get @listeners (listener-id conn key))]
         (stop-listener! listener))
       nil)))

#?(:clj
   (defmacro emit-remote-api
     "Define the API functions for ClojureScript, one per specification entry:
      the remote-capable ones call the server, the rest throw as on the JVM."
     []
     `(do
        ~@(for [[n {:keys [args doc supports-remote? referentially-transparent?]}] api/api-specification
                :when (not (#{'listen 'unlisten} n))]
            `(defn ~(with-meta n {:arglists (list 'quote (api/malli-schema->argslist args)) :doc doc})
               [& ~'args]
               ~(if supports-remote?
                  `(remote-call ~(if referentially-transparent? :get :post) ~(api/->url n) ~'args ~(= n 'create-database))
                  `(throw (ex-info (str ~(str n) " is not supported for remote connections.")
                                   {:type :remote-not-supported :function ~(str n)}))))))))

#?(:cljs (emit-remote-api))

#?(:clj (defmethod remote/remote-deref :datahike-server [conn] (db conn)))
