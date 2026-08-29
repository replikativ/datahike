(ns datahike.http.middleware
  (:require
   [clojure.edn :as edn]
   [clojure.walk :as cw]
   [datahike.json :as json]
   [datahike.readers :refer [edn-readers]]
   [muuntaja.core :as m]
   [replikativ.logging :as log])
  (:import
   [clojure.lang ExceptionInfo]))

(defn- bearer [request]
  (some->> (get-in request [:headers "authorization"])
           (re-find #"(?i)^token\s+(\S+)\s*$")
           second))

(defn- same-secret? [^String expected ^String given]
  (and expected given
       (java.security.MessageDigest/isEqual (.getBytes expected "UTF-8") (.getBytes given "UTF-8"))))

(defn validators
  "The authentication chain `config` describes, each a
   `(fn [request] -> principal | nil)` — the shape kabel's
   `kabel.auth.jwt/build-bearer-validator` returns, so one validator serves
   both transports. In order:

   - `:dev-mode true` — everyone is `{:sub \"dev\"}`. Never in production.
   - `:auth :upstream` — the host's own middleware authenticated already; its
     `:datahike/principal` on the request, or `{:sub \"upstream\"}`.
   - `:token` — the shared secret, sent as `authorization: token <token>`;
     the principal is `:token-subject`, \"root\" by default.
   - `:validator` — yours.

   A principal is a map with `:sub`, the subject's id. Subjects share one
   namespace, so the token's subject is reserved: a `:validator` principal
   claiming it is refused rather than let an outside identity become the
   break-glass one."
  [{:keys [dev-mode auth token token-subject validator]}]
  (let [token-subject (or token-subject "root")]
    (remove nil?
            [(when dev-mode
               (constantly {:sub "dev" :auth :dev-mode}))
             (when (= :upstream auth)
               (fn [request] (or (:datahike/principal request) {:sub "upstream" :auth :upstream})))
             (when token
               (fn [request] (when (same-secret? token (bearer request))
                               {:sub token-subject :auth :token})))
             (when validator
               (fn [request]
                 (when-let [principal (validator request)]
                   (when-not (= token-subject (:sub principal))
                     (assoc principal :auth :validator)))))])))

(defn authenticate
  "The principal the first accepting validator returns, or nil."
  [validators request]
  (some #(% request) validators))

;; TOOD map more errors
(defn cause->status-code [cause]
  400)

(defn encode-plain-value [muuntaja-with-opts]
  (fn [handler]
    (fn [request]
      (let [format         (:content-type request)
            encoder        (when format (m/encoder muuntaja-with-opts format))
            response       (handler request)
            should-encode? (and encoder
                                (not (instance? java.io.ByteArrayInputStream (:body response))))
            ret            (if should-encode? (update response :body #(encoder %)) response)]
        ret))))

(defn patch-swagger-json [handler]
  (fn [request]
    (let [response (handler request)]
      (if (get-in response [:body :swagger])
        (cw/postwalk (fn [n]
                       (if (set? n) (vec n) n))
                     response)
        response))))

(defn support-embedded-edn-in-json [handler]
  (fn [request]
    (let [{:keys [content-type body-params uri]} request]
      (if (= content-type "application/json")
        (if (.endsWith ^String uri "transact")
          (let [[conn tx-data] body-params
                new-body-params [conn (json/xf-data-for-tx tx-data @conn)]]
            (log/trace :datahike/http-transact-transform {:body-params new-body-params})
            (handler (assoc request :body-params new-body-params)))
          (let [[f & r]         body-params
                new-body-params (vec (concat [(if (string? f) (edn/read-string {:readers edn-readers} f) f)] r))]
            (log/trace :datahike/http-old-body-params {:body-params body-params})
            (log/trace :datahike/http-new-body-params {:body-params new-body-params})
            (handler (assoc request :body-params new-body-params))))
        (handler request)))))
