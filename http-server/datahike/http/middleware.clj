(ns datahike.http.middleware
  (:require
   [buddy.auth :refer [authenticated?]]
   [buddy.auth.backends :as buddy-auth-backends]
   [buddy.auth.middleware :as buddy-auth-middleware]
   [clojure.walk :as cw]
   [datahike.json :as json]
   [muuntaja.core :as m]
   [taoensso.timbre :refer [info trace]])
  (:import
   [clojure.lang ExceptionInfo]))

(defn auth
  "Middleware used in routes that require authentication. If request is not
   authenticated a 401 not authorized response will be returned.
   Dev mode always authenticates."
  [config handler]
  (fn [request]
    (if (or (:dev-mode config) (authenticated? request))
      (handler request)
      {:status 401 :error "Not authorized"})))

(defn token-auth
  "Middleware used on routes requiring token authentication"
  [config handler]
  (buddy-auth-middleware/wrap-authentication
   handler
   (buddy-auth-backends/token {:token-name "token"
                               :authfn     (fn [_ token]
                                             (let [valid-token?  (not (nil? token))
                                                   correct-auth? (= (:token config) token)]
                                               (when (and correct-auth? valid-token?)
                                                 "authenticated-user")))})))

;; TOOD map more errors
(defn cause->status-code [cause]
  400)

(defn wrap-server-exception [handler]
  (fn [request]
    (try
      (handler request)
      (catch ExceptionInfo e
        (let [cause (:cause (.getData e))]
          (info "Server error:" e (.printStackTrace e))
          {:status (cause->status-code cause)
           :body   {:message (.getMessage e)}})))))

(defn wrap-fallback-exception
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (info "Fallback exception:" e (.printStackTrace e))
        {:status 500
         :body   {:message "Unexpected internal server error."}}))))

;; TODO why do we need to do this?
(defn encode-plain-value [muuntaja-with-opts]
  (fn [handler]
    (fn [request]
      (let [format   (:content-type request)
            encoder  (m/encoder muuntaja-with-opts format)
            response (handler request)
            ret      (if (not (instance? java.io.ByteArrayInputStream (:body response)))
                       (update response :body #(encoder %))
                       response)]
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
            (trace "transact transformation" new-body-params)
            (handler (assoc request :body-params new-body-params)))
          (let [[f & r]         body-params
                new-body-params (vec (concat [(if (string? f) (read-string f) f)] r))]
            (trace "old-body-params" body-params)
            (trace "new-body-params" new-body-params)
            (handler (assoc request :body-params new-body-params))))
        (handler request)))))
