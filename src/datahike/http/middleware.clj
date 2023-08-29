(ns datahike.http.middleware
  (:require
   [buddy.auth :refer [authenticated?]]
   [buddy.auth.backends :as buddy-auth-backends]
   [buddy.auth.middleware :as buddy-auth-middleware]
   [taoensso.timbre :refer [info]]
   [muuntaja.core :as m])
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
            _        (prn response)
            ret      (try
                  (if (not (instance? java.io.ByteArrayInputStream (:body response)))
                    (update response :body #(encoder %))
                    response)
                  (catch Exception e
                    (prn "oops" e)))]
        (prn "ret-response" ret)
        ret))))
