(ns datahike.http.admin
  "Static, dependency-free operator landing page for the standalone server."
  (:require [clojure.java.io :as io]))

(def ^:private security-headers
  {"content-security-policy" (str "default-src 'none'; "
                                  "connect-src 'self'; "
                                  "img-src 'self'; "
                                  "script-src 'self'; "
                                  "style-src 'self'; "
                                  "base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
   "referrer-policy" "no-referrer"
   "cross-origin-resource-policy" "same-origin"
   "permissions-policy" "camera=(), microphone=(), geolocation=()"
   "x-content-type-options" "nosniff"
   "x-frame-options" "DENY"})

(defn- resource-bytes [path]
  (if-let [resource (io/resource path)]
    (with-open [in (io/input-stream resource)
                out (java.io.ByteArrayOutputStream.)]
      (io/copy in out)
      (.toByteArray out))
    (throw (ex-info (str "Missing admin resource " path)
                    {:type :datahike.http/missing-admin-resource
                     :resource path}))))

(def ^:private assets
  {:html {:bytes (delay (resource-bytes "datahike/http/admin/index.html"))
          :content-type "text/html; charset=utf-8"
          :cache-control "no-store"}
   :css  {:bytes (delay (resource-bytes "datahike/http/admin/style.css"))
          :content-type "text/css; charset=utf-8"
          :cache-control "no-store"}
   :js   {:bytes (delay (resource-bytes "datahike/http/admin/app.js"))
          :content-type "text/javascript; charset=utf-8"
          :cache-control "no-store"}
   :logo {:bytes (delay (resource-bytes "datahike/http/admin/datahike-logo.svg"))
          :content-type "image/svg+xml"
          :cache-control "public, max-age=86400"}})

(defn- asset-response [asset]
  (let [{:keys [bytes content-type cache-control]} (get assets asset)
        body @bytes]
    {:status 200
     :headers (assoc security-headers
                     "cache-control" cache-control
                     "content-type" content-type
                     "content-length" (str (alength ^bytes body)))
     ;; Muuntaja deliberately leaves byte-array streams untouched.
     :body (java.io.ByteArrayInputStream. body)}))

(defn routes
  "Public shell and assets. Data is fetched from the normally authenticated
   `/version` and `/databases` APIs, so this introduces no authorization path."
  []
  [["/"
    {:public? true
     :get {:no-doc true
           :metric-op :admin
           :handler (fn [_] (asset-response :html))}}]
   ["/admin"
    {:public? true
     :get {:no-doc true
           :metric-op :admin
           :handler (fn [_] (asset-response :html))}}]
   ["/admin/"
    {:public? true
     :get {:no-doc true
           :handler (fn [_]
                      {:status 308
                       :headers {"location" "/admin"
                                 "cache-control" "no-store"}})}}]
   ["/admin/style.css"
    {:public? true
     :get {:no-doc true
           :handler (fn [_] (asset-response :css))}}]
   ["/admin/app.js"
    {:public? true
     :get {:no-doc true
           :handler (fn [_] (asset-response :js))}}]
   ["/admin/datahike-logo.svg"
    {:public? true
     :get {:no-doc true
           :handler (fn [_] (asset-response :logo))}}]])
