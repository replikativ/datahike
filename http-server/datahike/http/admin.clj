(ns datahike.http.admin
  "Static, dependency-free operator landing page for the standalone server."
  (:require [clojure.java.io :as io]
            [datahike.http.routes :as routes]
            [datahike.http.system :as system]
            [datahike.metrics :as metrics]))

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

(defn- server-admin? [config principal]
  (if-let [authorize (:authorize config)]
    (authorize {:op :admin :principal principal :db nil :payload nil})
    true))

(defn- status-response [config connections nrepl-status principal page-options]
  (try
    (let [runtime (metrics/runtime-snapshot connections)
          ;; Do not let viewing the dashboard alter its query statistics.
          page    (binding [metrics/*query-metrics?* false]
                    (system/visible-entry-page config principal page-options))]
      {:status 200
       :body {:node (when (server-admin? config principal)
                      (assoc (:node runtime) :nrepl @nrepl-status))
              :page (:page page)
              :databases
              (mapv (fn [{:keys [store-id] :as database}]
                      (assoc database :activity
                             (get-in runtime [:databases store-id]
                                     {:loaded? false
                                      :leases 0
                                      :transactions 0
                                      :transacted-datoms 0
                                      :commits 0
                                      :head-conflicts 0})))
                    (:databases page))}})
    (catch Exception e
      (routes/error-response e))))

(defn routes
  "Public shell and assets. Data is fetched from the normally authenticated
   `/version` and `/admin/status` APIs, so this introduces no authorization path."
  ([config connections] (routes config connections (atom {:enabled false})))
  ([config connections nrepl-status]
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
           :handler (fn [_] (asset-response :logo))}}]
    ["/admin/status"
     {:get {:no-doc true
            :metric-op :read
            :parameters {:query [:map
                                 [:q {:optional true} :string]
                                 [:offset {:optional true :default 0} [:int {:min 0}]]
                                 [:limit {:optional true :default 24} [:int {:min 1 :max 100}]]]}
            :handler (fn [{{query :query} :parameters
                           principal :datahike/principal}]
                       (status-response config connections nrepl-status principal query))}}]]))
