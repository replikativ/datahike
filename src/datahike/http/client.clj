(ns datahike.http.client
  (:refer-clojure :exclude [filter])
  (:require [babashka.http-client :as http]
            [cognitect.transit :as transit]
            [datahike.api.specification :as api]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [datahike.datom :as dd]
            [datahike.remote :as remote]
            [datahike.spec :as spec]
            [datahike.impl.entity :as de]
            [taoensso.timbre :as log])
  (:import [java.io ByteArrayOutputStream]))

(def MEGABYTE (* 1024 1024))

(def MAX_OUTPUT_BUFFER_SIZE (* 4 MEGABYTE))

(defn post-edn [end-point remote-peer data]
  (let [{:keys [url token]} remote-peer
        fmt                 "application/edn"
        url                 (str url "/" end-point)
        body                (remote/edn-replace-remote-literals (pr-str data))
        _                   (log/trace "request" url end-point token data body)
        response
        (try
          (http/post url {:headers (merge {:content-type fmt
                                           :accept       fmt}
                                          (when token
                                            {:authorization (str "token " token)}))
                          :body    body})
          (catch Exception e
            (let [msg  (ex-message e)
                  data (ex-data e)
                  new-data
                  (update data :body #(edn/read-string {:readers remote/edn-readers} %))]
              (throw (ex-info msg new-data)))))
        response            (:body response)]
    (log/trace "response" response)
    (edn/read-string {:readers remote/edn-readers} response)))

(defn post-transit
  ([end-point remote-peer data]
   (post-transit end-point remote-peer data
                 remote/transit-read-handlers
                 remote/transit-write-handlers))
  ([end-point remote-peer data read-handlers write-handlers]
   (let [{:keys [url token max-output-buffer-size]}
         remote-peer
         fmt      "application/transit+json"
         url      (str url "/" end-point)
         out      (ByteArrayOutputStream. (or max-output-buffer-size MAX_OUTPUT_BUFFER_SIZE))
         writer   (transit/writer out :json {:handlers write-handlers})
         _        (transit/write writer data)
         _        (log/trace "request" url end-point token data out)
         response
         (try
           (http/post url {:headers
                           (merge {:content-type fmt
                                   :accept       fmt}
                                  (when token
                                    {:authorization (str "token " token)}))
                           :as   :stream
                           :body (.toByteArray out)})
           (catch Exception e
             ;; read exception
             (let [msg  (ex-message e)
                   data (ex-data e)
                   new-data
                   (update data :body
                           #(when (seq %)
                              (transit/read (transit/reader % :json {:handlers read-handlers}))))]
               (throw (ex-info msg new-data)))))
         response (:body response)
         response (transit/read (transit/reader response :json {:handlers read-handlers}))]
     (log/trace  "response" response)
     response)))

(defn get-remote [args]
  (let [remotes (disj
                 (into #{} (map :remote-peer args))
                 nil)]
    (if (> (count remotes) 1)
      (throw (ex-info "Arguments refer to more than one remote-peer." {:remotes remotes
                                                                       :args args}))
      (first remotes))))

(doseq [[n {:keys [args doc supports-remote?]}] api/api-specification]
  (eval
   `(~'def
     ~(with-meta n
        {:arglists `(api/spec-args->argslist (quote ~args))
         :doc      doc})
     (fn [& args#]
       ~(if-not supports-remote?
          `(throw (ex-info (str ~(str n) " is not supported for remote connections.")
                           {:type     :remote-not-supported
                            :function ~(str n)}))
          `(binding [remote/*remote-peer* (get-remote args#)]
             (let [url#    (:url remote/*remote-peer*)
                   format# (:format remote/*remote-peer*)]
               (({:transit post-transit
                  :edn     post-edn} (or format# :transit))
                ~(api/->url n) remote/*remote-peer* (vec args#)))))))))

(defmethod remote/remote-deref :datahike-server [conn] (db conn))
