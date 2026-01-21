(ns datahike.pod
  "Babashka pod interface for Datahike.

   This namespace provides the pod protocol implementation that allows
   Datahike to be used from Babashka scripts.

   The pod manages connections and database snapshots via ID-based references,
   allowing immutable db values to be cached and released explicitly.

   Functions are generated at compile-time from the API specification via
   datahike.codegen.pod macros."
  (:refer-clojure :exclude [read read-string])
  (:require [bencode.core :as bencode]
            [clojure.java.io :as io]
            [cognitect.transit :as transit]
            [datahike.api :as d]
            [datahike.writing :as writing]
            [datahike.codegen.pod :as codegen])
  (:import [java.io PushbackInputStream]))

(set! *warn-on-reflection* true)

;; =============================================================================
;; I/O Infrastructure
;; =============================================================================

(def stdin (PushbackInputStream. System/in))
(def stdout System/out)
(def stderr System/err)

(def debug? false)

(defn debug [& strs]
  (when debug?
    (binding [*out* (io/writer System/err)]
      (apply prn strs))))

(defn write
  ([v] (write stdout v))
  ([stream v]
   (debug :writing v)
   (bencode/write-bencode stream v)
   (flush)))

(defn write-err
  ([v] (write stderr v))
  ([stream v]
   (debug :writing v)
   (bencode/write-bencode stream v)
   (flush)))

(defn read-string [^"[B" v]
  (String. v))

(defn read [stream]
  (bencode/read-bencode stream))

(defn read-transit [^String v]
  (transit/read
   (transit/reader
    (java.io.ByteArrayInputStream. (.getBytes v "utf-8"))
    :json)))

(defn write-transit [v]
  (let [baos (java.io.ByteArrayOutputStream.)]
    (transit/write (transit/writer baos :json) v)
    (.toString baos "utf-8")))

;; =============================================================================
;; Generated Pod Runtime (atoms, helpers)
;; =============================================================================

(codegen/defpod-runtime)

;; =============================================================================
;; Generated Pod Functions
;; =============================================================================

(codegen/defpod-functions)

;; =============================================================================
;; Generated Pod Protocol Maps
;; =============================================================================

(codegen/defpod-publics)

(codegen/defpod-describe-map)

(defn lookup
  "Look up a function by name."
  [var]
  (get publics (symbol (name var))))

;; =============================================================================
;; Pod Runner
;; =============================================================================

(defn run-pod
  "Main pod event loop."
  [_args]
  (loop []
    (let [message (try (read stdin)
                       (catch java.io.EOFException _
                         ::EOF))]
      (when-not (identical? ::EOF message)
        (let [op (get message "op")
              op (read-string op)
              op (keyword op)
              id (some-> (get message "id")
                         read-string)
              id (or id "unknown")]
          (case op
            :describe (do (write {"format" "transit+json"
                                  "namespaces" [{"name" "datahike.pod"
                                                 "vars" describe-map}]
                                  "id" id
                                  "ops" {"shutdown" {}}})
                          (recur))
            :invoke (do (try
                          (let [var (-> (get message "var")
                                        read-string
                                        symbol)
                                args (get message "args")
                                args (read-string args)
                                args (read-transit args)]
                            (if-let [f (lookup var)]
                              (do (debug f)
                                  (debug args)
                                  (let [result (apply f args)
                                        _ (debug result)
                                        value (write-transit result)
                                        reply {"value" value
                                               "id" id
                                               "status" ["done"]}]
                                    (write stdout reply)))
                              (throw (ex-info (str "Var not found: " var) {}))))
                          (catch Throwable e
                            (debug e)
                            (let [reply {"ex-message" (ex-message e)
                                         "ex-data" (write-transit
                                                    (-> (ex-data e)
                                                        (update :argument-type str)
                                                        (assoc :type (str (class e)))))
                                         "id" id
                                         "status" ["done" "error"]}]
                              (write stdout reply))))
                        (recur))
            :shutdown (System/exit 0)
            (do
              (let [reply {"ex-message" "Unknown op"
                           "ex-data" (pr-str {:op op})
                           "id" id
                           "status" ["done" "error"]}]
                (write stdout reply))
              (recur))))))))
