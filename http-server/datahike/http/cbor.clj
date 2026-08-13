(ns datahike.http.cbor
  "The `application/cbor` muuntaja format — the TRANSPORT half of CBOR support.

  The codec itself is `datahike.remote.cbor`, in `src/`, because it describes
  the remote protocol rather than HTTP. This namespace is only the adapter that
  lets reitit negotiate that codec by media type, which is why it lives here
  with the rest of the server and takes muuntaja as a dependency.

  Binary, so `charset` is ignored on both sides — CBOR frames its own strings
  as UTF-8 by construction.

  No `EncodeToOutputStream`. boring builds a complete byte array before
  writing, so implementing the streaming arity would only wrap `encode-to-bytes`
  and claim a property it does not have; muuntaja falls back to the byte-array
  arity by itself."
  (:require [boring.core :as boring]
            [clojure.java.io :as io]
            [muuntaja.format.core :as core])
  (:import [java.io ByteArrayOutputStream InputStream]))

(defn- ^"[B" read-all-bytes [^InputStream in]
  (let [out (ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn decoder [options]
  (reify
    core/Decode
    (decode [_ data _charset]
      (boring/decode (if (bytes? data) data (read-all-bytes data)) options))))

(defn encoder [options]
  (reify
    core/EncodeToBytes
    (encode-to-bytes [_ data _charset]
      (boring/encode data options))))

(def cbor-format
  (core/map->Format
   {:name    "application/cbor"
    :decoder [decoder]
    :encoder [encoder]}))
