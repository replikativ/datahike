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
            [datahike.remote.cbor :as rcbor]
            [muuntaja.format.core :as core])
  (:import [java.io ByteArrayOutputStream InputStream]))

(defn- ^"[B" read-all-bytes [^InputStream in]
  (let [out (ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn- decoder [options]
  (reify
    core/Decode
    (decode [_ data _charset]
      (boring/decode (if (bytes? data) data (read-all-bytes data)) options))))

(defn- encoder [options]
  (reify
    core/EncodeToBytes
    (encode-to-bytes [_ data _charset]
      (boring/encode data options))))

(defn cbor-format
  "The muuntaja Format, with its codec options ALREADY IN IT.

   A function rather than a bare `def`, because the options are the contract.
   As a plain Format with empty `:decoder-opts`, anyone installing it into their
   own muuntaja instance — the only reason to make it public — would get no
   registry and boring's `:fallback`: every datahike handle would decode to a
   tagged literal, and a protocol mismatch between peers would surface as an
   unrelated failure somewhere later. That is exactly the outcome
   `datahike.remote.cbor/decode-opts` chooses `:on-unknown-record :error` to
   prevent, and it would have been silently undone one namespace away.

   Taking the registry as an argument also removes the process-global: the
   server builds one and hands it in, so what a given format speaks is visible
   at the call site rather than resolved through a var."
  [registry]
  (core/map->Format
   {:name         "application/cbor"
    :decoder      [decoder]
    :decoder-opts (rcbor/decode-opts registry)
    :encoder      [encoder]
    :encoder-opts (rcbor/encode-opts registry)}))
