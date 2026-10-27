(ns datahike.test.http.get-args-test
  "A GET must not carry a body: Google's front end answers one with a 400 or
   closes the stream. The JVM client sends a read's arguments in the URL while
   they fit and as a POST when they do not."
  (:require
   [babashka.http-client :as http]
   [boring.core :as boring]
   [clojure.edn :as edn]
   [clojure.test :as t :refer [is deftest testing]]
   [cognitect.transit :as transit]
   [datahike.http.client :as client]
   [datahike.remote.cbor :as rcbor])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Base64]))

(defn- transit-nil []
  (let [out (ByteArrayOutputStream.)]
    (transit/write (transit/writer out :json) nil)
    (ByteArrayInputStream. (.toByteArray out))))

;; format -> [request fn, stub response body]
(def ^:private formats
  {"edn"     [client/request-edn "nil"]
   "transit" [client/request-transit transit-nil]
   "cbor"    [client/request-cbor #(boring/encode nil (rcbor/encode-opts (rcbor/client-registry)))]
   "json"    [client/request-json #(ByteArrayInputStream. (.getBytes "null" "UTF-8"))]})

(defn- sent
  "The request `f` makes for `method` and `data`, with `http/request` stubbed."
  [f stub method data]
  (let [seen (atom nil)]
    (with-redefs [http/request (fn [req]
                                 (reset! seen req)
                                 {:body (if (fn? stub) (stub) stub)})]
      (f method "q" {:url "http://localhost:1"} data))
    @seen))

(deftest small-get-carries-its-arguments-in-the-url
  (doseq [[fmt [f stub]] formats]
    (testing fmt
      (let [req (sent f stub :get [[:find '?e :where ['?e :name "x"]]])]
        (is (= :get (:method req)))
        (is (nil? (:body req)) "a GET has no body")
        (is (nil? (get-in req [:headers "content-type"]))
            "no body, so nothing for a server to decode by content type")
        (is (some? (get-in req [:headers "accept"])))
        (is (= fmt (get-in req [:query-params "f"])))
        (is (string? (get-in req [:query-params "args"])))
        (is (pos? (alength (.decode (Base64/getUrlDecoder)
                                    ^String (get-in req [:query-params "args"])))))))))

(deftest large-get-goes-by-post
  (let [big [(apply str (repeat (* 2 client/url-args-limit) "x"))]]
    (doseq [[fmt [f stub]] formats]
      (testing fmt
        (let [req (sent f stub :get big)]
          (is (= :post (:method req)))
          (is (some? (:body req)))
          (is (some? (get-in req [:headers "content-type"])))
          (is (nil? (get-in req [:query-params "args"]))))))))

(deftest post-keeps-its-body
  (doseq [[fmt [f stub]] formats]
    (testing fmt
      (let [req (sent f stub :post [{:name "x"}])]
        (is (= :post (:method req)))
        (is (some? (:body req)))
        (is (nil? (:query-params req)))))))

(deftest edn-arguments-round-trip-through-the-url
  (let [data [[:find '?e :where ['?e :name "x"]]]
        req  (sent client/request-edn "nil" :get data)
        back (-> (get-in req [:query-params "args"])
                 (->> (.decode (Base64/getUrlDecoder)))
                 (String. "UTF-8")
                 edn/read-string)]
    (is (= data back))))
