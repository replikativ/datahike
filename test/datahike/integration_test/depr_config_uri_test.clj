(ns datahike.integration-test.depr-config-uri-test
  (:require [clojure.test :refer :all]
            [datahike.integration-test :as it]))

;; A `/tmp` literal, and a real one: the fixture below DOES create, connect and
;; delete a store at this path, so this test is not portable to Windows. It is
;; left as is because the path travels inside a URI — a Windows temp directory
;; (`C:\Users\…\Temp`) is not a legal URI path, `java.net.URI` hands back
;; `/C:/…` for the encoded form, and `io/file` cannot open that — so making it
;; portable means teaching the deprecated `uri->config` form Windows drive
;; letters. This test runs on Linux CI only (`bb test integration`).
(def config "datahike:file:///tmp/file-test-3?id=5c6e0000-0000-0000-0000-000000000003")

(defn depr-config-uri-fixture [f]
  (println "deprecated file uri config: " config)
  (it/integration-test-fixture config)
  (f))

(use-fixtures :once depr-config-uri-fixture)

(deftest ^:integration depr-config-uri-test []
  (it/integration-test config))
