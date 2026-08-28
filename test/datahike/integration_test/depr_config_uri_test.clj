(ns datahike.integration-test.depr-config-uri-test
  (:require [clojure.test :refer :all]
            [datahike.integration-test :as it]))

;; The one store path in the suite still spelled `/tmp` literally. It is not a
;; path here, it is the PATH COMPONENT OF A URI, and `uri->config` parses it with
;; `java.net.URI` — a Windows temp directory (`C:\Users\…\Temp`) is not a legal
;; URI path at all, so making this portable means URI-encoding a filesystem path
;; rather than swapping in a helper. Not worth it on the deprecated URI config
;; form, which is what this test exists to keep working.
(def config "datahike:file:///tmp/file-test-3?id=5c6e0000-0000-0000-0000-000000000003")

(defn depr-config-uri-fixture [f]
  (println "deprecated file uri config: " config)
  (it/integration-test-fixture config)
  (f))

(use-fixtures :once depr-config-uri-fixture)

(deftest ^:integration depr-config-uri-test []
  (it/integration-test config))
