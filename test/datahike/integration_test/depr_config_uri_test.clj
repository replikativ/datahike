(ns datahike.integration-test.depr-config-uri-test
  (:require [clojure.test :refer :all]
            [datahike.integration-test :as it]))

;; Still a Linux-only deprecated URI test (Windows drive handling is separate).
;; Build the outer URI as well, since uri->config decodes two URI layers.
(def config
  (let [path (.getAbsolutePath (java.io.File. (System/getProperty "java.io.tmpdir")
                                              "file-test-3"))
        file-uri (java.net.URI. "file" nil path
                                "id=5c6e0000-0000-0000-0000-000000000003" nil)]
    (str (java.net.URI. "datahike" (str file-uri) nil))))

(defn depr-config-uri-fixture [f]
  (println "deprecated file uri config: " config)
  (try
    (it/integration-test-fixture config)
    (f)
    (finally (datahike.api/delete-database config))))

(use-fixtures :once depr-config-uri-fixture)

(deftest ^:integration depr-config-uri-test []
  (it/integration-test config))
