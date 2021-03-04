(ns datahike.integration-test.depr-config-uri-test
  (:require [clojure.test :refer :all]
            [datahike.integration-test :as it]))

(def config "datahike:file:///tmp/file-test-3")

(defn depr-config-uri-fixture [f]
  (println "deprecated file uri config: " config)
  (it/integration-test-fixture config)
  (f))

(use-fixtures :once depr-config-uri-fixture)

(deftest ^:integration depr-config-uri-test []
  (it/integration-test config))
