(ns datahike.test.env-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.env :as env]))

(deftest keys-retain-the-historical-normalization
  (is (= :datahike-store-backend (env/normalize-key "DATAHIKE_STORE_BACKEND")))
  (is (= :datahike-store-backend (env/normalize-key "datahike.store.backend")))
  (is (= :schema-meta-cache-size (env/normalize-key :schema_meta_cache_size))))

(deftest only-datahike-settings-enter-the-runtime-map
  (is (= {:datahike-store-backend "file"
          :datahike-keep-history "false"
          :schema-meta-cache-size "42"}
         (env/merge-sources
          {:datahike-store-backend :memory
           :unrelated-secret "must-not-enter-the-map"
           :schema-meta-cache-size 42}
          {"DATAHIKE_STORE_BACKEND" "file"
           "DATAHIKE_KEEP_HISTORY" "false"
           "JAVA_HOME" "/environment/jdk"}
          {"java.home" "/property/jdk"}))))

(deftest later-sources-win
  (is (= {:datahike-store-backend "system-property"}
         (env/merge-sources
          {"datahike.store.backend" "file"}
          {"DATAHIKE_STORE_BACKEND" "environment"}
          {"datahike.store.backend" "system-property"}))))
