(ns datahike.test.config
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.config :refer :all])
  (:import
   (datahike.config Configuration
                    Store)))

(deftest int-from-env-test
  (is (= 1000
         (int-from-env :foo 1000))))

(deftest bool-from-env-test
  (is (bool-from-env :foo true)))

(deftest deep-merge-test
  (is (= (Configuration. (Store. :pg "foobar" nil "/tmp/testfoo" nil nil nil nil nil)
                         true true)
         (deep-merge (Configuration. (Store. :file nil nil "/tmp/testfoo" nil nil nil nil nil)
                                     false true)
                     {:store {:backend :pg :username "foobar"}
                      :schema-on-read true}))))

(deftest config-test
  (let [mem-uri "datahike:mem://config-test"
        file-uri "datahike:file:///tmp/config-test"
        level-uri "datahike:level:///tmp/config-test"
        pg-uri "datahike:pg://alice:foo@localhost:5432/config-test"]

    (are [x y] (= x (uri->config y))
      {:backend :mem :host "config-test" :uri mem-uri}
      mem-uri

      {:backend :file :path "/tmp/config-test" :uri file-uri}
      file-uri

      {:backend :level :path "/tmp/config-test" :uri level-uri}
      level-uri

      {:backend :pg
       :host "localhost" :port 5432 :username "alice" :password "foo" :path "/config-test"
       :uri pg-uri}
      pg-uri)))

(deftest load-config-test
  (testing "Loading configuration defaults"
    (is (= (Configuration. (Store. :mem nil nil nil nil nil nil false nil)
                           false true)
           (reload-config)))))

(deftest reload-config-test
  (testing "Reloading configuration"
    (is (= (Configuration. (Store. :file nil nil "/tmp/testfoo" nil nil nil false nil)
                           true true)
           (reload-config {:store {:backend :file
                                   :path "/tmp/testfoo"}
                           :schema-on-read true})))))
