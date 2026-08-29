(ns datahike.test.http.logging-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.http.logging :as logging]
            [jsonista.core :as json]))

(deftest structured-events-have-a-stable-json-shape
  (let [event (logging/trove-event-map
               "datahike.http.server" [12 7] :warn
               :datahike/test-event
               {:msg "something happened"
                :data {:backend :memory}
                :error (ex-info "boom" {:secret "not-in-message"})})]
    (is (string? (:timestamp event)))
    (is (= "warn" (:level event)))
    (is (= "datahike.http.server" (:logger event)))
    (is (= {:line 12 :column 7} (:source event)))
    (is (= "datahike/test-event" (:event event)))
    (is (= "something happened" (:message event)))
    (is (= {:backend "memory"} (:data event)))
    (is (= "clojure.lang.ExceptionInfo" (get-in event [:error :class])))
    (is (= "boom" (get-in event [:error :message])))
    (is (string? (get-in event [:error :stack-trace])))))

(deftest json-logger-filters-before-forcing-and-writes-one-line
  (let [forced? (atom false)
        logger (logging/json-log-fn :warn)]
    (is (= "" (with-out-str
                (logger "test" nil :info :datahike/hidden
                        (delay (reset! forced? true))))))
    (is (false? @forced?) "filtered events retain Trove's lazy fast path")
    (let [output (with-out-str
                   (logger "test" nil :warn :datahike/visible
                           (delay {:data {:value 1}})))
          decoded (json/read-value output)]
      (is (= "warn" (get decoded "level")))
      (is (= "datahike/visible" (get decoded "event")))
      (is (= 1 (get-in decoded ["data" "value"])))
      (is (= 1 (count (filter #{\newline} output)))))))

(deftest logging-configuration-is-strict
  (testing "bad file values fail even though env and CLI are parsed earlier"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Log level"
                          (logging/configure! {:level :verbose})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Log format"
                          (logging/configure! {:log-format :xml})))))
