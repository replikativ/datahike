(ns datahike.test.listener-ack-test
  (:require
   #?(:cljs [cljs.test :refer-macros [is]]
      :clj [clojure.test :refer [is]])
   [clojure.core.async :refer [<! alts! timeout]]
   [datahike.api :as d]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]
   [datahike.test.utils :as du]
   [taoensso.trove :as trove]))

(deftest-async transaction-listener-failure-does-not-hide-commits-or-skip-other-listeners
  (let [conn (<! (du/setup-db-async (du/cfg-template)))
        reports (atom [])
        diagnostics (atom 0)
        previous-log-fn trove/*log-fn*]
    (try
      ;; A stable real registry puts the broken listener before the healthy one.
      (d/listen conn :broken (fn [_] (throw (ex-info "transaction listener failed" {}))))
      (d/listen conn :healthy #(swap! reports conj %))
      (swap! (:listeners (meta conn)) #(into (sorted-map) %))
      ;; CLJS async callbacks do not convey dynamic bindings. Keep the narrowly
      ;; failing test sink installed through acknowledgement on both platforms.
      (trove/set-log-fn! (fn [_ _ _ id _]
                           (when (= :datahike/transaction-listener-error id)
                             (swap! diagnostics inc)
                             (throw (ex-info "diagnostic sink failed" {})))))
      (doseq [name ["first" "second"]]
        (let [pending (d/transact! conn {:tx-data [[:db/add -1 :name name]]})
              [report port] (alts! [pending (timeout 5000)])]
          (is (= port pending) "the test deadline is not transaction cancellation")
          (is (map? report) "a throwing listener cannot strand the public result")))
      (is (= 2 @diagnostics) "the throwing diagnostic sink was actually exercised")
      (is (= 2 (count @reports)))
      (is (= #{["first"] ["second"]} (d/q '[:find ?name :where [_ :name ?name]] @conn)))
      (finally
        (trove/set-log-fn! previous-log-fn)
        (d/release conn)))))
