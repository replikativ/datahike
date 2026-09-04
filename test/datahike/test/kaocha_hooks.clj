(ns datahike.test.kaocha-hooks
  "Kaocha hooks keeping process-wide state from leaking between namespaces."
  (:require [datahike.query.resolve :as qr]))

(defn reset-query-resolver
  "`routes/handler` and `start-server` set the safe query function resolver
   process-wide, and a test that never releases its handler leaves it there
   for every namespace after it. Start each namespace with the embedded
   default."
  [testable _test-plan]
  (when (= :kaocha.type/ns (:kaocha.testable/type testable))
    (alter-var-root #'qr/*symbol-resolver* (constantly qr/permissive-symbol-resolver)))
  testable)
