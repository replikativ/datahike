(ns datahike.test.backfill-range-file-test
  (:require [clojure.test :refer [deftest is]]
            [boring.core :as boring]
            [datahike.backfill.journal.file :as journal])
  (:import [java.io RandomAccessFile]))

(defn- error-type [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))

(deftest suffix-replacement-invalidates-only-abandoned-boundaries
  (let [empty-prefix (journal/create! {})]
    (try
      (let [retained (journal/append! empty-prefix [:kept])
            retained-cursor (journal/end-cursor retained)
            abandoned (journal/append! retained [:old])
            abandoned-cursor (journal/end-cursor abandoned)
            replacement (journal/append! retained [:new])
            replacement-cursor (journal/end-cursor replacement)]
        (is (= (:end abandoned) (:end replacement)) "same byte position must not certify a rewritten frame")
        (is (not= abandoned-cursor replacement-cursor))
        (is (= retained-cursor (journal/end-cursor retained)))
        (is (= [:kept] (journal/reduce-journal retained conj [])))
        (is (= [:new] (journal/reduce-range replacement retained-cursor
                                            (fn [acc value _] (conj acc value)) [])))
        (is (= -1 (journal/compare-cursors replacement retained-cursor replacement-cursor)))
        (is (= (- (:end replacement) (:end retained))
               (journal/range-byte-size replacement retained-cursor replacement-cursor)))
        (doseq [operation [#(journal/reduce-range replacement abandoned-cursor
                                                  (fn [acc _ _] acc) nil)
                           #(journal/compare-cursors replacement retained-cursor abandoned-cursor)
                           #(journal/range-byte-size replacement retained-cursor abandoned-cursor)]]
          (is (= :backfill.journal/invalid-cursor (error-type operation))))
        (doseq [operation [#(journal/start-cursor abandoned)
                           #(journal/end-cursor abandoned)
                           #(journal/reduce-journal abandoned conj [])
                           #(journal/append! abandoned [:must-not-write])]]
          (is (= :backfill.journal/invalid-descriptor (error-type operation))))
        (is (= [:kept :new] (journal/reduce-journal replacement conj []))
            "rejecting an abandoned append must not truncate the live replacement"))
      (finally (journal/dispose! empty-prefix)))))

(deftest cursors-are-owned-opaque-boundaries-not-numeric-offsets
  (let [a (journal/create! {})
        b (journal/create! {})]
    (try
      (let [first-prefix (journal/append! a [:first])
            final-prefix (journal/append! first-prefix [:last])
            zero (journal/start-cursor final-prefix)
            first-end (journal/end-cursor first-prefix)
            final-end (journal/end-cursor final-prefix)]
        (is (not (map? zero)))
        (is (= 1 (count (set [zero (journal/start-cursor first-prefix)]))))
        (is (= 0 (journal/compare-cursors final-prefix zero (journal/start-cursor a))))
        (is (= 1 (journal/compare-cursors final-prefix final-end first-end)))
        (doseq [invalid [nil 0 (:end first-prefix) {:offset (:end first-prefix)}
                         (Object.) (journal/start-cursor b) final-end]]
          (is (= :backfill.journal/invalid-cursor
                 (error-type #(journal/reduce-range first-prefix invalid
                                                    (fn [acc _ _] acc) nil)))))
        (doseq [modified [(assoc first-prefix :end (inc (:end first-prefix)))
                          (assoc first-prefix :end 0)
                          (assoc first-prefix :datahike.backfill.journal.file/cursor
                                 {:offset (:end first-prefix)})]]
          (is (= :backfill.journal/invalid-descriptor
                 (error-type #(journal/end-cursor modified))))
          (is (= :backfill.journal/invalid-descriptor
                 (error-type #(journal/append! modified [:must-not-write])))))
        (is (= [:first :last] (journal/reduce-journal final-prefix conj [])))
        (is (= [] (journal/reduce-range final-prefix final-end
                                        (fn [acc value _] (conj acc value)) []))))
      (finally (journal/dispose! a) (journal/dispose! b)))))

(deftest range-validation-does-not-decode-earlier-payloads
  (let [empty-prefix (journal/create! {})]
    (try
      (let [first-prefix (journal/append! empty-prefix [:first])
            final-prefix (journal/append! first-prefix [:last])
            start (journal/end-cursor first-prefix)
            decode boring/decode
            decoded (atom 0)]
        (with-redefs [boring/decode (fn [& args] (swap! decoded inc) (apply decode args))]
          (is (= [:last] (journal/reduce-range final-prefix start
                                               (fn [acc value _] (conj acc value)) []))))
        (is (= 1 @decoded))
        (let [next-cursor (journal/reduce-range final-prefix start
                                                (fn [_ _ next] (reduced next)) nil)]
          (is (= (journal/end-cursor final-prefix) next-cursor)))
        (with-open [file (RandomAccessFile. (:path final-prefix) "rw")]
          (.seek file (:end first-prefix))
          (.writeLong file Long/MAX_VALUE))
        (is (= :backfill.journal/corrupt
               (error-type #(journal/reduce-range final-prefix start
                                                  (fn [acc _ _] acc) nil)))))
      (finally (journal/dispose! empty-prefix)))))
