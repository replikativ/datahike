(ns datahike.test.backfill-range-test
  (:require [clojure.test :refer [deftest is]]
            [boring.core :as boring]
            [datahike.backfill.journal :as journal]))

(deftest incremental-replay-seeks-to-exact-frame-boundaries
  (let [empty-prefix (journal/create! {})]
    (try
      (let [first-prefix (journal/append! empty-prefix [:first])
            second-prefix (journal/append! first-prefix [:second])
            final-prefix (journal/append! second-prefix [:third])
            decode boring/decode
            decoded (atom 0)
            step (fn [acc value end] (conj acc [value end]))]
        (is (= [[:first (journal/end-cursor first-prefix)] [:second (journal/end-cursor second-prefix)]
                [:third (journal/end-cursor final-prefix)]]
               (journal/reduce-range final-prefix (journal/start-cursor final-prefix) step [])))
        (with-redefs [boring/decode (fn [& args] (swap! decoded inc) (apply decode args))]
          (is (= [[:third (journal/end-cursor final-prefix)]]
                 (journal/reduce-range final-prefix (journal/end-cursor second-prefix) step []))))
        (is (= 1 @decoded) "earlier payloads are not decoded when resuming")
        (is (= [] (journal/reduce-range final-prefix (journal/end-cursor final-prefix) step [])))
        (is (thrown-with-msg? Exception #"callback"
                              (journal/reduce-range final-prefix (journal/end-cursor first-prefix)
                                                    (fn [& _] (throw (Exception. "callback"))) nil)))
        (with-redefs [boring/decode (fn [& _] (throw (Exception. "decode")))]
          (is (thrown-with-msg? Exception #"decode"
                                (journal/reduce-range final-prefix (journal/end-cursor first-prefix) step []))))
        (is (= [[:second (journal/end-cursor second-prefix)]]
               (journal/reduce-range second-prefix (journal/end-cursor first-prefix) step []))
            "a retained prefix does not consume later accepted writes")
        (let [cursor (journal/reduce-range final-prefix (journal/start-cursor final-prefix)
                                           (fn [_ _ end] (reduced end)) nil)]
          (is (= (journal/end-cursor first-prefix) cursor))
          (is (= [[:second (journal/end-cursor second-prefix)] [:third (journal/end-cursor final-prefix)]]
                 (journal/reduce-range final-prefix cursor step []))))
        (doseq [cursor [-1 1.5 nil {} (Object.)]]
          (is (= :backfill.journal/invalid-cursor
                 (try (journal/reduce-range final-prefix cursor step [])
                      (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))))
      (finally (journal/dispose! empty-prefix)))))

(defn- error-type [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))

(deftest opaque-cursors-are-scoped-and-ordered-within-a-prefix
  (let [a (journal/create! {})
        b (journal/create! {})]
    (try
      (let [first-prefix (journal/append! a [:first])
            second-prefix (journal/append! first-prefix [:second])
            start (journal/start-cursor a)
            middle (journal/end-cursor first-prefix)
            end (journal/end-cursor second-prefix)]
        (is (not (map? middle)))
        (is (not (number? middle)))
        (is (= start (journal/start-cursor second-prefix)))
        (is (= 0 (journal/compare-cursors second-prefix middle
                                          (journal/end-cursor first-prefix))))
        (is (neg? (journal/compare-cursors second-prefix start middle)))
        (is (pos? (journal/compare-cursors second-prefix end middle)))
        (is (= [] (journal/reduce-range a start conj [])))
        (doseq [foreign [(journal/start-cursor b) (journal/end-cursor b)]]
          (is (= :backfill.journal/invalid-cursor
                 (error-type #(journal/reduce-range second-prefix foreign conj []))))
          (is (= :backfill.journal/invalid-cursor
                 (error-type #(journal/compare-cursors second-prefix start foreign)))))
        (is (= :backfill.journal/invalid-cursor
               (error-type #(journal/reduce-range first-prefix end conj []))))
        (is (= :backfill.journal/invalid-cursor
               (error-type #(journal/compare-cursors first-prefix start end)))))
      (finally (journal/dispose! a) (journal/dispose! b)))))

(deftest replacing-a-suffix-invalidates-its-cursors-not-the-retained-prefix
  (let [a (journal/create! {})]
    (try
      (let [retained (journal/append! a [:kept])
            retained-end (journal/end-cursor retained)
            abandoned (journal/append! retained [:old])
            stale-end (journal/end-cursor abandoned)
            replacement (journal/append! retained [:new])
            end (journal/end-cursor replacement)]
        ;; Same-size payload replacement must not make the old cursor valid.
        (is (not= stale-end end))
        (is (= :backfill.journal/invalid-cursor
               (error-type #(journal/reduce-range replacement stale-end conj []))))
        (is (= :backfill.journal/invalid-cursor
               (error-type #(journal/compare-cursors replacement retained-end stale-end))))
        (is (thrown? clojure.lang.ExceptionInfo (journal/end-cursor abandoned)))
        (is (thrown? clojure.lang.ExceptionInfo (journal/append! abandoned [:invalid])))
        (is (= [:new] (journal/reduce-range replacement retained-end
                                            (fn [acc value _] (conj acc value)) [])))
        (is (= [:kept] (journal/reduce-journal retained conj [])))
        (is (= retained-end (journal/end-cursor retained))))
      (finally (journal/dispose! a)))))
