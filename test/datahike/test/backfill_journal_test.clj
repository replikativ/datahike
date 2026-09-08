(ns datahike.test.backfill-journal-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.backfill.journal :as journal]))

(deftest accepted-prefixes-and-owner-lifetime
  (let [empty-prefix (journal/create! {})]
    (try
      (let [first-prefix (journal/append! empty-prefix [:first])
            second-prefix (journal/append! first-prefix [:second])]
        (is (= (journal/descriptor-id empty-prefix)
               (journal/descriptor-id first-prefix)
               (journal/descriptor-id second-prefix)))
        (is (= [] (journal/reduce-journal empty-prefix conj [])))
        (is (= [:first] (journal/reduce-journal first-prefix conj [])))
        (is (= [:first :second] (journal/reduce-journal second-prefix conj [])))
        (is (= :done (journal/reduce-journal second-prefix
                                             (fn [_ _] (reduced :done)) nil)))
        (is (thrown-with-msg? Exception #"reader failed"
                              (journal/reduce-journal second-prefix
                                                      (fn [_ _] (throw (Exception. "reader failed"))) nil)))
        (is (= [:first :second] (journal/reduce-journal second-prefix conj []))))
      (finally (journal/dispose! empty-prefix)))
    (is (nil? (journal/dispose! empty-prefix)))))

(deftest failed-capture-does-not-accept-a-partial-batch
  (let [empty-prefix (journal/create! {:max-frame-bytes 64})]
    (try
      (let [accepted (journal/append! empty-prefix [:accepted])]
        (is (thrown? Exception
                     (journal/append! accepted [:unaccepted (apply str (repeat 1000 "x"))])))
        (is (= [:accepted] (journal/reduce-journal accepted conj [])))
        (let [replacement (journal/append! accepted [:retry])]
          (is (= [:accepted :retry] (journal/reduce-journal replacement conj [])))
          (is (= [:accepted] (journal/reduce-journal accepted conj [])))))
      (finally (journal/dispose! empty-prefix)))))

(deftest an-abandoned-suffix-is-not-replayed-by-a-replacement
  (let [empty-prefix (journal/create! {})]
    (try
      (let [accepted (journal/append! empty-prefix [:accepted])]
        ;; Intentionally abandon this result; no report or reader retains it.
        (journal/append! accepted [:abandoned])
        (let [replacement (journal/append! accepted [:replacement])]
          (is (= [:accepted :replacement] (journal/reduce-journal replacement conj [])))
          (is (= [:accepted] (journal/reduce-journal accepted conj [])))))
      (finally (journal/dispose! empty-prefix)))))
