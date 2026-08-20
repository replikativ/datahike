(ns datahike.test.migrate-export-sink-test
  "`export-to-sink` — the mirror of `import-source`, and the half that did not exist.

   A caller could supply records TO datahike and could not receive them FROM it:
   `export-db` accepted a filesystem path or a konserve store and that was the
   whole set, with the two writers reached through seven `store-target?` forks.
   So \"datahike imports and exports from anywhere\" was half true.

   These pin the properties a sink relies on and that nothing else checks:

     * the records are the SAME ones `export-db` writes, in the same order —
       shared through `export-record-seq`, so `:xform` and the `:sort?` builder
       choice cannot drift between the two;
     * chunks are TRANSACTION-ALIGNED, which `export-db`'s own chunking does not
       do (a dump chunk is a byte range whose reader reassembles the stream).
       A live-target sink transacts what it is handed, so a split transaction
       would be a fragment committed on its own;
     * `:close` runs on the FAILURE path, and not from a `finally`.

   The round trip is the real test: out through the sink, back in through
   `import-source`, no dump anywhere."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]
            [clojure.core.async :as a]
            [clojure.set]))

(defn- fresh-conn [& {:keys [history?] :or {history? true}}]
  (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                   :keep-history? history?
                   :schema-flexibility :write}))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- populate!
  "Schema, two entities with a ref between them, then a cardinality-one
   overwrite so history is non-trivial."
  [conn]
  (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident :pal :db/valueType :db.type/ref
                     :db/cardinality :db.cardinality/one}])
  (let [r (d/transact conn [{:db/id -1 :name "Ann"} {:db/id -2 :name "Bob" :pal -1}])]
    (d/transact conn [{:db/id (get-in r [:tempids -1]) :name "Anna"}]))
  conn)

(defn- collecting-sink [seen]
  {:open  (fn [_opts] {:n 0})
   :write (fn [ctx recs] (swap! seen conj recs) (update ctx :n + (count recs)))
   :close (fn [ctx] {:total (:n ctx)})})

(defn- snapshot [conn]
  {:names (sort (map first (d/q '[:find ?n :where [?e :name ?n]] @conn)))
   :hist  (sort (map (juxt first second)
                     (d/q '[:find ?n ?op :where [?e :name ?n _ ?op]] (d/history @conn))))
   :pal   (d/q '[:find ?a ?b :where [?x :pal ?y] [?x :name ?a] [?y :name ?b]] @conn)})

(deftest a-sink-receives-the-dump-stream-in-dump-order
  (testing "the same records export-db writes, in the same
            `(t, txInstant-first, e, a, op)` order"
    (let [conn (populate! (fresh-conn))
          seen (atom [])
          result (m/export-to-sink @conn (collecting-sink seen)
                                   {:history? true :chunk-size 3})
          all (vec (apply concat @seen))]
      (is (= {:total (count all)} result)
          "export-to-sink returns whatever :close returned")
      (is (pos? (count all)) "precondition: something was exported")
      (is (= (map #(nth % 3) all) (sort (map #(nth % 3) all)))
          "`t` ascending")
      (is (every? (fn [g] (= :db/txInstant (nth (first g) 1)))
                  (partition-by #(nth % 3) all))
          ":db/txInstant leads every transaction")
      (is (every? (fn [r] (and (= 5 (count r)) (keyword? (nth r 1)) (boolean? (nth r 4)))) all)
          "records are [e a v t op] with a keyword attribute and a boolean op")
      (teardown conn))))

(deftest chunks-are-transaction-aligned
  (testing "`:chunk-size` is a MINIMUM here — a chunk grows to the next change of
            `t`. export-db's own chunking does NOT do this; a dump chunk is a
            byte range and its reader reassembles the stream. A sink transacts
            what it is handed, so a split transaction would be a fragment
            committed on its own."
    (let [conn (populate! (fresh-conn))
          seen (atom [])]
      (m/export-to-sink @conn (collecting-sink seen) {:history? true :chunk-size 1})
      (is (< 1 (count @seen)) "precondition: it actually chunked")
      (let [ts (mapv (fn [c] (into #{} (map #(nth % 3)) c)) @seen)]
        (is (= (reduce + (map count ts)) (count (apply clojure.set/union ts)))
            (str "no `t` appears in two chunks, got "
                 (pr-str (mapv (fn [c] (mapv #(nth % 3) c)) @seen)))))
      (is (some #(> (count %) 1) @seen)
          "and chunk-size 1 overshoots rather than cutting a transaction in half")
      (teardown conn))))

(deftest out-through-the-sink-and-back-in-through-import-source
  (testing "the two seams compose with no dump, no filesystem and no konserve
            between them — which is the whole point of having both"
    (let [src (populate! (fresh-conn))
          captured (atom [])
          _ (m/export-to-sink @src {:open (fn [_] nil)
                                    :write (fn [_ recs] (swap! captured into recs) nil)
                                    :close (fn [_] :done)}
                              {:history? true :chunk-size 3})
          tgt (fresh-conn)
          rep (m/import-source tgt (m/records->chunk-src @captured 3)
                               {:source-meta {:history? true
                                              :expected-count (count @captured)}})]
      (is (true? (:verified? rep)))
      (is (= (snapshot src) (snapshot tgt))
          "same current values, same history, same refs")
      (teardown src)
      (teardown tgt))))

(deftest close-runs-on-the-failure-path-with-the-latest-context
  (testing "and NOT from a `finally`: in async mode this function returns a
            channel, so a `finally` would fire the moment it is handed back —
            before a single record had been written. `import-db` carries the same
            scar, where that read every chunk against a released store and still
            reported `:verified? true`.

            The context matters as much as the count. `:write` returns a NEW
            context, so a sink rotating a resource through it — a file handle, an
            open transaction — must be handed the LATEST one to close. An earlier
            version closed `ctx0`, which this sink's changing context catches and
            a constant one never would."
    (let [conn (populate! (fresh-conn))
          closed (atom [])
          sink {:open  (fn [_] {:gen 0})
                :write (fn [ctx _] (if (>= (:gen ctx) 1)
                                     (throw (ex-info "sink blew up" {:boom true}))
                                     (update ctx :gen inc)))
                :close (fn [ctx] (swap! closed conj (:gen ctx)) :closed)}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"sink blew up"
                            (m/export-to-sink @conn sink {:history? true :chunk-size 1}))
          "the sink's failure reaches the caller")
      (is (= 1 (count @closed)) ":close ran exactly once on the way out")
      (is (= [1] @closed)
          "and received the context the last successful :write returned, not :open's")
      (teardown conn))))

(deftest a-close-failure-does-not-mask-the-error-that-caused-it
  (testing "a sink whose write fails and whose close then also fails must report
            the WRITE failure — the close error is the consequence, and surfacing
            it instead would send someone to debug the wrong thing."
    (let [conn (populate! (fresh-conn))
          sink {:open  (fn [_] :ctx)
                :write (fn [_ _] (throw (ex-info "the real problem" {})))
                :close (fn [_] (throw (ex-info "close also failed" {})))}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"the real problem"
                            (m/export-to-sink @conn sink {:history? true})))
      (teardown conn))))

(deftest a-close-failure-on-the-success-path-surfaces-once
  (testing "close is called ONCE. An earlier version closed inside the loop's
            success branch AND in the catch, so a throwing :close saw its own
            exception and ran a second time."
    (let [conn (populate! (fresh-conn))
          calls (atom 0)
          sink {:open  (fn [_] :ctx)
                :write (fn [ctx _] ctx)
                :close (fn [_] (swap! calls inc) (throw (ex-info "close failed" {})))}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"close failed"
                            (m/export-to-sink @conn sink {:history? true})))
      (is (= 1 @calls) ":close ran once, not twice")
      (teardown conn))))

(deftest a-malformed-sink-is-refused-before-anything-is-read
  (let [conn (populate! (fresh-conn))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"must supply :open, :write and :close"
                          (m/export-to-sink @conn {:open (fn [_] nil)} {})))
    (teardown conn)))

(deftest the-async-path-works
  (testing "`:open`/`:write`/`:close` may each do IO, so each is awaited; under
            `:sync? false` each must return a channel. `default-sync?` is FALSE
            on ClojureScript, so this is the default path there."
    (let [conn (populate! (fresh-conn))
          seen (atom [])
          result (a/<!! (m/export-to-sink
                         @conn
                         {:open  (fn [_] (a/go {:n 0}))
                          :write (fn [ctx recs] (a/go (swap! seen conj recs)
                                                      (update ctx :n + (count recs))))
                          :close (fn [ctx] (a/go {:total (:n ctx)}))}
                         {:history? true :chunk-size 3 :sync? false}))]
      (is (not (instance? Throwable result))
          (str "async export failed: "
               (when (instance? Throwable result) (ex-message result))))
      (is (= (:total result) (count (apply concat @seen))))
      (teardown conn))))
