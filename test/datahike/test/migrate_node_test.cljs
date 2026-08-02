(ns datahike.test.migrate-node-test
  "Export and import, running on Node.

   This is what the whole `async+sync` conversion was for. Everything else
   proved the SHAPE was right — that no IO hides inside a closure the `go` state
   machine cannot enter, that retries do not park inside a `catch`, that a store
   is not released before it is read. All of that was verified on the JVM, where
   a blocking take makes failures observable.

   What only Node can prove is the platform half: that `format`, `*err*`,
   `Class/forName`, `Runtime` and blocking derefs are genuinely gone from every
   path an export or import touches, and that the konserve store medium works
   under a runtime with no synchronous mode at all.

   Both orderings run here. `:sort? false` needs no scratch space at all;
   `:sort? true` spills sorted runs to local files and k-way merges them, which
   works on Node because every read in the merge is a synchronous local file
   read (`fs.readSync`) and no channel op ever occurs inside it."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.core.async :refer [go <!]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [konserve.store :as ks]))

(defn- take-or-throw [v]
  (if (instance? js/Error v) (throw v) v))

(deftest export-and-import-round-trip-on-node
  (async done
         (go
           (let [cfg {:store {:backend :memory :id (random-uuid)}
                      :keep-history? true :schema-flexibility :write}]
             (try
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)]
                 (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                         :db/cardinality :db.cardinality/one
                                         :db/unique :db.unique/identity}
                                        {:db/ident :n :db/valueType :db.type/long
                                         :db/cardinality :db.cardinality/one}]))
                 (<! (d/transact! conn [{:name "a" :n 1} {:name "b" :n 2}]))
                 (let [store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                  {:sync? false}))
                       target {:store store :prefix "node-dump"}
                       man (take-or-throw
                            (<! (m/export-db @conn target {:history? true :sort? false})))]
                   (is (map? man) "export returned a manifest")
                   (is (pos? (count (:chunks man))) "and wrote chunks")
                   (is (= :gzip (:compression man)) "compressed by default, on Node too")
                   (is (every? #(re-find #"\.cbor\.gz$" (:file %)) (:chunks man)))

                   (testing "and it imports back into a fresh database"
                     (let [c2 {:store {:backend :memory :id (random-uuid)}
                               :keep-history? true :schema-flexibility :write}]
                       (<! (d/create-database c2))
                       (let [tgt (d/connect c2)
                             rep (take-or-throw (<! (m/import-db tgt target {})))]
                         (is (= (:count (:semantic-digest man)) (:datom-count rep))
                             "every datom landed")
                         (is (true? (:verified? rep)) "and post-import verification passed")
                         (is (= 2 (:tx-count rep)) "schema + data")
                         (is (= #{"a" "b"}
                                (set (map first (d/q '[:find ?n :where [?e :name ?n]] @tgt))))
                             "and the data is queryable"))
                       (<! (d/delete-database c2))))))
               (catch js/Error e
                 (is false (str "node round trip failed: " (.-message e)
                                "\nSTACK:\n" (.-stack e)))))
             (<! (d/delete-database cfg))
             (done)))))

(deftest sync-is-refused-by-name-on-node
  (testing "there is no blocking take here, so `:sync? true` cannot be honoured.
            Refusing by name beats failing deep inside the batcher with something
            about IDeref — the same choice `api/transact` makes."
    (async done
           (go
             (let [cfg {:store {:backend :memory :id (random-uuid)}
                        :keep-history? true :schema-flexibility :write}]
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)
                     store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                {:sync? false}))
                     target {:store store :prefix "refuse"}]
                 (is (thrown-with-msg? js/Error #"not supported in ClojureScript"
                                       (m/export-db @conn target {:sync? true})))
                 (is (thrown-with-msg? js/Error #"not supported in ClojureScript"
                                       (m/import-db conn target {:sync? true}))))
               (<! (d/delete-database cfg))
               (done))))))

(deftest the-external-sort-runs-on-node
  (testing "`:sort? true` used to be refused by name here, on the theory that a
            k-way merge over open files could not work off the JVM. The file
            handles were the portable part; `java.io.File` and
            `java.util.PriorityQueue` were not. With those gone the sort runs on
            Node, spilling runs through `fs.readSync` — so the two runtimes
            offer the same two orderings rather than Node offering one.

            A round trip rather than 'it did not throw': the sort rewrites the
            record stream, so the way it fails is a dump that is subtly
            misordered, which only reading it back detects.

            `:sort-buffer` is deliberately tiny so the export spills MANY runs
            and actually exercises the merge. At the default of 1,000,000 this
            database fits in one run and the k-way merge never runs at all."
    (async done
           (go
             (let [cfg {:store {:backend :memory :id (random-uuid)}
                        :keep-history? true :schema-flexibility :write}]
               (try
                 (<! (d/create-database cfg))
                 (let [conn (d/connect cfg)]
                   (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                           :db/cardinality :db.cardinality/one
                                           :db/unique :db.unique/identity}
                                          {:db/ident :n :db/valueType :db.type/long
                                           :db/cardinality :db.cardinality/one}]))
                   ;; several transactions, so transaction order is a real
                   ;; constraint the sort has to establish rather than inherit
                   (doseq [i (range 8)]
                     (<! (d/transact! conn [{:name (str "e" i) :n i}])))
                   ;; a card-one overwrite: the retract and the assert must come
                   ;; out retract-first, which is the ordering the sort exists for
                   (<! (d/transact! conn [{:name "e0" :n 99}]))
                   (let [store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                    {:sync? false}))
                         target {:store store :prefix "sorted"}
                         man (take-or-throw
                              (<! (m/export-db @conn target {:history? true
                                                             :sort? true
                                                             :sort-buffer 4})))]
                     (is (map? man) "the sorted export produced a manifest")
                     (is (pos? (count (:chunks man))) "and wrote chunks")

                     (testing "and the sorted dump imports back correctly"
                       (let [c2 {:store {:backend :memory :id (random-uuid)}
                                 :keep-history? true :schema-flexibility :write}]
                         (<! (d/create-database c2))
                         (let [tgt (d/connect c2)
                               rep (take-or-throw (<! (m/import-db tgt target {})))]
                           (is (= (:count (:semantic-digest man)) (:datom-count rep))
                               "every datom landed")
                           (is (true? (:verified? rep))
                               "and post-import verification passed")
                           (is (= 99 (ffirst (d/q '[:find ?n :where
                                                    [?e :name "e0"] [?e :n ?n]]
                                                  @tgt)))
                               "the overwritten value is the surviving one, so the
                                retract and assert were not transposed"))
                         (<! (d/delete-database c2))))))
                 (catch js/Error e
                   (is false (str "node sorted round trip failed: " (.-message e)
                                  "\nSTACK:\n" (.-stack e)))))
               (<! (d/delete-database cfg))
               (done))))))
