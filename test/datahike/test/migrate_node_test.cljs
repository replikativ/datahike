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

   A konserve store and `:sort? false`: the filesystem medium exists here too
   (`datahike.migrate.fs` runs on Node), but the external merge sort is JVM-only
   — its k-way merge is a lazy seq over open files, which cannot pull from async
   IO — so the portable export is the no-scratch one."
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

(deftest the-external-sort-is-refused-by-name
  (testing "`:sort? true` needs the JVM-only external merge sort. It must say so
            rather than failing inside `msort`, which does not exist here."
    (async done
           (go
             (let [cfg {:store {:backend :memory :id (random-uuid)}
                        :keep-history? true :schema-flexibility :write}]
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)
                     store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                                {:sync? false}))
                     v (<! (m/export-db @conn {:store store :prefix "sorted"}
                                        {:sort? true}))]
                 (is (instance? js/Error v) "refused")
                 (is (= :export/sort-not-portable (:error (ex-data v)))))
               (<! (d/delete-database cfg))
               (done))))))
