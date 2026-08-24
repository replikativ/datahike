(ns datahike.test.migrate-blob-guard-test
  "`import-db` restores a dump's carried blobs BEFORE any datom names them, and
   the datom that finally does may sit in the last batch. For the whole import a
   blob is therefore an object reachable from nothing — precisely what a
   concurrent sweep deletes. This pins that the import holds the GC guard from
   the first blob write to the last commit, on both media.

   Probed with a konserve write hook, like `commit-holds-the-guard`: hooks fire
   on the real write path, and `guard/in-flight?` is the question (a timestamp
   comparison cannot tell a held guard from one opened in the same millisecond)."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.blob :as blob]
            [datahike.gc-guard :as guard :refer [with-unreferenced-writes]]
            [datahike.migrate :as m]
            [konserve.core :as k]
            [clojure.java.io :as io]))

(def ^:private schema
  [{:db/ident :issue/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :issue/attachment
    :db/valueType :db.type/store-ref
    :db/cardinality :db.cardinality/one}])

(defn- fresh-cfg []
  (let [id (java.util.UUID/randomUUID)]
    {:store {:backend :file
             :path (str (System/getProperty "java.io.tmpdir") "/dh-blob-guard-" id)
             :id id}
     :schema-flexibility :write
     :keep-history? false}))

(defn- delete-dir! [^java.io.File f]
  (when (.isDirectory f) (run! delete-dir! (.listFiles f)))
  (.delete f))

(deftest import-holds-the-guard-across-blob-restore
  (let [src-cfg (fresh-cfg)
        dst-cfg (fresh-cfg)
        dir (str (System/getProperty "java.io.tmpdir") "/dh-blob-guard-dump-"
                 (java.util.UUID/randomUUID))]
    (d/create-database src-cfg)
    (d/create-database dst-cfg)
    (let [src (d/connect src-cfg)
          dst (d/connect dst-cfg)]
      (try
        (d/transact src schema)
        (let [bytes (.getBytes "attachment bytes" "UTF-8")
              key (blob/blob-id bytes)
              sid (:id (:store src-cfg))]
          (with-unreferenced-writes sid
            (k/bassoc (:store @src) key bytes {:sync? true})
            (d/transact src [{:issue/title "carried" :issue/attachment key}]))
          (m/export-db @src dir {})
          (is (.exists (io/file dir "store-refs"))
              "precondition: the dump carries the blob")

          (testing "every store write the import makes happens with the guard open"
            (let [store (:store @dst)
                  dst-sid (:id (:store dst-cfg))
                  observed (atom [])]
              (is (not (guard/in-flight? dst-sid)) "nothing is open before the import")
              (k/add-write-hook! store ::probe
                                 (fn [msg]
                                   (swap! observed conj {:key (:key msg)
                                                         :api-op (:api-op msg)
                                                         :guarded? (guard/in-flight? dst-sid)})))
              (m/import-db dst dir {})
              (k/remove-write-hook! store ::probe)
              (is (some #(= key (:key %)) @observed)
                  "precondition: the import wrote the carried blob to the target store")
              (is (every? :guarded? @observed)
                  (str "unguarded writes: " (pr-str (map :key (remove :guarded? @observed)))))
              (is (not (guard/in-flight? dst-sid))
                  "and the guard is released once the import has committed")
              (is (= key (d/q '[:find ?k . :where [_ :issue/attachment ?k]] (d/db dst)))
                  "the imported datom names the blob"))))
        (finally
          (d/release src)
          (d/release dst)
          (d/delete-database src-cfg)
          (d/delete-database dst-cfg)
          (delete-dir! (io/file dir)))))))
