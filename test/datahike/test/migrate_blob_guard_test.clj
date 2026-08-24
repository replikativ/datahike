(ns datahike.test.migrate-blob-guard-test
  "`import-db` restores a dump's carried blobs BEFORE any datom names them, and
   the datom that finally does may sit in the last batch. For the whole import a
   blob is therefore an object reachable from nothing — precisely what a
   concurrent sweep deletes. This pins that the import holds ONE guard from the
   first blob write to the last commit, on both media.

   Probed with a konserve write hook, like `commit-holds-the-guard`: hooks fire
   on the real write path. Asserted with `guard/safe-point`, not `in-flight?`:
   every commit inside the import opens its own short guard, so `in-flight?`
   would be true at every write even if the outer guard were released right
   after the blobs. `safe-point` is the START of the OLDEST open sequence — one
   guard spanning the whole import makes it a constant across every write, and
   a constant no later than the blob write itself."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.blob :as blob]
            [datahike.gc-guard :as guard :refer [with-unreferenced-writes]]
            [datahike.migrate :as m]
            [konserve.core :as k]
            [konserve.store :as ks]
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

(defn- source-with-blob
  "A database holding one carried blob; returns [conn cfg blob-key]."
  []
  (let [cfg (fresh-cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        bytes (.getBytes "attachment bytes" "UTF-8")
        key (blob/blob-id bytes)]
    (d/transact conn schema)
    (with-unreferenced-writes (:id (:store cfg))
      (k/bassoc (:store @conn) key bytes {:sync? true})
      (d/transact conn [{:issue/title "carried" :issue/attachment key}]))
    ;; A few more commits so the import has several batches after the blob.
    (dotimes [i 3] (d/transact conn [{:issue/title (str "filler " i)}]))
    [conn cfg key]))

(defn- import-observing
  "Import `target` into a fresh database while a write hook records, for every
   store write, the guard's safe point at that instant. Returns the observed
   writes."
  [target key]
  (let [cfg (fresh-cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        sid (:id (:store cfg))
        store (:store @conn)
        observed (atom [])]
    (try
      (is (not (guard/in-flight? sid)) "nothing is open before the import")
      (k/add-write-hook! store ::probe
                         (fn [msg]
                           (swap! observed conj {:key (:key msg)
                                                 :api-op (:api-op msg)
                                                 :in-flight? (guard/in-flight? sid)
                                                 :safe-point (guard/safe-point sid)})))
      (m/import-db conn target {})
      (k/remove-write-hook! store ::probe)
      (is (not (guard/in-flight? sid))
          "and the guard is released once the import has committed")
      (is (= key (d/q '[:find ?k . :where [_ :issue/attachment ?k]] (d/db conn)))
          "the imported datom names the blob")
      @observed
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(defn- assert-one-guard-spans-everything [observed key]
  (let [blob-write (first (filter #(= key (:key %)) observed))
        safe-points (map :safe-point observed)]
    (is blob-write "precondition: the import wrote the carried blob to the target store")
    (is (< 1 (count (distinct (map :key observed))))
        "precondition: there were writes other than the blob (the commits)")
    (is (every? :in-flight? observed)
        (str "unguarded writes: " (pr-str (map :key (remove :in-flight? observed)))))
    (is (= 1 (count (distinct safe-points)))
        (str "one guard must span every write; the safe point moved, which means the "
             "outer guard closed and later writes were covered only by per-commit guards: "
             (pr-str (distinct safe-points))))
    (is (<= (.getTime ^java.util.Date (first safe-points))
            (.getTime ^java.util.Date (:safe-point blob-write)))
        "and that guard was already open when the blob was written")))

(deftest import-holds-one-guard-from-blob-restore-to-last-commit
  (let [[src src-cfg key] (source-with-blob)
        dir (str (System/getProperty "java.io.tmpdir") "/dh-blob-guard-dump-" (java.util.UUID/randomUUID))
        mem (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
        store-target {:store mem :prefix "blob-guard"}]
    (try
      (m/export-db @src dir {})
      (m/export-db @src store-target {})
      (is (.exists (io/file dir "store-refs")) "precondition: the filesystem dump carries the blob")
      (testing "filesystem medium"
        (assert-one-guard-spans-everything (import-observing dir key) key))
      (testing "konserve-store medium"
        (assert-one-guard-spans-everything (import-observing store-target key) key))
      (finally
        (d/release src)
        (d/delete-database src-cfg)
        (delete-dir! (io/file dir))))))
