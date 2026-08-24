(ns datahike.test.gc-safe-defaults-test
  "Long-running operations protect themselves with durable GC roots by DEFAULT —
   no API involved. Each test observes the root DURING the operation through the
   operation's own `:progress-fn` (deterministic — no sleeps), and asserts it is
   gone afterwards; where retention is claimed, the release is the control."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.blob :as blob]
            [datahike.gc :as gc]
            [datahike.gc-guard :as guard :refer [with-unreferenced-writes]]
            [datahike.gc-roots :as roots]
            [datahike.migrate :as m]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]
            [clojure.java.io :as io])
  (:import [java.util Date]))

(defn- fresh-cfg [& [extra]]
  (let [id (java.util.UUID/randomUUID)]
    (merge {:store {:backend :file
                    :path (str (System/getProperty "java.io.tmpdir") "/dh-gc-safe-" id)
                    :id id}
            :writer {:backend :self :writer-ownership :exclusive}
            :schema-flexibility :write
            :keep-history? false}
           extra)))

(def ^:private schema
  [{:db/ident :issue/title :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :issue/attachment :db/valueType :db.type/store-ref :db/cardinality :db.cardinality/one}])

(defn- delete-dir! [^java.io.File f]
  (when (.isDirectory f) (run! delete-dir! (.listFiles f)))
  (.delete f))

(defn- roots-now [store] (<?? S (roots/roots store)))

(deftest a-keys-only-root-protects-literal-store-keys
  (testing "a record with only :datahike.gc/keys — no trees — is a valid root"
    (let [cfg (fresh-cfg)]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (try
          (d/transact conn schema)
          (let [store (:store @conn)
                bytes (.getBytes "orphan bytes" "UTF-8")
                key (blob/blob-id bytes)
                _ (k/bassoc store key bytes {:sync? true})
                ;; The blob is reachable from nothing — exactly what an import
                ;; has restored before any datom names it.
                id (<?? S (roots/root! (d/db conn)
                                       {:kind :checkpoint
                                        :record {:meta {:datahike/commit-id (random-uuid)}
                                                 :datahike.gc/keys #{key}}}))]
            (is (not (contains? (set (<?? S (d/gc-storage conn (Date.) {:min-age-ms 0}))) key))
                "the sweep spares a key the root names literally")
            (is (contains? (<?? S (gc/reachable-store-refs (d/db conn) (Date.))) key)
                "and the application's blob-sweep set includes it")
            (<?? S (roots/release! (d/db conn) id))
            (is (contains? (set (<?? S (d/gc-storage conn (Date.) {:min-age-ms 0}))) key)
                "control: collected the moment the root is gone"))
          (finally (d/release conn) (d/delete-database cfg)))))))

(defn- observing-progress
  "A :progress-fn that records, per phase, the registry as the operation saw it."
  [store seen]
  (fn [{:keys [phase]}]
    (swap! seen assoc phase (roots-now store))))

(deftest export-pins-its-snapshot-for-the-exports-duration
  (let [cfg (fresh-cfg)
        dir (str (System/getProperty "java.io.tmpdir") "/dh-gc-safe-dump-" (random-uuid))]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn schema)
        (d/transact conn [{:issue/title "one"}])
        (let [seen (atom {})
              snapshot-cid (get-in (d/db conn) [:meta :datahike/commit-id])]
          (m/export-db (d/db conn) dir {:progress-fn (observing-progress (:store @conn) seen)})
          (is (seq @seen) "precondition: the export reported progress")
          (let [mid (mapcat vals (vals @seen))]
            (is (seq mid) "a root existed at every observed phase")
            (is (every? #(and (= :pin (:kind %)) (= snapshot-cid (:commit-id %))) mid)
                "and it pins the exported snapshot's commit"))
          (is (empty? (roots-now (:store @conn))) "released when the export ends"))
        (finally
          (d/release conn) (d/delete-database cfg) (delete-dir! (io/file dir)))))))

(deftest import-roots-its-restored-blobs-until-the-last-commit
  (let [src-cfg (fresh-cfg)
        dst-cfg (fresh-cfg)
        dir (str (System/getProperty "java.io.tmpdir") "/dh-gc-safe-dump-" (random-uuid))]
    (d/create-database src-cfg)
    (d/create-database dst-cfg)
    (let [src (d/connect src-cfg)
          dst (d/connect dst-cfg)]
      (try
        (d/transact src schema)
        (let [bytes (.getBytes "carried bytes" "UTF-8")
              key (blob/blob-id bytes)]
          (with-unreferenced-writes (:id (:store src-cfg))
            (k/bassoc (:store @src) key bytes {:sync? true})
            (d/transact src [{:issue/title "carried" :issue/attachment key}]))
          (m/export-db @src dir {})
          (let [seen (atom {})]
            (m/import-db dst dir {:progress-fn (observing-progress (:store @dst) seen)})
            (is (seq @seen) "precondition: the import reported progress")
            (let [mid (mapcat vals (vals @seen))]
              (is (seq mid) "a root existed while the import ran")
              (is (every? #(= :checkpoint (:kind %)) mid))
              (is (every? (fn [entry]
                            (contains? (set (:datahike.gc/keys
                                             (k/get (:store @dst) (:record-key entry) nil {:sync? true})))
                                       key))
                          mid)
                  "and its record names the restored blob"))
            (is (empty? (roots-now (:store @dst))) "released once the import committed")
            (is (= key (d/q '[:find ?k . :where [_ :issue/attachment ?k]] (d/db dst))))))
        (finally
          (d/release src) (d/release dst)
          (d/delete-database src-cfg) (d/delete-database dst-cfg)
          (delete-dir! (io/file dir)))))))

(deftest bulk-index-build-checkpoints-completed-families
  (let [src-cfg (fresh-cfg)
        dst-cfg (fresh-cfg)
        dir (str (System/getProperty "java.io.tmpdir") "/dh-gc-safe-dump-" (random-uuid))]
    (d/create-database src-cfg)
    (d/create-database dst-cfg)
    (let [src (d/connect src-cfg)
          dst (d/connect dst-cfg)]
      (try
        (d/transact src [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        (d/transact src (mapv (fn [i] {:n i}) (range 500)))
        (m/export-db @src dir {})
        (let [store (:store @dst)
              seen (atom {})
              checkpoints (atom [])
              orig-set roots/set-record!
              probe (fn [{:keys [phase]}]
                      (swap! seen assoc phase (roots-now store)))]
          ;; The record grows between progress phases, so sample the writes
          ;; themselves rather than hoping a phase lands mid-build. And make the
          ;; root EARN the retention: neuter the in-process safe point so this
          ;; process's collector behaves like one in another process, then run a
          ;; full unfloored collection after the first family lands. Only the
          ;; checkpoint stands between the sweep and the unpublished trees; if
          ;; it fails to protect them, publish lands on swept nodes and the
          ;; queries below cannot answer.
          (let [gced? (atom false)]
            (with-redefs [roots/set-record! (fn [db id record & [opts]]
                                              (swap! checkpoints conj record)
                                              (let [r (orig-set db id record (or opts {:sync? true}))]
                                                (when (and (:eavt-key record)
                                                           (compare-and-set! gced? false true))
                                                  (with-redefs [guard/safe-point
                                                                (fn [_] (java.util.Date.))]
                                                    (<?? S (d/gc-storage dst (Date.) {:min-age-ms 0}))))
                                                r))]
              (m/import-db dst dir {:build-indexes? true :progress-fn probe}))
            (is @gced? "precondition: a full collection ran mid-build, after the first family"))
          (is (seq @seen) "precondition: the build reported progress")
          (is (some #(seq (vals %)) (vals @seen))
              "a checkpoint root existed during the build")
          (is (some :eavt-key @checkpoints)
              "the checkpoint record gained a completed family's tree")
          (is (empty? (roots-now store)) "released once the build published")
          (is (= 500 (count (d/q '[:find ?e :where [?e :n _]] (d/db dst))))
              "the published db answers — the checkpoint held through the sweep"))
        (finally
          (d/release src) (d/release dst)
          (d/delete-database src-cfg) (d/delete-database dst-cfg)
          (delete-dir! (io/file dir)))))))
