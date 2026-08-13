(ns datahike.test.migrate-store-ownership-test
  "Who closes the store, and whether it is closed when the export FAILS.

   A konserve target comes in two shapes and the only difference between them is
   ownership: `{:store s}` is lent to us and must not be released, `{:backend …}`
   is opened by us and must be released on every path. That obligation is the
   entire cost of supporting the config form, so it is the thing worth a test.

   Specifically the FAILURE path. The success path passes whether or not the
   `finally` exists — `close` on the happy path is just the last statement — so
   a test that only exports successfully proves nothing about the obligation. A
   leaked store is a leaked connection pool, and on S3 or JDBC that is the
   expensive kind.

   Note the store is CREATED before it is used as a target. `connect-store` does
   not create: for `:file` and `:memory` it refuses a store that is not there.
   The first version of this test used a fresh config and proved nothing,
   because the failure happened inside `open` — where there is correctly nothing
   to release."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [konserve.store :as ks]))

(defn- source-conn
  "A small database to export."
  []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true :schema-flexibility :read}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn (vec (for [i (range 20)] {:name (str "p" i)})))
      conn)))

(defn- fresh-store-config []
  (let [cfg {:backend :file
             :path (str (System/getProperty "java.io.tmpdir") "/dh-own-" (System/nanoTime))
             :id (random-uuid)}]
    (ks/create-store cfg {:sync? true})
    cfg))

(defn- with-release-spy
  "Runs `f`, recording the config of every store datahike releases."
  [f]
  (let [released (atom [])
        orig     ks/release-store]
    (with-redefs [ks/release-store (fn [config store opts]
                                     (swap! released conj config)
                                     (orig config store opts))]
      [(try {:ok (f)} (catch Exception e {:err (ex-message e)})) @released])))

;; A transducer that fails once records are flowing — i.e. after the store is
;; open, which is the only point at which there is anything to leak.
(def ^:private boom (map (fn [_] (throw (ex-info "boom" {})))))

(deftest a-store-we-opened-is-released-even-when-the-export-fails
  (testing "the config form makes datahike the owner, so a failure must not leak
            the store."
    (let [conn (source-conn)
          cfg  (fresh-store-config)]
      (try
        (let [[result released] (with-release-spy
                                  #(m/export-transformed @conn (assoc cfg :prefix "owned")
                                                         boom {:history? true}))]
          (is (= "boom" (:err result)) "the export must actually have failed")
          (is (= 1 (count released))
              "released exactly once, on the failure path")
          (is (= cfg (first released))
              "and released with the config it was opened from — which is why
               the medium map carries `:config` at all"))
        (finally (d/release conn))))))

(deftest a-store-we-were-lent-is-never-released
  (testing "the mirror. Releasing a caller's store would close a pool another
            export may still be using; `:owned? false` is what prevents it.
            Checked on the failure path too, since that is where an over-eager
            cleanup would live."
    (let [conn  (source-conn)
          cfg   (fresh-store-config)
          store (ks/connect-store cfg {:sync? true})]
      (try
        (let [[result released] (with-release-spy
                                  #(m/export-transformed @conn {:store store :prefix "lent"}
                                                         boom {:history? true}))]
          (is (= "boom" (:err result)) "the export must actually have failed")
          (is (empty? released) "a lent store is not ours to close, on either path"))

        (testing "and it is still usable afterwards, which is the point"
          (let [[result released] (with-release-spy
                                    #(m/export-db @conn {:store store :prefix "lent-2"}
                                                  {:history? true}))]
            (is (map? (:ok result)) "the same store serves a second export")
            (is (empty? released))))
        (finally (d/release conn))))))
