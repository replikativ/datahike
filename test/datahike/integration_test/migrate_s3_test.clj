(ns datahike.integration-test.migrate-s3-test
  "Round-trips a dump through a REAL S3-compatible store — Garage in docker — so
   the store target is exercised over the actual S3 wire protocol (endpoint
   override + path-style addressing + SigV4), not the in-memory konserve
   stand-in.

   Garage (garagehq.deuxfleurs.fr) is chosen over MinIO because it is actively
   maintained open source; nothing below is Garage-specific beyond the
   provisioning CLI — any S3-compatible target works. The config file is
   injected with `docker cp` (no volume mounts), so the test runs on any docker
   host. Skips cleanly when docker is unavailable."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [konserve-s3.core]  ;; registers the :s3 konserve backend
            [konserve.store :as ks]))

(def ^:private s3-port 3910)
(def ^:private container "dh-migrate-garage-test")
;; Pinned to the exact release this test was validated against.
(def ^:private image "dxflrs/garage:v2.3.0")
(def ^:private bucket "dh-migrate-test")

(def ^:private garage-toml
  (str "metadata_dir = \"/var/lib/garage/meta\"\n"
       "data_dir = \"/var/lib/garage/data\"\n"
       "db_engine = \"sqlite\"\n"
       "replication_factor = 1\n"
       "rpc_bind_addr = \"[::]:3901\"\n"
       ;; fixed test-only secret (any 32 bytes of hex)
       "rpc_secret = \"a3298f1d5b94b5bfae25f31e5cdd63b06b2b3b7d3e1f0a52c7d94d3e1f0a52c7\"\n"
       "[s3_api]\n"
       ;; a real AWS region name so the client's region enum resolves
       "s3_region = \"us-east-1\"\n"
       "api_bind_addr = \"[::]:3900\"\n"
       "root_domain = \".s3.garage.localhost\"\n"))

(defn- docker? []
  (try (zero? (:exit (sh "docker" "info"))) (catch Exception _ false)))

(defn- garage! [& args]
  (apply sh "docker" "exec" container "/garage" args))

(defn- wait-until
  "Poll `f` (a fn returning truthy on success) every 500ms, up to `n` times."
  [n f]
  (loop [n n]
    (when (pos? n)
      (or (f) (do (Thread/sleep 500) (recur (dec n)))))))

(defn- start-garage! []
  (sh "docker" "rm" "-f" container)
  (let [cfg (io/file (System/getProperty "java.io.tmpdir") "dh-garage-test.toml")]
    (spit cfg garage-toml)
    (when-not (zero? (:exit (sh "docker" "create" "--name" container
                                "-p" (str s3-port ":3900") image)))
      (throw (ex-info "could not create garage container" {})))
    (sh "docker" "cp" (str cfg) (str container ":/etc/garage.toml"))
    (sh "docker" "start" container)
    (when-not (wait-until 60 #(zero? (:exit (garage! "status"))))
      (throw (ex-info "garage did not become healthy" {})))))

(defn- provision!
  "Assign the single-node layout, create bucket + key, grant access.
   Returns {:access-key .. :secret ..}."
  []
  (let [node-id (some->> (:out (garage! "status"))
                         (re-find #"(?m)^([0-9a-f]{8,})\s")
                         second)]
    (when-not node-id (throw (ex-info "could not parse garage node id" {})))
    (garage! "layout" "assign" "-z" "dc1" "-c" "1G" node-id)
    (garage! "layout" "apply" "--version" "1")
    (when-not (wait-until 20 #(zero? (:exit (garage! "bucket" "list"))))
      (throw (ex-info "garage layout did not settle" {})))
    (garage! "bucket" "create" bucket)
    (let [out (:out (garage! "key" "create" "dh-test-key"))
          kid (second (re-find #"Key ID:\s*(\S+)" out))
          sec (second (re-find #"Secret key:\s*(\S+)" out))]
      (when-not (and kid sec)
        (throw (ex-info "could not parse garage key" {:out out})))
      (garage! "bucket" "allow" "--read" "--write" bucket "--key" "dh-test-key")
      {:access-key kid :secret sec})))

(defn- stop-garage! []
  (sh "docker" "rm" "-f" container))

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? true :schema-flexibility :write}]
    (d/create-database cfg)
    (d/connect cfg)))

(deftest migrate-via-garage-test
  (if-not (docker?)
    (do (println "[migrate-s3-test] docker unavailable — skipping Garage integration test")
        (is true "skipped: no docker"))
    (try
      (start-garage!)
      (let [{:keys [access-key secret]} (provision!)
            s3-spec {:backend :s3
                     :bucket bucket
                     :region "us-east-1"
                     :access-key access-key
                     :secret secret
                     :endpoint-override {:protocol :http :hostname "localhost" :port s3-port}
                     :path-style-access? true}]
        (testing "export → estimate → import → verify through a real S3 API"
          (let [store  (ks/create-store (assoc s3-spec :id (java.util.UUID/randomUUID))
                                        {:sync? true})
                target {:store store :prefix "backup-it"}
                src    (fresh-db)]
            (d/transact src [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                             {:db/ident :score :db/valueType :db.type/double
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :pal :db/valueType :db.type/ref
                              :db/cardinality :db.cardinality/many}])
            (d/transact src [{:db/id "a" :name "Alice" :score 0.0}
                             {:db/id "b" :name "Bob" :score 3.14 :pal "a"}])
            (d/transact src [[:db/retractEntity [:name "Bob"]]])
            (let [man (m/export-db src target {:history? true :chunk-size 5})
                  est (m/estimate-import-memory target)
                  tgt (fresh-db)
                  rep (m/import-db tgt target {})
                  ver (m/verify tgt target)]
              (is (> (count (:chunks man)) 1) "wrote multiple chunk objects to garage")
              (is (string? (:recommended-heap est)) "estimate reads the manifest from S3")
              (is (:verified? rep) "import from S3 verifies")
              (is (:ok? ver) "full tiered verify against the S3 dump passes")
              (is (get-in ver [:tier2 :match?]))
              (is (= Double (class (d/q '[:find ?v . :where [?e :name "Alice"] [?e :score ?v]] @tgt)))
                  "#633 type-exactness holds over the S3 wire")
              (d/release src)
              (d/release tgt)))))
      (finally
        (stop-garage!)))))
