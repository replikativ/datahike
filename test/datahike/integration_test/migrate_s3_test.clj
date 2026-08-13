(ns datahike.integration-test.migrate-s3-test
  "Round-trips a dump through a REAL S3-compatible store — Garage in docker — so
   the store target is exercised over the actual S3 wire protocol (endpoint
   override + path-style addressing + SigV4), not the in-memory konserve
   stand-in.

   Garage (garagehq.deuxfleurs.fr) is chosen over MinIO because it is actively
   maintained open source; nothing below is Garage-specific beyond the
   provisioning CLI — any S3-compatible target works. The config file is
   injected with `docker cp` (no volume mounts), so the test runs on any docker
   host.

   ## Why not just export to a filestore instead

   Because a filestore cannot fail the way this test exists to catch. The store
   target over an in-memory konserve, over a filestore, and over the filesystem
   medium are all already covered elsewhere; what is only covered HERE is the S3
   wire itself — SigV4 signing, path-style addressing, an endpoint override, and
   konserve-s3's `bget` handle shape (see `migrate.store`'s note on
   replikativ/konserve#162, which is exactly a real-backend difference an
   in-memory stand-in hid). Substituting a local store would keep the test green
   and delete its subject.

   What was wrong was the FAILURE MODE, not the backend: `docker?` returning
   true only means a daemon answered, and everything after it — image pull,
   container create, `docker cp` from a tmpdir the daemon cannot see, the health
   poll — could still fail, and did, as an ERROR indistinguishable from datahike
   being broken. Those are now skips, like an absent docker.

   Set `DATAHIKE_REQUIRE_S3=1` to turn every skip back into a failure. That is
   the point of the flag: a test that skips itself when the environment is
   imperfect will eventually skip forever without anyone noticing, so CI (where
   the environment IS the deliverable) should run with it set."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [clojure.string :as str]
            [konserve-s3.core]  ;; registers the :s3 konserve backend
            [konserve.store :as ks]))

;; Both are per-RUN, not fixed. A fixed container name and port made two
;; concurrent runs — or one run against a container a crashed run left behind —
;; collide, and the suite is otherwise safe to run in parallel.
(def ^:private s3-port (+ 3910 (rand-int 1000)))
(def ^:private container (str "dh-migrate-garage-test-" (random-uuid)))
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
    ;; CHECKED. An unchecked `cp` is why a failure here used to surface thirty
    ;; seconds later as "garage did not become healthy" — garage boots without a
    ;; config, exits 1, and the health poll simply times out. The copy can fail
    ;; for reasons that have nothing to do with garage: notably a sandboxed
    ;; /tmp that the docker daemon cannot see.
    (when-not (zero? (:exit (sh "docker" "cp" (str cfg) (str container ":/etc/garage.toml"))))
      (throw (ex-info "could not copy garage config into the container"
                      {:config (str cfg)
                       :hint "is java.io.tmpdir visible to the docker daemon?"})))
    (sh "docker" "start" container)
    (when-not (wait-until 60 #(zero? (:exit (garage! "status"))))
      (throw (ex-info "garage did not become healthy"
                      {:logs (:out (sh "docker" "logs" container))})))))

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

(defn- required? []
  (contains? #{"1" "true" "yes"} (some-> (System/getenv "DATAHIKE_REQUIRE_S3")
                                         str/lower-case)))

(defn- skip!
  "Report an environment failure the way an absent docker is reported — unless
   the operator asked for the opposite."
  [why ex]
  (if (required?)
    (throw (or ex (ex-info why {})))
    (do (println (str "[migrate-s3-test] " why
                      (when ex (str " — " (ex-message ex)))
                      " — skipping. Set DATAHIKE_REQUIRE_S3=1 to make this a failure."))
        (is true (str "skipped: " why))
        nil)))

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? true :schema-flexibility :write}]
    (d/create-database cfg)
    (d/connect cfg)))

(deftest migrate-via-garage-test
  (if-not (docker?)
    (skip! "docker unavailable" nil)
    (try
      ;; Setup only. The assertions below stay OUTSIDE this catch on purpose —
      ;; once garage is up and the export begins, a failure is datahike's and
      ;; must be reported as one. Swallowing that would be worse than the error
      ;; this replaces.
      (when-let [{:keys [access-key secret]}
                 (try (start-garage!) (provision!)
                      (catch Exception e (skip! "could not bring up garage" e)))]
        (let [s3-spec {:backend :s3
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
              (let [man (m/export-db @src target {:history? true :chunk-size 5})
                    est (m/estimate-import-memory target)
                    tgt (fresh-db)
                    rep (m/import-db tgt target {})
                    ver (m/verify-against @tgt target)]
                (is (> (count (:chunks man)) 1) "wrote multiple chunk objects to garage")
                (is (string? (:recommended-heap est)) "estimate reads the manifest from S3")
                (is (:verified? rep) "import from S3 verifies")
                (is (:ok? ver) "full tiered verify against the S3 dump passes")
                (is (get-in ver [:tier2 :match?]))
                (is (= Double (class (d/q '[:find ?v . :where [?e :name "Alice"] [?e :score ?v]] @tgt)))
                    "#633 type-exactness holds over the S3 wire")
                (d/release src)
                (d/release tgt))))))
      (finally
        (stop-garage!)))))
