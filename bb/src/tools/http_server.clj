(ns tools.http-server
  (:require
   [babashka.fs :as fs]
   [babashka.http-client :as http]
   [babashka.process :as p]
   [cheshire.core :as json]
   [clojure.string :as str]
   [tools.build :as build]
   [tools.version :as version])
  (:import
   [java.net ServerSocket]
   [java.util UUID]
   [java.util.zip ZipFile]))

(defn- free-port []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn- response [method url options]
  (http/request (merge {:method method
                        :uri url
                        :throw false}
                       options)))

(defn- expect! [description predicate value]
  (when-not (predicate value)
    (throw (ex-info (str "Standalone server smoke check failed: " description)
                    {:description description :actual value})))
  value)

(defn- wait-until-live! [process base-url]
  (loop [attempt 0]
    (when-not (p/alive? process)
      (throw (ex-info "Standalone server exited during startup" {})))
    (let [result (try (response :get (str base-url "/health/live") {})
                      (catch Exception _ nil))]
      (cond
        (= 200 (:status result)) result
        (< attempt 479) (do (Thread/sleep 250) (recur (inc attempt)))
        :else (throw (ex-info "Standalone server did not become live within 120 seconds" {}))))))

(defn- assert-slim! [jar max-bytes]
  (let [bytes (fs/size jar)]
    (expect! (str "jar exceeds " max-bytes " bytes") #(<= % max-bytes) bytes)
    ;; The remote-invocation runtime is kabel.remote; the server no longer
    ;; depends on distributed-scope, which used to pull the ClojureScript
    ;; analyzer in for macro free-variable analysis.
    (with-open [zip (ZipFile. (str jar))]
      (let [names (mapv #(.getName %) (enumeration-seq (.entries zip)))
            name-set (set names)]
        (expect! "portable Java launcher is present"
                 #(contains? % "datahike/http/Launcher.class")
                 name-set)
        (expect! "build-JVM-dependent superv.async AOT classes are absent"
                 #(not (contains? % "superv/async__init.class"))
                 name-set)))))

(defn- assert-thin! [jar]
  (with-open [zip (ZipFile. (str jar))]
    (let [names (into #{} (map #(.getName %)) (enumeration-seq (.entries zip)))]
      (doseq [path ["datahike/http/main.clj"
                    "datahike/http/kabel.clj"
                    "datahike/kabel/handlers.cljc"
                    "datahike/http/Launcher.class"
                    "eacl/core.cljc"
                    "eacl/datahike/core.clj"
                    "META-INF/maven/org.replikativ/datahike-http-server/pom.xml"]]
        (expect! (str "thin jar contains " path) #(contains? % path) names))
      (doseq [path ["datahike/java/DatahikeGeneratedTest.class"
                    "datahike/java/DatahikeTest.class"]]
        (expect! (str "thin jar excludes " path) #(not (contains? % path)) names)))))

(defn smoke!
  "Run the built standalone jar and verify its public shell, authenticated
   operational APIs, catalog, metrics, and bundled backend inventory."
  [config]
  (let [thin-project (get-in config [:build :http-server-clj])
        thin-jar     (build/jar-path config thin-project)
        project      (get-in config [:build :http-server-standalone])
        jar          (build/jar-path config project)
        port     (free-port)
        kabel-port (loop [candidate (free-port)]
                     (if (= candidate port) (recur (free-port)) candidate))
        token    "standalone-smoke-token"
        base-url (str "http://127.0.0.1:" port)
        temp-dir (fs/create-temp-dir {:prefix "datahike-http-server-smoke-"})
        config-file (str (fs/file temp-dir "config.edn"))
        database-id (UUID/randomUUID)]
    (expect! "thin jar exists" fs/exists? thin-jar)
    (assert-thin! thin-jar)
    (expect! "standalone jar exists" fs/exists? jar)
    (assert-slim! jar (:max-bytes project))
    (spit config-file
          (pr-str {:host "127.0.0.1"
                   :port port
                   :token token
                   :metrics true
                   :kabel {:host "127.0.0.1"
                           :port kabel-port
                           :jwt {:alg :HS256 :secret "standalone-kabel-smoke-secret"}
                           :store {:backend :memory}}
                   :nrepl {:port 0}
                   :system-db {:store {:backend :memory}}}))
    (let [process (p/process ["java" "-jar" jar config-file]
                             {:out :inherit :err :inherit})
          auth    {"authorization" (str "token " token)}]
      (try
        (wait-until-live! process base-url)
        (let [landing (response :get base-url {})]
          (expect! "landing page returns 200" #(= 200 %) (:status landing))
          (expect! "landing page identifies Datahike" #(str/includes? % "Datahike server") (:body landing)))
        (expect! "admin status is authenticated"
                 #(= 401 %)
                 (:status (response :get (str base-url "/admin/status") {})))
        (let [created (response :post (str base-url "/create-database")
                                {:headers (assoc auth
                                                 "content-type" "application/edn"
                                                 "accept" "application/edn")
                                 :body (pr-str [{:name "smoke"
                                                :store {:backend :memory :id database-id}
                                                :schema-flexibility :write}])})]
          (expect! "create-database succeeds" #(= 200 %) (:status created)))
        (let [status (-> (response :get (str base-url "/admin/status")
                                   {:headers (assoc auth "accept" "application/json")})
                         :body
                         (json/parse-string true))]
          (expect! "catalog contains the smoke database"
                   #(= ["smoke"] %)
                   (mapv :name (:databases status)))
          (expect! "packaged nREPL starts on loopback"
                   #(and (true? (:enabled %))
                         (= ["!kw" "tcp"] (:transport %))
                         (pos-int? (:port %)))
                   (get-in status [:node :nrepl])))
        (let [version-body (:body (response :get (str base-url "/version")
                                             {:headers (assoc auth "accept" "application/json")}))]
          (doseq [backend ["memory" "file" "s3" "jdbc" "dynamodb" "redis"
                           "gcs" "azure-blob"]]
            (expect! (str "backend inventory contains " backend)
                     #(str/includes? % backend)
                     version-body)))
        (let [metrics (:body (response :get (str base-url "/prometheus")
                                       {:headers auth}))]
          (expect! "Prometheus output contains JVM metrics"
                   #(str/includes? % "jvm_memory_used_bytes")
                   metrics))
        (println "Standalone server smoke test passed:" jar (fs/size jar) "bytes")
        (finally
          (p/destroy-tree process)
          (fs/delete-tree temp-dir))))))

(def ^:private local-image "datahike-server:dev")
(def ^:private container-context "docker/server")

(defn- container-engine []
  (let [requested (System/getenv "DATAHIKE_CONTAINER_ENGINE")
        candidates (if requested [requested] ["docker" "podman"])]
    (or (some #(when (fs/which %) %) candidates)
        (throw (ex-info
                "No container engine found; install Docker or Podman, or set DATAHIKE_CONTAINER_ENGINE"
                {:type :datahike.tools/no-container-engine
                 :candidates candidates})))))

(defn image!
  "Build the local server image from the already-built standalone JAR."
  [config]
  (let [project (get-in config [:build :http-server-standalone])
        jar (build/jar-path config project)
        staged (str (fs/file container-context "datahike-http-server.jar"))
        engine (container-engine)]
    (expect! "standalone jar exists before container build" fs/exists? jar)
    (fs/copy jar staged {:replace-existing true})
    (try
      (apply p/shell
             (concat [engine "build"]
                     ;; Podman's default OCI manifest omits Docker health
                     ;; checks. Its Docker format preserves the same image
                     ;; contract Buildx publishes in CI.
                     (when (str/ends-with? engine "podman") ["--format" "docker"])
                     ["--tag" local-image
                      "--build-arg" (str "VERSION=" (version/string config))
                      "--build-arg" (str "REVISION=" (version/current-commit))
                      container-context]))
      (println "Built local server image" local-image "with" engine)
      (finally
        (fs/delete-if-exists staged)))))

(defn- wait-until-container-live! [base-url]
  (loop [attempt 0]
    (let [result (try (response :get (str base-url "/health/live") {})
                      (catch Exception _ nil))]
      (cond
        (= 200 (:status result)) result
        (< attempt 479) (do (Thread/sleep 250) (recur (inc attempt)))
        :else (throw (ex-info "Container did not become live within 120 seconds" {}))))))

(defn container-smoke!
  "Run the local image as its non-root user and exercise its HTTP lifecycle."
  [_config]
  (let [engine (container-engine)
        port (free-port)
        name (str "datahike-server-smoke-" (UUID/randomUUID))
        base-url (str "http://127.0.0.1:" port)
        token "container-smoke-token"
        image-user (-> (p/shell {:out :string}
                                engine "image" "inspect" "--format" "{{.Config.User}}" local-image)
                       :out str/trim)]
    (expect! "container image runs as the stable non-root user"
             #(= "10001:10001" %) image-user)
    (p/shell engine "run" "--detach" "--name" name
             "--publish" (str "127.0.0.1:" port ":4444")
             "--env" (str "DATAHIKE_TOKEN=" token)
             local-image)
    (try
      (wait-until-container-live! base-url)
      (expect! "container landing page returns 200"
               #(= 200 %)
               (:status (response :get base-url {})))
      (expect! "container operational API accepts its configured token"
               #(= 200 %)
               (:status (response :get (str base-url "/version")
                                  {:headers {"authorization" (str "token " token)}})))
      ;; Longer than the server's default 30-second graceful drain. A failed
      ;; stop is a smoke failure; the finally block still force-cleans it.
      (p/shell engine "stop" "--time" "40" name)
      (println "Local server container smoke test passed:" local-image)
      (catch Throwable t
        (try (p/shell engine "logs" name)
             (catch Exception _))
        (throw t))
      (finally
        (try (p/shell engine "rm" "--force" "--volumes" name)
             (catch Exception _))))))
