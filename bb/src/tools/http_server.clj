(ns tools.http-server
  (:require
   [babashka.fs :as fs]
   [babashka.http-client :as http]
   [babashka.process :as p]
   [cheshire.core :as json]
   [clojure.string :as str]
   [tools.build :as build])
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
        (< attempt 119) (do (Thread/sleep 250) (recur (inc attempt)))
        :else (throw (ex-info "Standalone server did not become live within 30 seconds" {}))))))

(defn- assert-slim! [jar max-bytes]
  (let [bytes (fs/size jar)]
    (expect! (str "jar exceeds " max-bytes " bytes") #(<= % max-bytes) bytes)
    (with-open [zip (ZipFile. (str jar))]
      (let [names (mapv #(.getName %) (enumeration-seq (.entries zip)))
            compiler-content
            (filterv #(or (str/starts-with? % "com/google/javascript/jscomp/")
                          (str/starts-with? % "cljs/analyzer")
                          (str/starts-with? % "cljs/compiler")
                          (str/starts-with? % "cljs/closure")
                          (str/starts-with? % "cljs/repl")
                          (= % "goog/base.js"))
                     names)]
        (expect! "ClojureScript compiler content is absent"
                 empty?
                 compiler-content)))))

(defn- assert-thin! [jar]
  (with-open [zip (ZipFile. (str jar))]
    (let [names (into #{} (map #(.getName %)) (enumeration-seq (.entries zip)))]
      (doseq [path ["datahike/http/main.clj"
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
                   (mapv :name (:databases status))))
        (let [version-body (:body (response :get (str base-url "/version")
                                             {:headers (assoc auth "accept" "application/json")}))]
          (doseq [backend ["memory" "file" "s3" "jdbc" "dynamodb" "redis"]]
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
