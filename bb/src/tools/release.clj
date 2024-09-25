(ns tools.release
  (:require
   [babashka.fs :as fs]
   [babashka.process :as p]
   [borkdude.gh-release-artifact :as gh]
   [selmer.parser :refer [render]]
   [tools.build :as build]
   [tools.version :as version])
  (:import
   (clojure.lang ExceptionInfo)))

(defn fib [a b]
  (lazy-seq (cons a (fib b (+ a b)))))

(defn retry-with-fib-backoff [retries exec-fn fail-test-fn]
  (loop [idle-times (take retries (fib 1 2))]
    (let [result (exec-fn)]
      (if (fail-test-fn result)
        (do (println "Returned: " result)
            (if-let [sleep-ms (first idle-times)]
              (do (println "Retrying with remaining back-off times (in s): " idle-times)
                  (Thread/sleep (* 1000 sleep-ms))
                  (recur (rest idle-times)))
              result))
        result))))

(defn try-release [file repo-config]
  (try (gh/release-artifact
        {:org (:org repo-config)
         :repo (:lib repo-config)
         :tag (version/string repo-config)
         :commit (version/current-commit)
         :file file
         :content-type "application/java-archive"
         :draft false})
       (catch ExceptionInfo e
         (assoc (ex-data e) :failure? true))))

(defn gh-release
  "Create a GitHub release and upload the library jar"
  [repo-config file]
  (println "Trying to release artifact" file "...")
  (let [_ (when-not (fs/exists? file)
            (println "Release file at" file "doesn't exist!")
            (System/exit 1))
        ret (retry-with-fib-backoff 10 #(try-release file repo-config) :failure?)]
    (if (:failure? ret)
      (do (println "GitHub release failed!")
          (System/exit 1))
      (println (:url ret)))))

(defn zip-path [lib version target-dir zip-pattern]
  (let [platform (System/getenv "DTHK_PLATFORM")
        arch (System/getenv "DTHK_ARCH")]
    (if-not (and platform arch)
      (do (println "ERROR: Environment variables DTHK_PLATFORM and DTHK_ARCH need to be set")
          (System/exit 1))
      (str target-dir "/" (render zip-pattern {:platform platform
                                               :arch arch
                                               :lib lib
                                               :version version})))))

(defn zip-cli [config project]
  (let [{:keys [target-dir binary-name zip-pattern]} (-> config :release project)
        lib (:lib config)
        version (version/string config)
        binary-path (str target-dir "/" binary-name)]
    (if-not (fs/exists? binary-path)
      (do (println (str "ERROR: " binary-path " executable not found, please compile first."))
          (System/exit 1))
      (let [zip-name (zip-path lib version target-dir zip-pattern)]
        (p/shell "zip" zip-name binary-name)
        zip-name))))

(defn -main [config args]
  (let [cmd (first args)]
    (case cmd
      "jar" (gh-release config (build/jar-path config (-> config :build :clj)))
      "native-image" (->> (zip-cli config :native-cli)
                          (gh-release config))
      (do (println "ERROR: Command not found: " cmd)
          (System/exit 1)))))
