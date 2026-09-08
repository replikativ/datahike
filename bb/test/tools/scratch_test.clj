(ns tools.scratch-test
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [clojure.test :refer [deftest is run-tests]]
            [tools.scratch :as scratch]))

(defn with-root [f]
  (fs/create-dirs ".scratch")
  (let [root (fs/create-temp-dir {:dir (fs/absolutize ".scratch") :prefix "check-"})]
    (try (with-redefs [scratch/root (constantly root)] (f root))
         (finally (fs/delete-tree root)))))

(deftest subprocess-environment-and-failure-cleanup
  (with-root
    (fn [root]
      (let [cleanup (scratch/start! nil)
            tmp (System/getProperty "java.io.tmpdir")]
        (try
          (let [result (p/shell {:out :string :err :string
                                 :extra-env {"LOG_LEVEL" "warn"}}
                                "bb" "--config" "/dev/null" "-e"
                                "(assert (= (System/getenv \"TMPDIR\") (System/getenv \"TEMP\"))) (assert (= \"warn\" (System/getenv \"LOG_LEVEL\"))) (println (System/getenv \"JAVA_TOOL_OPTIONS\"))")]
            (is (.contains (:out result) tmp)))
          (spit (fs/file tmp "leftover-db") "bytes")
          (throw (ex-info "test failed" {}))
          (catch Exception e (is (= "test failed" (ex-message e))))
          (finally (cleanup)))
        (is (empty? (fs/list-dir root)))))))

(deftest janitor-respects-live-locks-and-incomplete-records
  (with-root
    (fn [root]
      (let [cleanup (scratch/start! nil)
            abandoned (fs/path root "run-finished")
            incomplete (fs/path root "run-incomplete")]
        (try
          (doseq [[dir state] [[abandoned :finished] [incomplete :running]]]
            (fs/create-dirs dir)
            (spit (fs/file dir "owner.edn") (pr-str {:state state})))
          (scratch/inspect! true)
          (is (not (fs/exists? abandoned)))
          (is (fs/exists? incomplete))
          (is (fs/exists? (System/getProperty "java.io.tmpdir")))
          (finally (cleanup)))))))

(deftest task-init-covers-dependencies-and-java
  (with-root
    (fn [root]
      (let [config (fs/file root "tasks.edn")]
        (spit config
              (pr-str {:paths [(str (fs/relativize root (fs/absolutize "bb/src")))]
                       :tasks {:requires '([tools.scratch :as scratch])
                               :init '(scratch/ensure-task-run!)
                               'dependency {:task '(let [proc (.start (.inheritIO (ProcessBuilder. ["java" "-XshowSettings:properties" "-version"])))]
                                                     (assert (zero? (.waitFor proc))))}
                               'probe {:depends ['dependency] :task '(println "done")}
                               'fail {:task '(throw (ex-info "intentional task failure" {}))}}}))
        (let [result (p/shell {:out :string :err :string
                               :extra-env {"DATAHIKE_SCRATCH_ROOT" (str root)
                                           "DATAHIKE_SCRATCH_RUN" ""}}
                              "bb" "--config" (str config) "probe")]
          (is (.contains (:err result) (str root)))
          (is (.contains (:err result) "java.io.tmpdir ="))
          (is (= [config] (mapv fs/file (fs/list-dir root)))))
        (let [result (p/shell {:out :string :err :string :continue true
                               :extra-env {"DATAHIKE_SCRATCH_ROOT" (str root)
                                           "DATAHIKE_SCRATCH_RUN" ""}}
                              "bb" "--config" (str config) "fail")]
          (is (not (zero? (:exit result))))
          (is (= [config] (mapv fs/file (fs/list-dir root)))))))))

(deftest cleanup-stops-a-leftover-child-before-removing-files
  (with-root
    (fn [root]
      (let [cleanup (scratch/start! nil)
            child (p/process ["bb" "--config" "/dev/null" "-e" "(Thread/sleep 60000)"]
                             {:out :string :err :string})]
        (try
          (cleanup)
          (is (not (p/alive? child)))
          (is (empty? (fs/list-dir root)))
          (finally (p/destroy-tree child) (cleanup)))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'tools.scratch-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
