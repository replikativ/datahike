(ns tools.scratch-test
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [clojure.test :refer [deftest is run-tests]]
            [tools.scratch :as scratch]))

(def scratch-config (str (fs/absolutize "bb/scratch.edn")))

(deftest task-command-never-drops-options
  (let [args ["-Xmx512m" "--config" "path with spaces/tasks.edn"
              "run" "--parallel" "probe" "--" "a b" ""]]
    (is (= (into ["C:\\Program Files\\bb.exe"] args)
           (scratch/task-command "C:\\Program Files\\bb.exe" args))))
  (doseq [[command args] [[nil ["probe"]] ["bb" nil] [nil nil]]]
    (is (= :tools.scratch/unavailable-task-command
           (try (scratch/task-command command args)
                (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))))

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
                                "bb" "--config" scratch-config "-e"
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

(deftest automatic-task-entry-when-process-information-is-available
  (let [info (.info (java.lang.ProcessHandle/current))]
    (when (and (.isPresent (.command info)) (.isPresent (.arguments info)))
      (with-root
        (fn [root]
          (let [config (fs/file root "automatic.edn")]
            (spit config
                  (pr-str {:paths [(str (fs/relativize root (fs/absolutize "bb/src")))]
                           :tasks {:requires '([tools.scratch :as scratch])
                                   :init '(scratch/ensure-task-run!)
                                   'probe '(assert (seq (System/getenv "DATAHIKE_SCRATCH_RUN")))}}))
            (is (zero? (:exit (p/shell {:out :string :err :string
                                        :extra-env {"DATAHIKE_SCRATCH_ROOT" (str root)
                                                    "DATAHIKE_SCRATCH_RUN" ""}}
                                       "bb" "--config" (str config) "probe"))))
            (is (= [config] (mapv fs/file (fs/list-dir root))))))))))

(deftest task-init-covers-dependencies-and-java
  (with-root
    (fn [root]
      (let [config (fs/file root "tasks.edn")]
        (spit config
              (pr-str {:paths [(str (fs/relativize root (fs/absolutize "bb/src")))]
                       :tasks {:requires '([tools.scratch :as scratch])
                               :init '(with-redefs [scratch/task-command
                                                    (fn [& _] (throw (ex-info "OS inspection unavailable" {})))]
                                        (scratch/ensure-task-run!))
                               'dependency {:task '(let [proc (.start (.inheritIO (ProcessBuilder. ["java" "-XshowSettings:properties" "-version"])))]
                                                     (assert (zero? (.waitFor proc))))}
                               'probe {:depends ['dependency]
                                       :task '(do (assert (= ["a b" "quote\"value" "--option"] (vec *command-line-args*)))
                                                  (println "done"))}
                               'fail {:task '(throw (ex-info "intentional task failure" {}))}}}))
        (let [result (p/shell {:out :string :err :string
                               :extra-env {"DATAHIKE_SCRATCH_ROOT" (str root)
                                           "DATAHIKE_SCRATCH_RUN" ""}}
                              "bb" "--config" scratch-config "-m" "tools.scratch"
                              "run" "--" "bb" "--config" (str config)
                              "probe" "a b" "quote\"value" "--option")]
          (is (.contains (:err result) (str root)))
          (is (.contains (:err result) "java.io.tmpdir ="))
          (is (= [config] (mapv fs/file (fs/list-dir root)))))
        (let [result (p/shell {:out :string :err :string :continue true
                               :extra-env {"DATAHIKE_SCRATCH_ROOT" (str root)
                                           "DATAHIKE_SCRATCH_RUN" ""}}
                              "bb" "--config" scratch-config "-m" "tools.scratch"
                              "run" "--" "bb" "--config" (str config) "fail")]
          (is (not (zero? (:exit result))))
          (is (= [config] (mapv fs/file (fs/list-dir root)))))))))

(deftest cleanup-stops-a-leftover-child-before-removing-files
  (with-root
    (fn [root]
      (let [cleanup (scratch/start! nil)
            child (p/process ["bb" "--config" scratch-config "-e" "(Thread/sleep 60000)"]
                             {:out :string :err :string})]
        (try
          (cleanup)
          ;; Windows can report the ProcessHandle terminated before the owned
          ;; Process reports completion. Check both, using a bounded wait rather
          ;; than assuming their liveness observations change simultaneously.
          (is (not (.isAlive (.toHandle (:proc child)))))
          (is (.waitFor (:proc child) 5 java.util.concurrent.TimeUnit/SECONDS)
              "cleanup must terminate the child, not leave its 60s sleep running")
          (is (not (p/alive? child)))
          (is (empty? (fs/list-dir root)))
          (finally (p/destroy-tree child) (cleanup)))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'tools.scratch-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
