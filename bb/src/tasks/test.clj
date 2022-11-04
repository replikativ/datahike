(ns tasks.test
  (:refer-clojure :exclude [test])
  (:require [babashka.fs :as fs]
            [tasks.build :as build]
            [tasks.settings :refer [load-settings]]
            [utils.shell :refer [sh git clj with-opts *cwd*]]))

(defn kaocha [& args]
  (with-opts {:extra-env {"TIMBRE_LEVEL" ":warn"}}
             (partial apply clj "-M:test" "-m" "kaocha.runner" args)))

(def settings (load-settings))

(defn back-compat []
  (let [old-version-dir "datahike-old"
        ssh-dir (fs/expand-home "~/.ssh")
        known-hosts-file (fs/expand-home "~/.ssh/known_hosts")]
    (println "WRITING TEST DATA TO TEST-DB")
    (when-not (fs/exists? ssh-dir)
      (fs/create-dirs ssh-dir))
    (fs/delete-on-exit old-version-dir)
    
    (let [output (:out (with-opts {:out :string} 
                         (sh "ssh-keyscan" "github.com")))]
      (when-not (fs/exists? known-hosts-file)
        (fs/create-file known-hosts-file))
      (println known-hosts-file)
      (println output)
      (fs/write-lines known-hosts-file [output] {:append true}))
    
(git "clone" "--depth" "1" (:git-url settings) old-version-dir)
(build/compile {:build/class-dir (str old-version-dir "/" (:build/class-dir settings))
                :build/java-src-dirs [(str old-version-dir "/java")]
                :build/deps-file (str old-version-dir "/deps.edn")})
    
    (binding [*cwd* old-version-dir]
      (clj "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                         " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
           "-X" "backward-test/write"))
    (fs/delete-tree old-version-dir)
    
    (println "READING TEST DATA FROM TEST-DB")
    (clj "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X" "backward-test/read")))

(defn all []
  (kaocha "--focus" "unit")
  (kaocha "--focus" "integration")
  (back-compat))

(defn -main [& args]
  (if (seq args)
    (case (first args)
      "back-compat" (back-compat)
      (kaocha "--focus" (first args)))
    (all)))
