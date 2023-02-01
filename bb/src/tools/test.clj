(ns tools.test
  (:refer-clojure :exclude [test])
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [tools.build :as build]))

(defn clj [opts & args] (apply p/shell opts "clojure" args))
(defn git [opts & args] (apply p/shell opts "git" args))

(defn kaocha [& args]
  (apply clj {:extra-env {"TIMBRE_LEVEL" ":warn"}}
         "-M:test" "-m" "kaocha.runner" args))

(defn back-compat [config]
  (let [old-version-dir "datahike-old"
        ssh-dir (fs/expand-home "~/.ssh")
        known-hosts-file (fs/expand-home "~/.ssh/known_hosts")]
    (println "WRITING TEST DATA TO TEST-DB")
    (when-not (fs/exists? ssh-dir)
      (fs/create-dirs ssh-dir))
    (fs/delete-on-exit old-version-dir)
    (let [output (:out (p/shell {:out :string}
                                "ssh-keyscan" "github.com"))]
      (when-not (fs/exists? known-hosts-file)
        (fs/create-file known-hosts-file))
      (fs/write-lines known-hosts-file [output] {:append true}))
    
    (git {:dir "."}
         "clone" "--depth" "1" (:git-url config) old-version-dir)
    (build/compile {:build/class-dir (str old-version-dir "/" (:build/class-dir config))
                    :build/java-src-dirs [(str old-version-dir "/java")]
                    :build/deps-file (str old-version-dir "/deps.edn")})
    
    (clj {:dir old-version-dir}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X" "backward-test/write")
    (fs/delete-tree old-version-dir)
    
    (println "READING TEST DATA FROM TEST-DB")
    (clj {:dir "."}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X" "backward-test/read")))

(defn native-image []
  (if (fs/exists? "./dhi")
    (p/shell "./bb/resources/native-image-tests/run-native-image-tests")
    (println "Native image cli missing. Please run 'bb ni-build' and try again.")))

(defn all [config]
  (kaocha)
  (back-compat config)
  (native-image))

(defn -main [config & args]
  (if (seq args)
    (case (first args)
      "native-image" (native-image)
      "back-compat" (back-compat config)
      (apply kaocha "--focus" args))
    (all config)))
