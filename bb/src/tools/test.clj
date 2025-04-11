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
  (println "Testing backwards compatibility")
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
    (fs/delete-tree old-version-dir)
    (git {:dir "."}
         "clone" "--depth" "1" (:git-url config) old-version-dir)
    (build/compile-java {:class-dir (str old-version-dir "/target/classes")
                         :java-src-dirs [(str old-version-dir "/java")]
                         :deps-file (str old-version-dir "/deps.edn")})

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
  (if (fs/exists? "./dthk")
    (p/shell "./bb/resources/native-image-tests/run-native-image-tests")
    (println "Native image cli missing. Please run 'bb ni-cli' and try again.")))

(defn bb-pod []
  (if (fs/exists? "./dthk")
    (p/shell "./bb/resources/native-image-tests/run-bb-pod-tests.clj")
    (do (println "Native image cli missing. Please run 'bb ni-cli' and try again.")
        (System/exit 1))))

(defn specs []
  (kaocha "--focus" "specs" "--plugin" "kaocha.plugin/orchestra"))

(defn cljs-compile-test []
  (p/shell "clj -M:cljs -m shadow.cljs.devtools.cli compile :comptest")
  (p/shell "node target/out/comptest.js"))

(defn all [config]
  (kaocha "--skip" "specs")
  (specs)
  (back-compat config)
  (native-image)
  (bb-pod)
  (cljs-compile-test))

(defn -main [config & args]
  (if (seq args)
    (case (first args)
      "native-image" (native-image)
      "bb-pod" (bb-pod)
      "back-compat" (back-compat config)
      "specs" (specs)
      "cljs" (cljs-compile-test)
      (apply kaocha "--focus" args))
    (all config)))
