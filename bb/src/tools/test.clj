(ns tools.test
  (:refer-clojure :exclude [test])
  (:require [babashka.fs :as fs]
            [babashka.http-client]
            [babashka.process :as p]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [tools.build :as build]))

(defn clj [opts & args] (apply p/shell opts "clojure" args))
(defn git [opts & args] (apply p/shell opts "git" args))

(defn kaocha [& args]
  (apply clj {:extra-env {"LOG_LEVEL" ":warn"}}
         "-M:test" "-m" "kaocha.runner" args))

(defn back-compat [config]
  (println "Testing backwards compatibility")
  (let [old-version-dir "datahike-old"
        release-tag (str/trim (:out (git {:out :string}
                                         "describe" "--tags" "--abbrev=0" "HEAD")))
        secondary-root (str (fs/create-temp-dir {:prefix "datahike-secondary-back-compat-"}))
        secondary-fixture (str (fs/absolutize
                                "test/datahike/backward_compatibility_test/src"))
        old-secondary-fixture "backward-secondary-src"
        secondary-env {"BACK_COMPAT_ROOT" secondary-root
                       "LOG_LEVEL" ":fatal"
                       "JAVA_OPTS" (str "--enable-native-access=ALL-UNNAMED "
                                        "-XX:+UnlockExperimentalVMOptions "
                                        "-XX:-UseJVMCICompiler "
                                        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn")}
        ssh-dir (fs/expand-home "~/.ssh")
        known-hosts-file (fs/expand-home "~/.ssh/known_hosts")]
    (println "Using released fixture writer" release-tag)
    (println "WRITING TEST DATA TO TEST-DB")
    (when-not (fs/exists? ssh-dir)
      (fs/create-dirs ssh-dir))
    (fs/delete-on-exit old-version-dir)
    (fs/delete-on-exit secondary-root)
    (let [output (:out (p/shell {:out :string}
                                "ssh-keyscan" "github.com"))]
      (when-not (fs/exists? known-hosts-file)
        (fs/create-file known-hosts-file))
      (fs/write-lines known-hosts-file [output] {:append true}))
    (fs/delete-tree old-version-dir)
    (git {:dir "."}
         "clone" "--depth" "1" "--branch" release-tag
         (:git-url config) old-version-dir)

    ;; Generate Java API bindings before compiling
    (println "Generating Java API for old version...")
    (let [old-dir old-version-dir
          cp (:out (p/shell {:out :string :dir old-dir}
                            "clojure" "-Spath"))]
      ;; First compile Java dependencies
      (println "Compiling Java dependencies...")
      (p/shell {:dir old-dir}
               "javac" "-cp" cp "-d" "target/classes"
               "java/src/datahike/java/IEntity.java"
               "java/src/datahike/java/Util.java")
      ;; Generate DatahikeGenerated.java
      (println "Generating DatahikeGenerated.java...")
      (p/shell {:dir old-dir}
               "clojure" "-M" "-m" "datahike.codegen.java" "java/src-generated"))

    (build/compile-java {:class-dir (str old-version-dir "/target/classes")
                         :java-src-dirs [(str old-version-dir "/java")
                                         (str old-version-dir "/java/src-generated")]
                         :deps-file (str old-version-dir "/deps.edn")})
    (fs/create-dirs (str old-version-dir "/" old-secondary-fixture))
    (fs/copy (str secondary-fixture "/backward_secondary_test.clj")
             (str old-version-dir "/" old-secondary-fixture
                  "/backward_secondary_test.clj"))

    (clj {:dir old-version-dir}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X" "backward-test/write")

    (println "WRITING RELEASED SECONDARY-INDEX FIXTURES")
    (clj {:dir old-version-dir
          :extra-env secondary-env}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"" old-secondary-fixture "\"]}")
         "-X:test" "backward-secondary-test/write")

    (println "READING TEST DATA FROM TEST-DB")
    (clj {:dir "."}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X" "backward-test/read")

    (println "VERIFYING RELEASED SECONDARY-INDEX ROOTS WITH CURRENT CODE")
    (clj {:dir "."
          :extra-env secondary-env}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X:test" "backward-secondary-test/verify-current")

    (println "WRITING CURRENT SECONDARY-INDEX ROOTS")
    (clj {:dir "."
          :extra-env secondary-env}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X:test" "backward-secondary-test/write-current")

    (println "REOPENING CURRENT SECONDARY-INDEX ROOTS IN A FRESH PROCESS")
    (clj {:dir "."
          :extra-env secondary-env}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"test/datahike/backward_compatibility_test/src\"]}")
         "-X:test" "backward-secondary-test/verify-current-formats")

    (println "VERIFYING RELEASED ROOTS REMAIN READABLE BY THE RELEASE")
    (clj {:dir old-version-dir
          :extra-env secondary-env}
         "-Sdeps" (str "{:deps {io.replikativ/datahike {:local/root \".\"}}"
                       " :paths [\"" old-secondary-fixture "\"]}")
         "-X:test" "backward-secondary-test/verify-old")
    (fs/delete-tree old-version-dir)
    (fs/delete-tree secondary-root)))

(defn- cli-binary
  "native-image appends .exe on Windows; the rest of the tree spells it dthk."
  []
  (first (filter fs/exists? ["./dthk" "./dthk.exe"])))

(defn native-image []
  (if (cli-binary)
    ;; Invoked through bash rather than executed directly: the script carries a
    ;; shebang, which Windows does not honour, and Git Bash is on PATH there.
    (p/shell "bash" "./bb/resources/native-image-tests/run-native-image-tests")
    (println "Native image cli missing. Please run 'bb ni-cli' and try again.")))

(defn libdatahike []
  (if (fs/exists? "./libdatahike/target")
    (p/shell "bash" "./bb/resources/native-image-tests/run-libdatahike-tests")
    (println "libdatahike binaries missing. Please run 'bb ni-compile' and try again.")))

(defn bb-pod []
  (if (cli-binary)
    ;; Through `bb` rather than executed directly, for the same reason
    ;; `native-image` goes through bash: the script carries a shebang, and
    ;; Windows does not honour one.
    (p/shell "bb" "./bb/resources/native-image-tests/run-bb-pod-tests.clj")
    (do (println "Native image cli missing. Please run 'bb ni-cli' and try again.")
        (System/exit 1))))

(defn python []
  (if (fs/exists? "./libdatahike/target")
    ;; Through bash, like `native-image` and `libdatahike` above: the script
    ;; carries a shebang, which Windows does not honour.
    (p/shell "bash" "./bb/resources/native-image-tests/run-python-tests")
    (println "libdatahike binaries missing. Please run 'bb ni-compile' and try again.")))

(defn specs []
  (kaocha "--focus" "specs" "--plugin" "kaocha.plugin/orchestra"))

(defn kabel []
  (kaocha "--focus" "kabel"))

(defn datomic
  "Datomic Pro <-> datahike migration. Needs the `:datomic` alias for the peer
   jar, so it cannot go through `kaocha` above, which pins `-M:test`:
   `datahike.migrate.datomic` requires `datomic.api` and fails to LOAD without
   it. `datomic:mem://` needs no license key or transactor, so this runs anywhere
   the jar resolves. Deliberately NOT part of `all` — the peer jar is not a
   datahike dependency and most contributors will not have it."
  []
  (apply clj {:extra-env {"LOG_LEVEL" ":warn"}}
         "-M:test:datomic" "-m" "kaocha.runner" ["--config-file" "tests-datomic.edn"]))

(defn cljs-node-test []
  (p/shell "clj -M:cljs -m shadow.cljs.devtools.cli compile :node-test")
  (p/shell "node target/out/node-test.js"))

(defn cljs-browser-test []
  (println "Installing npm dependencies...")
  (p/shell "npm install")
  (println "Starting Kabel test server...")
  (let [server-process (p/process "clojure -M:test -e \"(require 'datahike.kabel.browser-test-server) (datahike.kabel.browser-test-server/start-test-server!) (Thread/sleep Long/MAX_VALUE)\"")]
    (try
      (println "Waiting for server startup...")
      (Thread/sleep 5000)
      (println "Compiling browser tests...")
      (p/shell "npx shadow-cljs compile :browser-ci")
      (println "Running tests with Karma...")
      ;; Set CHROME_BIN to chromium if available
      (let [chrome-bin (or (System/getenv "CHROME_BIN")
                           (first (keep #(when (.exists (io/file %)) %)
                                        ["/snap/bin/chromium"
                                         "/usr/bin/chromium"
                                         "/usr/bin/chromium-browser"
                                         "/usr/bin/google-chrome"])))
            env (if chrome-bin
                  (assoc (into {} (System/getenv)) "CHROME_BIN" chrome-bin)
                  (into {} (System/getenv)))]
        (p/shell {:env env} "npx karma start --single-run"))
      (finally
        (println "Stopping test server...")
        (p/destroy server-process)))))

(defn all [config]
  (kaocha "--skip" "specs")
  (specs)
  (kabel)
  (back-compat config)
  (native-image)
  (libdatahike)
  (python)
  (bb-pod)
  (cljs-node-test)
  (cljs-browser-test))

(defn -main [config & args]
  (if (seq args)
    (case (first args)
      "native-image" (native-image)
      "libdatahike" (libdatahike)
      "python" (python)
      "bb-pod" (bb-pod)
      "back-compat" (back-compat config)
      "specs" (specs)
      "kabel" (kabel)
      "datomic" (datomic)
      "cljs-node" (cljs-node-test)
      "cljs-browser" (cljs-browser-test)
      (apply kaocha "--focus" args))
    (all config)))

(defn- wait-for-http! [process url]
  (loop [attempt 0]
    (when-not (p/alive? process)
      (throw (ex-info "The test server exited before it came up" {:url url})))
    (let [up? (try (= 200 (:status (babashka.http-client/get url {:throw false})))
                   (catch Exception _ false))]
      (cond up? true
            ;; `clojure -M` resolves the classpath before the JVM starts; the
            ;; native-image script measured 79 s warm, and a busy machine is slower.
            (< attempt 959) (do (Thread/sleep 250) (recur (inc attempt)))
            :else (throw (ex-info "The test server never came up" {:url url}))))))

(defn- with-remote-server! [description command]
  (fs/create-dirs "target")
  (let [config "bb/resources/remote-test-config.edn"
        base-url "http://localhost:32192"
        log (fs/file "target/remote-test-server.log")
        process (p/process ["clojure" "-M:http-server" config] {:out log :err log})]
    (try
      (println "Starting the test server (see target/remote-test-server.log)...")
      (wait-for-http! process (str base-url "/swagger.json"))
      (println description)
      (p/shell {:extra-env {"DATAHIKE_REMOTE_URL" base-url
                            "DATAHIKE_REMOTE_TOKEN" "remotetesttoken"
                            "DATAHIKE_LOG_LEVEL" "off"}}
               command)
      (finally
        (p/destroy-tree process)))))

(defn node-remote
  "The ClojureScript thin-client tests against a JVM server started for the run."
  []
  (println "Compiling the thin-client node tests...")
  (p/shell "npx shadow-cljs compile node-remote-test")
  (with-remote-server! "Running the thin-client node tests..."
    "node target/out/node-remote-test.js"))

(defn ts-client
  "The TypeScript thin-client tests against a JVM server started for the run."
  []
  (p/shell "npm run ts-client-build")
  (with-remote-server! "Running the TypeScript thin-client tests..."
    "node --test ts-client/test/remote.test.mjs"))
