(ns tools.cljdoc
  "Run cljdoc.org's analyzer against the built jar, locally.

   cljdoc builds documentation by *loading* every namespace shipped in the
   published jar (clj) and statically analyzing it (cljs). That means a jar can
   pass every unit test yet still fail on cljdoc.org — e.g. a namespace that
   requires a dep which isn't in the published pom (only in a dev/test alias), or
   a `.cljc` ns form the ClojureScript analyzer rejects. This task reproduces the
   exact cljdoc analysis step so those failures surface in CI before a release,
   instead of on cljdoc.org after it."
  (:require
   [babashka.fs :as fs]
   [babashka.process :as p]
   [clojure.string :as str]
   [tools.build :refer [jar-path pom-path]]
   [tools.version :as version]))

(def analyzer-sha
  "Commit of cljdoc/cljdoc-analyzer to run. Keep roughly in sync with the SHA
   cljdoc.org's builder uses — read `CLJDOC_ANALYZER_DEP` from a recent build at
   https://cljdoc.org/builds and bump this when it drifts. Pinned (rather than a
   floating branch) so the check is reproducible."
  "52a1281dff1a7d0421c58531f82eb29cf5a4ebfc")

(defn- analyzer-args [repo-config project-config jarpath pompath]
  ;; :languages nil -> cljdoc-analyzer auto-detects from the jar (clj + cljs when
  ;; any .cljc/.cljs is present), matching a real cljdoc.org build. Local absolute
  ;; paths for the jar/pom are used directly instead of downloading from Clojars.
  (pr-str {:project   (str (:lib project-config))
           :version   (version/string repo-config)
           :jarpath   (str (fs/absolutize jarpath))
           :pompath   (str (fs/absolutize pompath))
           :languages nil
           :repos     {"clojars" {:url "https://repo.clojars.org/"}
                       "central" {:url "https://repo.maven.apache.org/maven2/"}}}))

(defn- run-analyzer
  "Analyze `jarpath` (+ `pompath`) exactly as cljdoc.org does; exit non-zero if it
   fails."
  [repo-config project-config jarpath pompath]
  (when-not (fs/exists? jarpath)
    (println "cljdoc-check: no jar at" (str jarpath))
    (println "Build it first with `bb jar` (or run `bb cljdoc-check`, which does).")
    (System/exit 1))
  (let [dep  (format "{:deps {cljdoc/cljdoc-analyzer {:git/url \"https://github.com/cljdoc/cljdoc-analyzer.git\" :sha \"%s\"}}}"
                     analyzer-sha)
        args (analyzer-args repo-config project-config jarpath pompath)]
    (println "Running cljdoc-analyzer" analyzer-sha "against" (str jarpath))
    (let [{:keys [exit]} (p/shell {:continue true}
                                  "clojure" "-Sdeps" dep
                                  "-M" "-m" "cljdoc-analyzer.cljdoc-main" args)]
      (if (zero? exit)
        (println "cljdoc analysis succeeded — this jar would build on cljdoc.org.")
        (do
          (println)
          (println "cljdoc analysis FAILED — this jar would fail to build on cljdoc.org.")
          (println "See the exception above: a shipped namespace either can't be")
          (println "loaded (clj) or can't be analyzed (cljs). Fix it, or exclude the")
          (println "namespace with ^:no-doc, before releasing.")
          (System/exit exit))))))

(defn analyze
  "Analyze the freshly built jar under target/. Assumes the jar + pom already
   exist (run after `bb jar`)."
  [repo-config project-config]
  (run-analyzer repo-config project-config
                (jar-path repo-config project-config)
                (pom-path project-config)))

(defn- m2-artifact
  "Path of the jar/pom `bb install` places in the local Maven repo."
  [repo-config {:keys [lib]} ext]
  (let [version  (version/string repo-config)
        artifact (name lib)
        group    (str/replace (namespace lib) "." "/")]
    (str (System/getProperty "user.home")
         "/.m2/repository/" group "/" artifact "/" version "/"
         artifact "-" version "." ext)))

(defn analyze-installed
  "Analyze the jar already installed to the local Maven repo by `bb install`,
   instead of rebuilding it. Used by CI's cljdoc-analysis job, which runs after
   the build job has built + installed + persisted the jar — so the artifact is
   reused rather than recompiled."
  [repo-config project-config]
  (run-analyzer repo-config project-config
                (m2-artifact repo-config project-config "jar")
                (m2-artifact repo-config project-config "pom")))
