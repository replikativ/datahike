(ns tools.reflection
  "Fail the build on reflective calls in our own sources.

  Reflection is a latency cost on the JVM. Under native-image it is a
  correctness cost: a reflective call resolves at run time, so it works in the
  binary only if the target was registered for reflection when the image was
  built. That turns a warning the compiler prints and nobody reads into a
  runtime failure in a shipped artifact, which is why this is a gate and not a
  report.

  Only warnings under `datahike/` count. A dependency's warnings are real but
  are not fixable from here, and gating on them would make our build red when
  someone else's release regresses."
  (:require [babashka.process :as p]
            [clojure.string :as str]))

(def ^:private src-roots ["src" "libdatahike/src"])

(defn- namespaces
  "Every namespace under `src-roots`, as the compiler would name it."
  []
  (->> src-roots
       (mapcat #(->> (file-seq (java.io.File. ^String %))
                     (filter (fn [^java.io.File f] (.isFile f)))
                     (map (fn [^java.io.File f] [% (.getPath f)]))))
       (keep (fn [[root path]]
               (when (re-find #"\.cljc?$" path)
                 (-> path
                     (str/replace (re-pattern (str "^" root "/")) "")
                     (str/replace #"\.cljc?$" "")
                     (str/replace "/" ".")
                     (str/replace "_" "-")))))
       ;; data_readers.clj is a data file, not a namespace.
       (remove #{"data-readers"})
       sort
       distinct))

(defn check
  "Compile every namespace with *warn-on-reflection* and fail on any warning."
  [& _]
  (let [nses (namespaces)
        form (str "(set! *warn-on-reflection* true)"
                  "(doseq [n '" (pr-str (vec (map symbol nses))) "]"
                  "  (try (require n) (catch Throwable _ nil)))")
        _ (println (format "Compiling %d namespaces with *warn-on-reflection*..."
                           (count nses)))
        {:keys [err]} (p/sh "clojure" "-M:libdatahike" "-e" form)
        warnings (->> (str/split-lines (or err ""))
                      (filter #(str/starts-with? % "Reflection warning"))
                      (filter #(str/includes? % "datahike/"))
                      distinct
                      sort)]
    (if (seq warnings)
      (do (println)
          (println (count warnings) "reflective call(s) in datahike sources:")
          (println)
          (doseq [w warnings] (println " " w))
          (println)
          (println "Add a type hint. See tools.reflection for why this is a gate.")
          (System/exit 1))
      (println "No reflective calls in datahike sources."))))
