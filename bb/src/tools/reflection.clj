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

(defn- ns-name-of
  "The namespace the file at `path` (found under `root`) declares, or nil if the
   file is not a Clojure source.

   Separator-agnostic on purpose. `File/getPath` hands back `src\\datahike\\api.cljc`
   on Windows, where a `\"^src/\"` prefix strip and a `\"/\"` -> `\".\"` replace both
   silently miss — and this gate reacts to a namespace it cannot name by
   swallowing the failure, so the whole check would pass having compiled
   nothing. A gate that reports success without looking is worse than no gate."
  [root path]
  (let [->slash #(str/replace % "\\" "/")
        root    (str/replace (->slash root) #"/+$" "")
        path    (->slash path)
        prefix  (str root "/")]
    (when (and (str/starts-with? path prefix)
               (re-find #"\.cljc?$" path))
      (-> (subs path (count prefix))
          (str/replace #"\.cljc?$" "")
          (str/replace "/" ".")
          (str/replace "_" "-")))))

(defn- namespaces
  "Every namespace under `src-roots`, as the compiler would name it."
  []
  (->> src-roots
       (mapcat #(->> (file-seq (java.io.File. ^String %))
                     (filter (fn [^java.io.File f] (.isFile f)))
                     (map (fn [^java.io.File f] [% (.getPath f)]))))
       (keep (fn [[root path]] (ns-name-of root path)))
       ;; data_readers.clj is a data file, not a namespace.
       (remove #{"data-readers"})
       sort
       distinct))

(defn check
  "Compile every namespace with *warn-on-reflection* and fail on any warning."
  [& _]
  (let [nses (namespaces)
        ;; An empty list would compile nothing and report "no reflective calls",
        ;; which is how a broken path convention used to hide here.
        _ (when (empty? nses)
            (println "No namespaces found under" (pr-str src-roots)
                     "- the reflection gate cannot check anything.")
            (System/exit 1))
        form (str "(set! *warn-on-reflection* true)"
                  "(doseq [n '" (pr-str (vec (map symbol nses))) "]"
                  "  (require n))")
        _ (println (format "Compiling %d namespaces with *warn-on-reflection*..."
                           (count nses)))
        {:keys [err exit]} (p/sh "clojure" "-M:libdatahike" "-e" form)
        ;; A namespace that does not load is a hard failure, not a skip. The
        ;; old form caught every Throwable, so a broken require was silently
        ;; dropped and the gate reported "no reflective calls" over whatever
        ;; happened to compile — the same silent pass the empty-list guard
        ;; below exists to prevent, one level down.
        _ (when-not (zero? exit)
            (println "Compilation failed (exit" exit ") — the reflection gate did not run:")
            (println err)
            (System/exit 1))
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
