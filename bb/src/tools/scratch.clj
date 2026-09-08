(ns tools.scratch
  "Owned scratch for Babashka tasks and their subprocesses."
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption OpenOption]
           [java.lang ProcessHandle]))

(defn root []
  (fs/absolutize (or (System/getenv "DATAHIKE_SCRATCH_ROOT") ".scratch")))

(defn usage [dir]
  (reduce (fn [result path]
            (try
              (-> result
                  (update :entries inc)
                  (update :bytes + (if (fs/regular-file? path {:nofollow-links true})
                                     (fs/size path) 0)))
              (catch java.nio.file.NoSuchFileException _ result)))
          {:bytes 0 :entries 0}
          (fs/glob dir "**" {:hidden true})))

(defn- channel [path]
  (FileChannel/open (fs/path path)
                    (into-array OpenOption [StandardOpenOption/CREATE StandardOpenOption/WRITE])))

(defn inspect!
  "Report usage, optionally reclaim finished runs. Incomplete crash records are
   retained: an unlocked dead parent does not prove its children are dead."
  ([] (inspect! false))
  ([clean?]
   (let [root (root)]
     (fs/create-dirs root)
     (doseq [dir (fs/list-dir root)
             :when (and (str/starts-with? (str (fs/file-name dir)) "run-")
                        (not (fs/sym-link? dir)) (fs/directory? dir))]
       (try
         (with-open [ch (channel (fs/path dir ".lock"))]
           (let [lock (try (.tryLock ch) (catch Exception _ nil))]
             (try
               (let [state (when lock (edn/read-string (slurp (fs/file dir "owner.edn"))))]
                 (binding [*out* *err*]
                   (println (str dir) (usage dir)
                            (if lock (:state state) :active)))
                 (when (and clean? lock (= :finished (:state state)))
                   ;; Windows cannot remove an open lock file.
                   (.close ch)
                   (fs/delete-tree dir)))
               (finally (.close ch)))))
         (catch Exception e
           (binding [*out* *err*]
             (println "Scratch retained:" (str dir) (ex-message e)))))))))

(defn- descendants []
  (with-open [stream (.descendants (ProcessHandle/current))]
    (vec (iterator-seq (.iterator stream)))))

(defn- stop-children! []
  (let [children (descendants)]
    (doseq [child (reverse children)] (.destroy child))
    (doseq [child children]
      (when (.isAlive child)
        (try (.get (.onExit child) 5 java.util.concurrent.TimeUnit/SECONDS)
             (catch java.util.concurrent.TimeoutException _
               (.destroyForcibly child)))))
    (doseq [child children]
      (when (.isAlive child)
        (try (.get (.onExit child) 5 java.util.concurrent.TimeUnit/SECONDS)
             (catch java.util.concurrent.TimeoutException _ nil))))
    children))

(defn start!
  "Install scratch before task dependencies execute. Returns idempotent cleanup.
   Nested bb processes inherit their parent's run instead of creating another."
  ([] (start! (not-empty (System/getenv "DATAHIKE_SCRATCH_RUN"))))
  ([inherited]
   (if-let [inherited inherited]
     (do (System/setProperty "java.io.tmpdir" (str (fs/path inherited "tmp")))
         (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (stop-children!))))
         (fn []))
     (let [_ (inspect! true)
           root (root)
           _ (fs/create-dirs root)
           dir (fs/create-temp-dir {:dir root :prefix "run-"})
           tmp (str (fs/path dir "tmp"))
           _ (fs/create-dirs tmp)
           ch (channel (fs/path dir ".lock"))
           _lock (.lock ch)
           owner (fs/file dir "owner.edn")
           done? (atom false)
           old-tmp (System/getProperty "java.io.tmpdir")
           old-defaults p/*defaults*
           cleanup (fn []
                     (when (compare-and-set! done? false true)
                      ;; Stop remaining foreground-task children before removing
                      ;; stores, native mappings, or temporary shared libraries.
                       (let [children (stop-children!)]
                         (try
                           (when-not (some #(.isAlive %) children)
                             (spit owner (pr-str {:state :finished})))
                           (finally
                             (.close ch)))
                         (if (some #(.isAlive %) children)
                           (binding [*out* *err*] (println "Scratch retained for live children:" (str dir)))
                           (fs/delete-tree dir)))
                       (System/setProperty "java.io.tmpdir" old-tmp)
                       (alter-var-root #'p/*defaults* (constantly old-defaults))))]
       (spit owner (pr-str {:state :running :pid (.pid (ProcessHandle/current))}))
       (System/setProperty "java.io.tmpdir" tmp)
       (alter-var-root
        #'p/*defaults* assoc :env
        (merge (into {} (System/getenv))
               (:env old-defaults)
               {"DATAHIKE_SCRATCH_RUN" (str dir)
                "TMPDIR" tmp "TMP" tmp "TEMP" tmp
                "JAVA_TOOL_OPTIONS" (str (or (get (:env old-defaults) "JAVA_TOOL_OPTIONS")
                                             (System/getenv "JAVA_TOOL_OPTIONS") "")
                                         " -Djava.io.tmpdir=\"" tmp "\"")}))
       (.addShutdownHook (Runtime/getRuntime) (Thread. cleanup))
       (doto (Thread. (fn []
                        (while (not @done?)
                          (Thread/sleep 30000)
                          (when-not @done?
                            (binding [*out* *err*]
                              (println "Scratch usage:" (str dir) (usage dir)))))))
         (.setDaemon true)
         (.start))
       (binding [*out* *err*] (println "Scratch:" (str dir)))
       cleanup))))

(defn task-command
  "Return an exact invocation when the OS exposes it. Never reconstruct a task
   from partial metadata: that would drop VM flags, config or runner options."
  [command arguments]
  (when-not (and command arguments)
    (throw (ex-info
            (str "Cannot inspect the complete Babashka invocation on this platform. "
                 "Launch explicitly with: bb --config bb/scratch.edn "
                 "-m tools.scratch run -- bb <original options and task>")
            {:type ::unavailable-task-command})))
  (into [command] arguments))

(defn ensure-task-run!
  "Re-enter the bb task once with a real inherited environment. This also covers
   tools.build's ProcessBuilder launches, which bypass babashka.process defaults."
  []
  (if (not-empty (System/getenv "DATAHIKE_SCRATCH_RUN"))
    (start!)
    (let [info (.info (ProcessHandle/current))
          command (task-command (.orElse (.command info) nil)
                                (.orElse (.arguments info) nil))
          cleanup (start!)]
      (try
        (let [result (apply p/shell {:continue true} command)]
          (cleanup)
          (System/exit (:exit result)))
        (finally (cleanup))))))

(defn -main [action & command]
  (case action
    "status" (inspect!)
    "clean" (inspect! true)
    "run" (let [cleanup (start!)]
            (try
              (let [command (if (= "--" (first command)) (rest command) command)
                    result (apply p/shell {:continue true} command)]
                (cleanup)
                (System/exit (:exit result)))
              (finally (cleanup))))
    (throw (ex-info "Expected run, status, or clean" {:action action}))))
