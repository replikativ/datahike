(ns utils.shell
  (:require [babashka.process :as p]
            [clojure.string :as str]
            [io.aviso.ansi :as a])
  (:import [java.time Duration]))

(def ^:dynamic *cwd* nil)
(def ^:dynamic *opts* {:inherit true})

(defmacro with-opts [opts & body]
  `(binding [*opts* (merge *opts* ~opts)]
     ~@body))

(defn- now [] (System/currentTimeMillis))

(defn- format-millis [ms]
  (let [duration (Duration/ofMillis ms)
        h        (mod (.toHours duration) 24)
        m        (mod (.toMinutes duration) 60)
        s        (mod (.toSeconds duration) 60)
        ms       (mod ms 1000)]
    (str
     (a/bold-magenta "->")
     " "
     (a/bold-yellow
      (str
       (when (> h 0)
         (str h " hours, "))
       (when (> m 0)
         (str h " minutes, "))
       s "." ms " seconds")))))

(def ^:private in-bb? (some? (System/getProperty "babashka.version")))

(def ^:private fns
  (when in-bb? {:clojure (requiring-resolve 'babashka.deps/clojure)}))

(defn- clip
  ([arg]
   (clip arg 80))
  ([arg soft-max]
   (clip arg soft-max (- soft-max 5)))
  ([arg soft-max hard-max]
   (if-let [n (str/index-of arg "\n")]
     (str (subs arg 0 (min n hard-max)) "(...)")
     (if (> (count arg) soft-max)
       (str (subs arg 0 hard-max) "(...)")
       arg))))

(defn- sh* [& args]
  (binding [*out* *err*]
    (when *cwd*
      (println (a/blue "cd") (a/bold-blue *cwd*)))
    (println (a/bold-magenta "=>")
             (a/bold-green (first args))
             (a/bold-white (str/join " " (map (comp clip str) (rest args))))))
  (let [opts   (merge {:dir      *cwd*
                       :shutdown p/destroy-tree}
                      (when-not (:inherit *opts*)
                        {:out *out*
                         :err *err*})
                      *opts*)
        start  (now)
        result (some-> (if-let [f (get fns (first args))]
                         (f (map str (rest args)) opts)
                         (p/process (map str args) opts))
                       deref)
        exit   (:exit result 0)]
    (when-not (:inherit opts)
      (.flush *out*)
      (.flush *err*))
    (binding [*out* *err*]
      (println
       (str (format-millis (- (now) start))
            (when-not (zero? exit)
              (str " " (a/bold-red (str "(exit: " exit ")")))))))
    (when-not (zero? exit)
      (throw (ex-info "Non-zero exit code"
                      {:cwd *cwd*
                       :opts *opts*
                       :args args
                       :exit exit})))
    result))

(defn sh [& args]
  (if-let [session (:session *opts*)]
    (apply session sh* args)
    (apply sh* args)))

(def clj (partial #'sh "clojure"))
(def git (partial #'sh "git"))
