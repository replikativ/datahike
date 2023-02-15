(ns tools.release
  (:require [babashka.fs :as fs]
            [borkdude.gh-release-artifact :as gh]
            [tools.build :refer [jar-path]]
            [tools.version :as version])
  (:import (clojure.lang ExceptionInfo)))

(defn fib [a b]
  (lazy-seq (cons a (fib b (+ a b)))))

(defn retry-with-fib-backoff [retries exec-fn fail-test-fn]
  (loop [idle-times (take retries (fib 1 2))]
    (let [result (exec-fn)]
      (if (fail-test-fn result)
        (do (println "Returned: " result)
            (if-let [sleep-ms (first idle-times)]
              (do (println "Retrying with remaining back-off times (in s): " idle-times)
                  (Thread/sleep (* 1000 sleep-ms))
                  (recur (rest idle-times)))
              result))
        result))))

(defn try-release [config]
  (try (gh/release-artifact
        {:org (:org config)
         :repo (name (:build/lib config))
         :tag (version/string config)
         :commit (version/current-commit)
         :file (jar-path config)
         :content-type "application/java-archive"
         :draft false})
       (catch ExceptionInfo e
         (assoc (ex-data e) :failure? true))))

(defn gh-release
  "Create a GitHub release and upload the library jar"
  [config]
  (println "Trying to release artifact...")
  (let [jar-file (jar-path config)
        _ (when-not (fs/exists? jar-file)
            (println "Library jar file at" jar-file "doesn't exist!")
            (System/exit 1))
        ret (retry-with-fib-backoff 10 #(try-release config) :failure?)]
    (if (:failure? ret)
      (do (println "GitHub release failed!")
          (System/exit 1))
      (println (:url ret)))))
