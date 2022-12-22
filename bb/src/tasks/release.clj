(ns tasks.release
  (:require
    [borkdude.gh-release-artifact.internal :as gh]                   ;; functions in internal???
    [tasks.build :refer [package jar-path]]
    [tasks.settings :refer [load-settings]]
    [tasks.version :refer [version-str]])
  (:import (clojure.lang ExceptionInfo)))

(def settings (load-settings))

(defn fib [a b]
  (lazy-seq (cons a (fib b (+ a b)))))

(defn retry-with-fib-backoff [retries exec-fn test-fn]
  (loop [idle-times (take retries (fib 1 2))]
    (let [result (exec-fn)]
      (if (test-fn result)
        (when-let [sleep-ms (first idle-times)]
          (println "Returned: " result)
          (println "Retrying with remaining back-off times (in s): " idle-times)
          (Thread/sleep (* 1000 sleep-ms))
          (recur (rest idle-times)))
        result))))

(defn try-release []
  (try (gh/overwrite-asset {:org (:org settings)
                            :repo (name (:lib settings))
                            :tag (version-str)
                            :commit (gh/current-commit)
                            :file jar-path
                            :content-type "application/java-archive"
                            :draft false})
       (catch ExceptionInfo e
         (assoc (ex-data e) :failure? true))))

(defn release []
  (println "Trying to release artifact...")
  (-> (retry-with-fib-backoff 10 #(try-release) :failure?)
      :url
      println))

(defn -main []
  (package)
  release)
