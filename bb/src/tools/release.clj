(ns tools.release
  (:require [babashka.fs :as fs]
            [borkdude.gh-release-artifact.internal :as gh]
            [tools.build :refer [jar-path]]
            [tools.version :as version])
  (:import (clojure.lang ExceptionInfo)))

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

(defn try-release [config]
  (try (gh/overwrite-asset {:org (:org config)
                            :repo (name (:lib config))
                            :tag (version/string config)
                            :commit (gh/current-commit)
                            :file (jar-path config)
                            :content-type "application/java-archive"
                            :draft false})
       (catch ExceptionInfo e
         (assoc (ex-data e) :failure? true))))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn gh-release
  "Create a GitHub release and upload the library jar"
  [config]
  (println "Trying to release artifact...")
  (let [jar-file (jar-path config)]
    (when-not (fs/exists? jar-file) 
      (println "Library jar file at" jar-file "doesn't exist!")
      (System/exit 1))
  (-> (retry-with-fib-backoff 10 #(try-release config) :failure?)
      :url
      println)))
