(ns datahike.test.scratch
  "Per-test directories. Resource-owning test bodies close resources before this
   outer fixture removes their files, on both successful and failed tests."
  (:require [clojure.java.io :as io])
  (:import [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *directory* nil)

(defn path [& parts]
  (when-not *directory*
    (throw (ex-info "Install scratch/fixture with use-fixtures :each" {})))
  (.getPath (apply io/file *directory* parts)))

(defn delete-tree! [directory]
  (when (Files/exists (.toPath (io/file directory)) (make-array java.nio.file.LinkOption 0))
    ;; Files.walk does not follow symlinks; never traverse outside the owned tree.
    (with-open [paths (Files/walk (.toPath (io/file directory))
                                  (make-array java.nio.file.FileVisitOption 0))]
      (doseq [^Path p (reverse (iterator-seq (.iterator paths)))]
        (Files/delete p)))))

(defn fixture [f]
  (let [dir (.toFile (Files/createTempDirectory "datahike-test-" (make-array FileAttribute 0)))]
    (try
      (binding [*directory* dir] (f))
      (finally (delete-tree! dir)))))
