(ns tasks.format
  (:require [utils.shell :refer [clj]]))

;; no babashka version of cljfmt found

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn check []
  (clj "-X:format" "cljfmt.main/check"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn fix []
  (clj "-X:format" "cljfmt.main/fix"))
