(ns datahike.codegen.naming
  "Shared naming conventions for JavaScript API.
  Used by both api_macros.clj and typescript.clj to ensure consistency."
  (:require [clojure.string :as str]))

;; Functions to skip in JS export (ClojureScript incompatible or aliases)
;; - transact/merge-db/writer-barrier: synchronous writer operations are not the JavaScript
;;             contract; use their async siblings, whose trailing ! is removed
;;             (`transact!` -> `transact`, `merge-db!` -> `mergeDb`)
;; - warm-*:   EXPERIMENTAL index warming. The walk's ClojureScript arm is REAL
;;             now (persistent-sorted-set >= 0.5.142 owns it; datahike adapts
;;             its partial-cps shape onto a channel — see
;;             datahike.index.persistent-set.warm). What keeps these skipped is
;;             only the JS binding round: the generated wrappers need the
;;             channel->Promise adaptation and regenerated artifacts, which is
;;             its own change. Remove them from this set in that round.
(def js-skip-list #{'transact 'merge-db 'writer-barrier 'warm-index 'warm-datoms 'warm-db})

(defn clj-name->js-name
  "Convert Clojure kebab-case to JavaScript camelCase.
  
  Examples:
    database-exists? -> databaseExists
    create-database -> createDatabase
    transact! -> transact (removes the !)
    with -> withDb (avoids JS reserved keyword)"
  [clj-name]
  (let [s (name clj-name)
        ;; Remove trailing ? or !
        s (cond-> s
            (str/ends-with? s "?") (subs 0 (dec (count s)))
            (str/ends-with? s "!") (subs 0 (dec (count s))))
        ;; Split on hyphens
        parts (str/split s #"-")
        ;; camelCase: first part lowercase, rest capitalized
        base-name (str (first parts)
                       (apply str (map str/capitalize (rest parts))))]
    ;; Handle JavaScript reserved words
    (if (= base-name "with")
      "withDb"
      base-name)))

(defn assert-unique-js-names!
  "Fail code generation when two Clojure API names collapse to one JavaScript
  name. A duplicate export can otherwise compile into the IIFE while producing
  an invalid ESM wrapper and ambiguous TypeScript declarations."
  [clj-names]
  (let [collisions (->> clj-names
                        (group-by clj-name->js-name)
                        (keep (fn [[js-name names]]
                                (when (< 1 (count names))
                                  [js-name (vec names)])))
                        (into {}))]
    (when (seq collisions)
      (throw (ex-info "JavaScript API name collision"
                      {:collisions collisions})))))

(defn remote-js-exports
  "The specification names the thin HTTP client exposes to JavaScript: the
   remote-capable functions. `js-skip-list` exists because two functions can
   share a JavaScript name (`transact` and `transact!` both become
   `transact`); a skipped name comes back here when the function that
   displaced it is not remote-capable, since on the thin client there is
   nothing to collide with."
  [api-specification]
  (let [remote (for [[n {:keys [supports-remote?]}] (sort-by first api-specification)
                     :when supports-remote?]
                 n)
        taken (frequencies (map clj-name->js-name remote))]
    (for [n remote
          :when (or (not (contains? js-skip-list n))
                    (= 1 (get taken (clj-name->js-name n))))]
      n)))
