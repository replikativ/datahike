(ns datahike.codegen.naming
  "Shared naming conventions for JavaScript API.
  Used by both api_macros.clj and typescript.clj to ensure consistency."
  (:require [clojure.string :as str]))

;; Functions to skip in JS export (ClojureScript incompatible or aliases)
;; - transact: synchronous version that throws error in ClojureScript
;;             use transact! instead (which becomes 'transact' in JS)
(def js-skip-list #{'transact})

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
