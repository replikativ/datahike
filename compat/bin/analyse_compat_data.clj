#!/usr/bin/env bb
(require '[babashka.fs :as fs])
(require '[clojure.string :as s])

(def header "<!DOCTYPE html>
<html>
<head>
<title>Datahike Compatibility</title>
</head>
<body>
<table>")

(def footer "</table></body>
</html>")

(defn cell [s]
  (if (string? s)
    (format "<td>%s</td>" s)
    (if s
      "<td style=text-align:center>🟩</td>"
      "<td style=text-align:center>🟥</td>")))

(let [[compat-folder] *command-line-args*]
  (when (empty? compat-folder)
    (println "Usage: <compat-folder>")
    (System/exit 1))
  (let [header-data (->> ["Lib Version" "Db Version" "Connection?" "Count?" "Data?" "Schema?" "Reverse Schema?" "Tx Count?" "Import" "Import Connection?" "Import Count?" "Import Data?" "Import Schema?" "Import Reverse Schema?" "Import Tx Count?"]
                         (map #(format "<th>%s</th>" %))
                         s/join)
        header-row (s/join ["<tr>" header-data "</tr>"])
        body (->> (fs/match compat-folder "regex:.*\\.edn")
                  sort
                  (mapv (fn [result]
                          (let [{:datahike.compat/keys [connection? count? schema? reverse-schema? tx-count? import? data?]
                                 import-connection? :datahike.import/connection?
                                 import-count? :datahike.import/count?
                                 import-schema? :datahike.import/schema?
                                 import-reverse-schema? :datahike.import/reverse-schema?
                                 import-tx-count? :datahike.import/tx-count?
                                 lib-version :datahike.lib/version
                                 import-data? :datahike.lib/data?
                                 db-version :datahike.db/version} (read-string (slurp (fs/file result)))
                                row-data (->> [lib-version db-version connection? count? data? schema? reverse-schema? tx-count? import?
                                               import-connection? import-count? import-data? import-schema? import-reverse-schema? import-tx-count?]
                                              (mapv cell)
                                              s/join)]
                            (s/join ["<tr>" row-data "</tr>"]))))
                  s/join)
        html (s/join [header header-row body footer])]
    (spit (format "%s/compat_overview.html" compat-folder) html)))
