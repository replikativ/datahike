(defproject io.replikativ/datahike "0.1.3"
  :description "A durable datalog implementation adaptable for distribution."
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/replikativ/datahike"

  :dependencies [[org.clojure/clojure       "1.8.0" :scope "provided"]
                 [org.clojure/clojurescript "1.8.51" :scope "provided"]
                 [io.replikativ/hitchhiker-tree "0.1.4"]
                 [io.replikativ/superv.async "0.2.9"]
                 [io.replikativ/konserve-leveldb "0.1.2"]
                 [com.apple.foundationdb/fdb-java "5.1.7"]
                 [byte-streams "0.2.3"]
                 [funcool/octet "1.1.1"]]

  :plugins [
    [lein-cljsbuild "1.1.5"]
  ]



  :global-vars {
;;    *warn-on-reflection* true
;;     *unchecked-math* :warn-on-boxed
  }

  :jvm-opts ["-Xmx2g" "-server"]

  :aliases {"test-clj"     ["run" "-m" "datahike.test/test-most"]
            "test-clj-all" ["run" "-m" "datahike.test/test-all"]
            "node-repl"    ["run" "-m" "user/node-repl"]
            "browser-repl" ["run" "-m" "user/browser-repl"]
            "test-all"     ["do" ["clean"]
                                 ["test-clj-all"]
                                 ["cljsbuild" "once" "release" "advanced"]
                                 ["run" "-m" "datahike.test/test-node" "--all"]]
            "test-1.8"     ["with-profile" "dev,1.8" "test-all"]
            "test-1.9"     ["with-profile" "dev,1.9" "test-all"]}

  :cljsbuild {
    :builds [
      { :id "release"
        :source-paths ["src" "bench/src"]
        :assert false
        :compiler {
          :output-to     "release-js/datahike.bare.js"
          :optimizations :advanced
          :pretty-print  false
          :elide-asserts true
          :output-wrapper false
          :parallel-build true
          :checked-arrays :warn
        }
        :notify-command ["release-js/wrap_bare.sh"]}

      { :id "advanced"
        :source-paths ["src" "bench/src" "test"]
        :compiler {
          :output-to     "target/datahike.js"
          :optimizations :advanced
          :source-map    "target/datahike.js.map"
          :pretty-print  true
          :recompile-dependents false
          :parallel-build true
          :checked-arrays :warn
        }}

      { :id "none"
        :source-paths ["src" "bench/src" "test" "dev"]
        :compiler {
          :main          datahike.test
          :output-to     "target/datahike.js"
          :output-dir    "target/none"
          :optimizations :none
          :source-map    true
          :recompile-dependents false
          :parallel-build true
          :checked-arrays :warn
        }}
  ]}

  :profiles {
    :1.8 { :dependencies [[org.clojure/clojure       "1.8.0" :scope "provided"]
                          [org.clojure/clojurescript "1.8.51" :scope "provided"]] }
    :1.9 { :dependencies [[org.clojure/clojure       "1.9.0-alpha17" :scope "provided"]
                          [org.clojure/clojurescript "1.9.671" :scope "provided"]]
           ;; because we use printer in tests, and earlier versions don’t support it
           :global-vars  { *print-namespace-maps* false }}
    :dev { :source-paths ["bench/src" "test" "dev"]
          :dependencies [[org.clojure/tools.nrepl "0.2.12"]
                         ] }
  }

  :clean-targets ^{:protect false} [
    "target"
    "release-js/datahike.bare.js"
    "release-js/datahike.js"
  ]
)
