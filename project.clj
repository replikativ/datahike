(defproject io.replikativ/datahike "0.2.0-beta1"
  :description "A durable datalog implementation adaptable for distribution."
  :license {:name "Eclipse"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/tonsky/datahike"

  :dependencies [[org.clojure/clojure       "1.10.0"   :scope "provided"]
                 [org.clojure/clojurescript "1.10.516" :scope "provided"]
                 [persistent-sorted-set     "0.1.1"]
                 [io.replikativ/hitchhiker-tree "0.1.4"]
                 [io.replikativ/superv.async "0.2.9"]
                 [io.replikativ/konserve-leveldb "0.1.2"]
                 [com.apple.foundationdb/fdb-java "5.1.7"]
                 [byte-streams "0.2.3"]
                 [funcool/octet "1.1.2"]]

  :plugins [
    [lein-cljsbuild "1.1.7"]
  ]

  :global-vars {
    *warn-on-reflection*   true
    *print-namespace-maps* false
;;     *unchecked-math* :warn-on-boxed
  }
  :jvm-opts ["-Xmx2g" "-server"]

  :aliases {"test-clj"     ["run" "-m" "datahike.test/test-clj"]
            "test-cljs"    ["do" ["cljsbuild" "once" "release" "advanced"]
                                 ["run" "-m" "datahike.test/test-node" "--all"]]
            "node-repl"    ["run" "-m" "user/node-repl"]
            "browser-repl" ["run" "-m" "user/browser-repl"]
            "test-all"     ["do" ["clean"] ["test-clj"] ["test-cljs"]]}
  
  :cljsbuild { 
    :builds [
      { :id "release"
        :source-paths ["src"]
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
        :source-paths ["src" "test"]
        :compiler {
          :output-to     "target/datahike.js"
          :optimizations :advanced
          :source-map    "target/datahike.js.map"
          :pretty-print  true
          :recompile-dependents false
          :parallel-build true
          :checked-arrays :warn
        }}

      { :id "bench"
        :source-paths ["src" "bench/src"]
        :compiler {
          :output-to     "target/datahike.js"
          :optimizations :advanced
          :source-map    "target/datahike.js.map"
          ; :pretty-print  true
          :recompile-dependents false
          :parallel-build true
          :checked-arrays :warn
          ; :pseudo-names  true
          :fn-invoke-direct true
          :elide-asserts true
        }}

      { :id "none"
        :source-paths ["src" "test"]
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
    :1.9 { :dependencies [[org.clojure/clojure         "1.9.0"   :scope "provided"]
                          [org.clojure/clojurescript   "1.9.946" :scope "provided"]] }
    :dev { :source-paths ["bench/src" "test" "dev"]
           :dependencies [[org.clojure/tools.nrepl     "0.2.13"]
                          [org.clojure/tools.namespace "0.2.11"]
                          [lambdaisland/kaocha         "0.0-389"]
                          [lambdaisland/kaocha-cljs    "0.0-16"]] }
    :aot { :aot [#"datahike\.(?!query-v3).*"]
           :jvm-opts ["-Dclojure.compiler.direct-linking=true"] }
  }
  
  :clean-targets ^{:protect false} [
    "target"
    "release-js/datahike.bare.js"
    "release-js/datahike.js"
  ]
)
