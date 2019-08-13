(defproject performance "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main performance.core
  :jvm-opts ["-Xmx2g" "-server"]
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [incanter/incanter-core "1.9.3"]
                 [incanter/incanter-charts "1.9.3"]
                 [incanter/incanter-io "1.9.3"]
                 [io.replikativ/datahike "0.2.0-beta4"]]
  :repl-options {:init-ns performance.core})
