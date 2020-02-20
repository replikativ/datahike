(defproject performance "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main performance.core
  :aot [performance.core]
  :jvm-opts ["-Xmx2g" "-server"]
  :dependencies [[org.clojure/clojure "1.10.0"]

                 [io.replikativ/datahike "0.2.1"]
                 [io.replikativ/datahike-leveldb "0.1.0"]
                 [io.replikativ/datahike-postgres "0.1.0"]

                 [com.datomic/datomic-free "0.9.5697"]

                 [criterium "0.4.5"]
                 [incanter/incanter-core "1.9.3"]
                 [incanter/incanter-charts "1.9.3"]
                 [incanter/incanter-io "1.9.3"]]
  :repl-options {:init-ns performance.core})
