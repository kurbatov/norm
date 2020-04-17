(defproject norm "0.1.0-SNAPSHOT"
  :description "NORM is not an ORM"
  :url "http://github.com/kurbatov/norm"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [seancorfield/next.jdbc "1.0.409"]
                 [camel-snake-kebab "0.4.1"]]
  :repl-options {:init-ns norm.core
                 :caught  clojure.stacktrace/print-stack-trace}
  :profiles {:dev {:dependencies [[com.h2database/h2 "1.4.200"]
                                  [com.zaxxer/HikariCP "3.4.2"]]}})
