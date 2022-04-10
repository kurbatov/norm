(defproject norm "0.1.0-SNAPSHOT"
  :description "NORM is not an ORM"
  :url "http://github.com/kurbatov/norm"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[com.github.seancorfield/next.jdbc "1.2.780"]
                 [camel-snake-kebab "0.4.2"]]
  :repl-options {:init-ns norm.core
                 :caught  clojure.stacktrace/print-stack-trace}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.3"]
                                  [com.h2database/h2 "1.4.200"]]}})
