(defproject norm "SNAPSHOT"
  :description "NORM is not an ORM"
  :url "http://github.com/kurbatov/norm"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[com.github.seancorfield/next.jdbc "1.2.737"]
                 [camel-snake-kebab "0.4.1"]]
  :repl-options {:init-ns norm.core
                 :caught  clojure.stacktrace/print-stack-trace}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [com.h2database/h2 "1.4.200"]]}})
