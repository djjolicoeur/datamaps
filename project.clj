(defproject datamaps "0.1.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[datascript "0.15.0"]]
  :profiles {:dev {:plugins [[lein-kibit "0.1.2"]]}
             :provided {:dependencies [[org.clojure/clojure "1.8.0"]]}})
