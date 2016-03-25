(defproject datamaps "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:plugins [[lein-kibit "0.1.2"]]}
             :provided {:dependencies
                        [[org.clojure/clojure "1.7.0"]
                         [com.datomic/datomic-free "0.9.5350"]]}})
