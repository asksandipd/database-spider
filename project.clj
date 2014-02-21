(defproject zoo-tool "1.0.0-SNAPSHOT"
  :description "Extract a complete object graph for a customer from a production database, scrub removing any PII and insert the graph into a development database"
  :dependencies [[org.clojure/clojure "1.2.0-beta1"]
                 [org.clojure/clojure-contrib "1.2.0-beta1"]
		 [org.clojars.alpheus/postgresql "8.4-701.jdbc3"]
		 [clj-record "1.0-SNAPSHOT"]
		 [robert/hooke "1.0.2"]]
  :dev-dependencies [[swank-clojure "1.2.0"]])
