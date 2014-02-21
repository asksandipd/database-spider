(ns
    #^{:author "Craig Ludington",
       :doc "Customer Zoo Tool.  Build a test database schema and populate it. The unit tests depend on this schema."}
  zoo-tool.schema
  (use [zoo-tool.db :only (db-spec)]
       [clojure.contrib.shell :only (sh)]
       [clojure.contrib.string :only (split)])
  (require [clojure.contrib.sql :as sql]))


;;;;
;;;; Generating a test database schema
;;;;


;;; Table Specs -- [dt & rts]
;;; Each spec that's a vector is a table name (dt) followed by zero or more referred tables names (rts).
;;; Each spec that's a string is executed as an sql command.
(def table-specs
     [[:a]
      [:ba :a]
      [:bb :a]
      [:bc :a]
      "ALTER TABLE bc ADD COLUMN other_a_id INT"
      "ALTER TABLE bc ADD CONSTRAINT bc_a__other_a_id FOREIGN KEY(other_a_id) REFERENCES a(id)"
      [:ca :ba :bb]
      [:x]
      [:ya :x]
      [:yb :x]
      [:z :bb :ya]])

;;; List the dependent and referred tables and the dependent column (when it couldn't be trivially inferred).
(def test-dependencies
     (letfn [(frob
	      ([dt rt] (frob dt rt rt))
	      ([dt rt dc]
		 {:dt (name dt)
		  :dc (str (name dc) "_id")
		  :rt (name rt)
		  :rc "id"}))] 
       (map #(apply frob %)
	    [[:ba :a]
	     [:bb :a]
	     [:bc :a]
	     [:bc :a :other_a]
	     [:ca :ba]
	     [:ca :bb]
	     [:ya :x]
	     [:yb :x]
	     [:z :bb]
	     [:z :ya]])))

(defn gen-constraint
  [dt rt]
  (let [constraint-name (str (name dt) "_" (name rt) "__" (name rt) "_id")]
    [(str "CONSTRAINT " constraint-name " FOREIGN KEY(" (name rt) "_id) REFERENCES " (name rt) "(id)" )]))

(defn gen-fk
  [table-name]
  (keyword (str (name table-name) "_id")))

(defn gen-columns
  [table-name referents]
  (map #(vector [(gen-fk %) :int] (gen-constraint table-name %)) referents))

(defn make-table-argv
  [table-name referents]
  (reduce into [table-name [:id :int "PRIMARY KEY"]] (gen-columns table-name referents)))

(defn make-table
  [table-name & referents]
  (apply sql/create-table (make-table-argv table-name referents)))

(defn make-schema
  "This generates a test schema, given a db spec and a vector of table specs."
  [db specs]
  (doall (map (fn [spec]
		(sql/with-connection db
		  (if (string? spec)
		    (sql/do-commands spec)
		    (apply make-table spec))))
	      specs)))

;;; Insert test data
(defn populate-test-schema
  [db]
  (sql/with-connection db
    (sql/transaction
     ;; Tier 0
     (sql/insert-values :a  [:id]                   [1]         [2]        )
     (sql/insert-values :x  [:id]                   [3]                    )
     ;; Tier 1						        
     (sql/insert-values :ba [:id :a_id]             [10 1]      [20 2]     )
     (sql/insert-values :bb [:id :a_id]             [30 1]                 )
     (sql/insert-values :bc [:id :a_id :other_a_id] [40 1 2]    [50 1 nil] )
     (sql/insert-values :ya [:id :x_id]             [60 3]      [70 3]     )
     (sql/insert-values :yb [:id :x_id]             [80 3]                 )
     ;; Tier 2
     (sql/insert-values :ca [:id :ba_id :bb_id]     [100 10 30]            )
     (sql/insert-values :z  [:id :bb_id :ya_id]     [200 30 60]            ))))


(defn setup
  [db-spec]
  (let [db-name (last (clojure.contrib.string/split #"/" (:subname db-spec)))]
    (sh "dropdb" db-name)
    (sh "createdb" db-name)
    (make-schema db-spec table-specs)
    (populate-test-schema db-spec)))
