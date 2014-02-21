 (ns
    #^{:author "Craig Ludington",
       :doc "Customer Zoo Tool.  Update the ''seed'' tables in the target database with values that should be there, but are only in the source database."}
   zoo-tool.seed
   (require [clojure.contrib.sql :as sql])
   (use [zoo-tool.config :only (config)]
	[zoo-tool.core :only ()]))

(defn have
  "Return the primary keys we already have for seed-table as a comma-delimited string.
   E.g. (1,3,99)"
  [target-db-spec seed-table primary-key]
  (str "("
       (apply str (interpose ","
			     (map (keyword primary-key)
				  (sql/with-connection target-db-spec
				    (sql/with-query-results rs
				      [ (str  "SELECT " (name primary-key) " FROM " (name seed-table)) ]
				      (reduce conj '() rs))))))
       ")"))

(defn need
  "Return the rows we need from the source database for the seed-table."
  [source-db-spec target-db-spec seed-table primary-key]
  (let [q (str  "SELECT * FROM " (name seed-table) " WHERE " (name primary-key) " NOT IN " (have target-db-spec seed-table primary-key))]
    (sql/with-connection source-db-spec
      (sql/with-query-results rs
	[q]
	(reduce conj '() rs)))))

(defn update-seed-table
  [source-db-spec target-db-spec seed-table primary-key]
  (let [rows (need source-db-spec target-db-spec seed-table primary-key)]
    (sql/with-connection target-db-spec
      (apply sql/insert-records (name seed-table) rows))))


(defn fix-triggers-and-drop-indices
  [target-db-spec]
  (sql/with-connection target-db-spec
    (sql/do-commands
     "alter table cnu.lead_seller_tiers disable trigger lead_seller_tiers__no_change_for_approved_versions"
     "alter table cnu.lead_seller_versions disable trigger lead_seller_versions__lead_seller_version_status_id"
     "alter table cnu.lead_seller_versions drop constraint lead_seller_versions_lead_seller_id_fkey"
     "alter table mef.equation_variables drop constraint equation_variables__ri_equation"
     "alter table mef.equation_variables drop constraint equation_variables__ri_variable"
     "alter table mef.equations disable trigger valid_transitions"
     "alter table mef.variables drop constraint variables_equation_id_fkey"
     "drop index mef.variables__u_variable_name__non_composite"
     "drop index cnu.email_templates__name__content_type__language__brand__country")))

(defn update
  "Update the ''seed'' tables in the target database."
  ([source-db-spec target-db-spec seed-tables]
     (fix-triggers-and-drop-indices target-db-spec)
     (doseq [{:keys [t pk]} seed-tables]
       (let [_ (println "Checking for need: " t " (" pk ")")
	     rows (need source-db-spec target-db-spec t pk)]
	 (when (seq rows)
	   (println t "(" pk ") " (count rows))
	   (update-seed-table source-db-spec target-db-spec t pk)
	   ))))
  ([source-db-spec target-db-spec]
     (update source-db-spec target-db-spec (-> config :prod :seed-tables)))
  ([target-db-spec]
     (update (-> config :prod :db) target-db-spec (-> config :prod :seed-tables)))
  ([]
     (update (-> config :prod :db) (-> config :targ :db) (-> config :prod :seed-tables))))

(comment
  ;;   <stuckmojo> craig: begin; update tools.settings set b = true where
  ;; 	    setting_name in ('Asserts disabled', 'Checks disabled'); update
  ;; 	    tools.settings set i = 2^32/2 -1 where setting_name = 'Minimum
  ;; 	    assert level'; ...
  ;; <stuckmojo> then do the opposite (b = false, i = 0) at the end  [15:17]
  ;; <stuckmojo> craig: 2^32/2 - 1 is an attempt to make a max signed int32
  ;; <stuckmojo> not really needed to go that high, i think our highest level
  ;; 	    assert is 1000  [15:18]
  ;; 
)


