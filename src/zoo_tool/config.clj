 (ns
    #^{:author "Craig Ludington",
       :doc "Customer Zoo Tool.  Configuration values.  Everything in here is overridden by whoever installs the program."}
   zoo-tool.config
   (require [zoo-tool.schema]
            [zoo-tool.db]
            [clojure.contrib.sql :as sql])
   (use [zoo-tool.schema :only (test-dependencies)]
        [zoo-tool.db     :only (db-spec)]))

(defmacro ignore-connection-errors [db & body]
  `(try (sql/with-connection  ~db ~@body)
	(catch org.postgresql.util.PSQLException e#
	  (println "ignore-connection-errors: ignoring: " e#))))

(let [slave-db (db-spec "dbhost-obscured" 5432 "db-obscured" "user-obscured" "pass-obscured")
      ignore-tables #{"payment_instruments.payment_instruments"}
      cnu-scrub-strategy {"table-name-obscured"
			  {:first_name                          :random
			   :last_name                           :random
			   :middle_name                         :random
			   :ssn                                 #"(\d{3})(\d{2})(\d{4})"
			   :home_phone                          :random
			   :work_phone                          :random
			   :mobile_phone                        :random
			   :stateid_num                         :random
			   :nin                                 :random
			   :passport                            :random
			   :callback_phone                      :random
			   :sin                                 :random}
			  "table-name-obscured"
			  {:work_phone                          :random 
			   :last_name                           :random
			   :birth_date                          :random
			   :nin                                 :random
			   :passport                            :random
			   :routing_num                         :random
			   :ssn                                 #"(\d{3})(\d{2})(\d{4})"
			   :home_phone                          :random
			   :line1                               :random
			   :line2                               :random
			   :idn                                 #"(.{5})(\d{3})(\d{2})(\d{4})" ;; 'US:S:ddddddddd' US SSN -- probably fails for GB etc.
			   :sin                                 :random   
			   :email                               #"(.*)(@)(example.com)"
			   :first_name                          :random
			   :account_num                         :random 
			   :middle_name                         :random}
                          }]

  (def config
       {:prod {:db slave-db
	       :dependencies (ignore-connection-errors slave-db
	       		       (sql/with-query-results rs
	       			 [ "SELECT referrer_namespace   || '.' || referrer_relation   AS dt,
                                           referrer_attribute                                 AS dc,
                                           referenced_namespace || '.' || referenced_relation AS rt,
                                           referenced_attribute                               AS rc
                                    FROM   sstrauch.zoo
                                    WHERE  kind IN ('c', 'i', 'm')"]
	       			 (reduce conj '() rs)))
	       :ignore-tables ignore-tables
	       :primary-keys  (ignore-connection-errors slave-db
				(sql/with-query-results rs
				  [ "SELECT DISTINCT t AS \"table\", pk AS \"primary-key\"
                                     FROM (
                                        SELECT referrer_namespace   || '.' || referrer_relation                            AS t,
                                               sstrauch.catalog_get_primary_key(referrer_namespace, referrer_relation)     AS pk
                                        FROM   sstrauch.zoo
                                        UNION
                                        SELECT referenced_namespace || '.' || referenced_relation                          AS t,
                                               sstrauch.catalog_get_primary_key(referenced_namespace, referenced_relation) AS pk
                                        FROM   sstrauch.zoo
                                      ) AS x"]
				  (reduce into {} (map #(hash-map (:table %) (keyword (:primary-key %))) rs))))
	       :seed-tables (ignore-connection-errors slave-db
	       		       (sql/with-query-results rs
	       			 ["SELECT DISTINCT
                                          z.referenced_namespace || '.' || z.referenced_relation AS t,
                                          z.referenced_attribute                                 AS pk,
                                          tso.dump_order                                         AS dump_order
                                   FROM   sstrauch.zoo z,
                                          tools.seed_objects tso
                                   WHERE  z.kind = 's'
                                   AND    z.referenced_namespace NOT IN (SELECT excluded_schema from sstrauch.nozoo)
                                   AND    tso.schema_name        || '.' || tso.object_name
                                           =
                                          z.referenced_namespace || '.' || z.referenced_relation
                                   ORDER BY tso.dump_order" ]
	       			 (reduce conj '() rs)))
	       :scrub-strategy cnu-scrub-strategy
	       :debug-everything true
	       :debug-sql true
	       }

	:test {:db (db-spec "localhost" 5432 "zoo-test" (System/getProperty "user.name") "")
	       :dependencies test-dependencies
	       :ignore-tables #{}
	       :primary-keys (fn [_] :id)
	       :scrub-strategy {}
	       :debug-sql true}

	:targ {:db (db-spec "parasite.dev.cashnetusa.com" 5432 "targ_us" "postgres" "")
	       :dependencies test-dependencies
	       :ignore-tables #{}
	       :primary-keys (fn [_] :id)
	       :debug-sql true}}))



