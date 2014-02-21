(ns
    #^{:author "Craig Ludington",
       :doc "Customer Zoo Tool.  Database functions."}
  zoo-tool.db)

(defn db-spec
  "Return a map of database connection parameters, suitable for use with sql/with-connection."
  [db-host db-port db-name db-user db-pass]
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname (str "//" db-host ":" db-port "/" db-name)
   :user db-user
   :password db-pass})
