(ns
    #^{:author "Craig Ludington",
       :doc "Customer Zoo Tool.  Extract the complete object graph for a customer from a production database,
             scrub to obscure all Personally Identifiable Information, then inject the clean object graph
             into a development database."}
  zoo-tool.core
  (use [zoo-tool.graph  :only (dependency-list post-ordered-nodes)]
       [zoo-tool.config :only (config)]
       [clojure.contrib.string :only (split)]
       [clojure.pprint  :only (pprint)])
  (require [zoo-tool.dot :as dot]
	   [clojure.contrib.sql :as sql]
	   [clojure.contrib.math :as math]
	   [clojure.contrib.shell-out :as shell-out]))

;;;; Configuration parameters
;;;; Don't bind these by hand, the with-config macro takes care of all the bindings.
(def
 ^{:doc "Bind to the value returned by db-spec before calling the data collection functions."}
 *db*
 nil)

(def
 ^{:doc "Bind to a set of tables that must not be selected.  For any table in this set, row-of will return nil."}
 *ignore-tables*
 nil)

(def
 ^{:doc "Bind to a map of table name (a string) to primary key column (a keyword). E.g. {\"myfans\" :id, \"things\" :itemnumber}"}
 *primary-keys*
 nil)

(def
 ^{:doc "Bind to the value returned by object-dependencies before calling any of the graph or data collection functions."}
 *dependencies*
 nil)

(def
 ^{:doc "Bind to a map, indexed by the string table name, returning a map, indexed by the keyword column name
   where each column name is a scrub strategy to use.

   Possible scrub strategies are:

     string - a literal string will be used as the replacement for the original value
     random - a value randomly selected from the same column in the same table will be used as the replacement for the original value
     regex  - mix/match - each grouping in the regex will be randomly selected from the same column in the same table and the
              results will be concatenated together and used as the replacement for the original value.  If the entire column value isn't
              described and collected by the regex, that's probably a mistake.

   For example, this *scrub-strategy*:

     { \"my_table\" { :column1 :random, :column2 '', :column3 #\"(.*)(@)(.*)\" }}

   says that there are three columns for my_table that need to be scrubbed.
   After scrubbing, column1 will be the same value as some randomly selected column1 from the source database,
   and column2 will be the empty string.  Column3 (presumably an email address) would be some randomly
   selected user name, @, and some randomly selected domain name.  N.B. The @ must be in parentheses or it won't
   be part of the replacement value."}
 *scrub-strategy*
 nil)

(def
 ^{:doc "Bind to true to see SQL queries and ignore database timeouts."}
 *debug-sql*
 nil)

(def
 ^{:doc "Bind to true to see calls to parents-of, children-of, ancestors-of, and descendents-of."}
 *debug-everything*
 nil)

(defmacro with-config [config-key & body]
  "Execute body with the configuration for config-key, which must be a key of the zoo-tool.config/config map.
    Bindings are established for *db*, *dependencies*, *ignore-tables* and *primary-keys*."
  `(let [k# ~config-key
	 c# (config k#)]
     (when-not c# (throw (Exception. (str "No such configuration: " k#))))
     (binding [*db*               (:db c#)
	       *dependencies*     (:dependencies c#)
	       *ignore-tables*    (:ignore-tables c#)
	       *primary-keys*,    (:primary-keys c#)
	       *scrub-strategy*   (:scrub-strategy c#)
	       *debug-sql*        (:debug-sql c#)
	       *debug-everything* (:debug-everything c#)]
       ~@body)))

(defn- configure-for-dev
  "Debugging aid -- bind the dynamic vars to their values for a specific configuration."
  [key]
  (with-config key
    (def *db* *db*)
    (def *dependencies*     *dependencies*)
    (def *ignore-tables*    *ignore-tables*)
    (def *primary-keys*     *primary-keys*)
    (def *scrub-strategy*   *scrub-strategy*)
    (def *debug-sql*        *debug-sql*)
    (def *debug-everything* *debug-everything*)))

(def ^{:doc "Bound to an atom referencing an array when collecting DOT graph output."} *dot* nil)
(defmacro with-png-graph [file & body]
  "Execute body in an environment where object graph collection is recorded in a PNG file."
  `(binding [*dot* (atom [])]
     (let [result# (do ~@body)]
       (shell-out/sh :in (dot/digraph "G" (dot/graph-attributes :rotate "90") (apply str (deref *dot*)))
		     "dot" "-Tpng" "-o" ~file)
       result#)))

;; http://stackoverflow.com/questions/1683680/clojure-with-timeout-macro
;; http://kotka.de/blog/2010/05/Did_you_know_IV.html
(defmacro time-limited [ms & body]
  `(let [f#   (bound-fn [] ~@body)
	 fut# (future (f#))]
     (.get fut# ~ms java.util.concurrent.TimeUnit/MILLISECONDS)))


;; Memoization with cache access
;; Credit to a_strange_guy via http://www.paullegato.com/blog/memoize-reset-clojure/
(defn memoize-visible-atom
  "Return a memoized version of function f, with its cache (an atom)
accessible as metadata with the key :memoize-atom."
  [f]
  (let [mem (atom {})]
    (with-meta
      (fn [& args]
	(if-let [e (find @mem args)]
	  (val e)
	  (let [ret (apply f args)]
	    (swap! mem assoc args ret)
	    ret)))
      {:memoize-atom mem})))

(defn cache-of
  "Return the cache (an atom) of the function f that was created by memoize-visible-atom."
  [f]
  (:memoize-atom (meta f)))

(defn reset-cache! 
  "Reset the cache (an atom) of the function f that was created by memoize-visible-atom."
  [f]
  (reset! (cache-of f) {}))

;;;; Database
(defn make-row
  "Represents 1 row from a table in the source database.
   table   the table name (a string)
   values  maps columns (as keywords) to values retrieved
   scrubs  maps columns to scrubbed values"
  [table values scrubs]
  {:table table :values values :scrubs scrubs})

(defn fetch
  "Return a sequence of Row objects from table, restricted by where-clause."
  [table where-clause]
  {:pre [*db*]}
  (let [q (str "SELECT * FROM " table " WHERE " where-clause)]
    (when *debug-sql*
      (println q))
    (when-not (contains? *ignore-tables* table)
      (try
	(time-limited 10000
		      (sql/with-connection *db*
			(sql/with-query-results rs
			  [q]
			  (reduce conj '() (map #(make-row table % nil) rs)))))
	(catch java.util.concurrent.TimeoutException e
	  (if *debug-sql*
	    (println (str "Ignoring: " e " :: while executing \"" q "\""))
	    (throw (Exception. (str e " :: while executing \"" q "\"")))))
	(catch org.postgresql.util.PSQLException e
	  (throw (Exception. (str e " :: while executing \"" q "\""))))))))

(defn -row-of
  "Return the Row of table where primary-key equals value.  Un-memoized version.  Use row-of instead."
  [table primary-key value]
  {:pre [*db* table primary-key value]}
  (first (fetch table (str (name primary-key) " = " value))))
(def row-of (memoize-visible-atom -row-of))

;; Really just a wrapper for fetch so we can memoize it.  FIXME!!! I'm not sure I shouldn't just memoize fetch.
(defn -rows-where
  "Return the set of Rows of table matching the where clause.  Un-memoized version.  Use rows-where instead."
  [table where-clause]
  {:pre [*db* table where-clause]}
  (fetch table where-clause))
(def rows-where (memoize-visible-atom -rows-where))

;;; Random database rows
(defn row-exists?
  "Return true if there is a row of table with the column = value."
  [table column value]
  (let [q (str "SELECT COUNT(*) FROM " table " WHERE " (name column) " = " value)]
    (when *debug-sql*
      (println q))
    (try
      (time-limited 10000
		    (= 1 (:count (sql/with-connection *db*
				   (sql/with-query-results rs
				     [q]
				     (into {} rs))))))
      (catch java.util.concurrent.TimeoutException e
	(if *debug-sql*
	    (println (str "Ignoring: " e " :: while executing \"" q "\""))
	    (throw (Exception. (str e " :: while executing \"" q "\"")))))
      (catch org.postgresql.util.PSQLException e
	(throw (Exception. (str e " :: while executing \"" q "\"")))))))

(defn -high-pk
  "Return the highest primary key value in table with primary key column pk."
  [table pk]
  {:pre [*db*]}
  ((keyword pk) (:values (first (fetch table (str "1=1 ORDER BY " (name pk) " DESC LIMIT 1"))))))
(def high-pk (memoize-visible-atom -high-pk))

(defn rand-between
  "Return a random number between lo (exclusive) and hi (inclusive)."
  [lo hi]
  (math/round (+ lo (rand (- (+ hi 1) lo)))))

(defn precis
  "Return a vector of the table name, primary-key column, and value of that primary key column for the given row.
   E.g. [\"my_table\" :id 1234]"
  [row]
  {:pre [*primary-keys* (:table row)]}
  (let [pk (or (*primary-keys* (:table row))
	       :id)]
    [(:table row), pk, (pk (:values row))]))

(defn random-row-of
  "Return a randomly-selected row of table or nil."
  [table]
  {:pre [*db* *primary-keys* (*primary-keys* table)]}
  (let [pk (name (*primary-keys* table))
	hi (high-pk table pk)]
    (loop [lo 0
	   tries 10]
      (let [val (rand-between lo hi)]
	(if (row-exists? table pk val)
	  (row-of table pk val)
	  (when (> tries 0)
	      (recur val (dec tries))))))))

(defn random-column-of
  "Return a randomly-selected column value of table/column.
   Table's a string, column's a keyword."
  [table column]
  {:pre [*db*]}
  (when-let [row (random-row-of table)]
    ((keyword column) (:values row))))

;;;; Table dependencies
(defn points-to-table
  "Return the dependencies that refer to table."
  [table]
  {:pre [*dependencies*]}
  (filter #(= (:rt %) table) *dependencies*))

(defn table-points-to
  "Return the dependencies for the given table."
  [table]
  {:pre [*dependencies*]}
  (filter #(= table (:dt %))
	  *dependencies*))

;;;; Scrubbing
(defn random-fragment-by-regex-grouping
  "Append the offset'th regex-match from a random row of table/column to acc and return that.
   E.g. (r-f-b-r-g \"my_fans\" :email #\"(.*)(@)(.*)\" \"prez@\" 2)
        ==> \"prez@whitehouse.gov\"
   Bugs: Fails too silently, appending nil to acc."
  [table column regex acc offset]
  {:pre [*db*]}
  (let [row (random-row-of table)
	val (when row (column (:values row)))
	ary (when val (re-matches regex val))
	part (when ary (ary (inc offset)))]
    (str acc part)))

(defn scrub-regex
  "Return a map of column to scrubbed replacement value for val by mixing and matching random
   fragments of the value (as indicated by groupings in the regex) from various rows in the given
   table/column.
   E.g. (scrub-regex \"my_fans\" :email #\"(.*)(@)(.*)\" \"me@example.com\")
        ==> \"prez@whitehouse.gov\""
  [table column regex val]
  {:pre [*db*]}
  (into {}
	(when val
	  (when-let [matches (re-matches regex val)]
	    (loop [matches (rest matches)
		   offset 0
		   result ""]
	      (if (seq matches)
		(recur (rest matches) (inc offset) (random-fragment-by-regex-grouping table column regex result offset))
		{column result}))))))

(defn scrub-strategically
  "Return a vector of scrub maps for row using the given strategy."
  [row strategy]
  {:pre [*db*]}
  (vector 
   (into {}
	 (map (fn [column]
		(cond
		 (instance? java.util.regex.Pattern (strategy column)) (scrub-regex (:table row)
										    column
										    (strategy column)
										    (column (:values row)))
		 (= (strategy column) :random)                         {column (random-column-of (:table row) column)}
		 :otherwise-it-is-static                               {column (strategy column)}))
	      (keys strategy)))))

(defn scrub
  "Return row, scrubbed according to its table's *scrub-strategy*.
   Each element of the row's :values is set to nil, and its :scrubs vector
   is filled in with maps of column names (as keywords) to replacement values.
   Bugs: There's only one scrub in the vector of scrubs!"
  [row]
  {:pre [*db* *scrub-strategy*]}
  (if-let [strategy (*scrub-strategy* (:table row))]
    (make-row (:table row)
	      (apply dissoc (into {} (:values row)) (keys strategy))
	      (scrub-strategically row strategy))
    row))

;;;; Source data collection
;;;;
(defn stash!
  "Store a collection in an atom containing a collection."
  [place stuff]
  {:pre [(instance? clojure.lang.Atom place)]}
  (swap! place into stuff))

(defn make-dot-node
  [row]
  (let [[t _ k] (precis row)
	name (str t ":" k)
	label (str t "\\n" k)]
    (dot/node name :shape :rectangle :label label)))

(defn make-dot-edge
  [r1 r2 style]
  (let [nm (fn [r]
	     (let [[t _ k] (precis r)]
	       (str t ":" k)))]
    (dot/edge (nm r1) (nm r2) :style style)))

(defn parents-of
    "Return the parents of row."
    [row]
    {:pre [*db* *dependencies* (instance? clojure.lang.PersistentHashSet *ignore-tables*)]
     :post [(every? identity %)]}
    (when *debug-everything* (println "parents-of:" (precis row)))
    (let [key                #((keyword (:dc %)) (:values row))
	  parent-deps        (table-points-to (:table row))
	  usable-parent-deps (filter (fn [dep] (not (*ignore-tables* (:rt dep))))
				     parent-deps) ] 
      (map (fn row-of-or-fail [dep]
	     (let [row (row-of (:rt dep) (:rc dep) (key dep))]
	       (when-not row (throw (Exception. (str "parents-of: (row-of " (:rt dep) " " (:rc dep) " " (key dep) ") is nil"))))
	       row))
	   (filter #(key %)
		   usable-parent-deps))))

(defn children-of
  "Return the children of row."
  [row]
  {:pre [*db* *dependencies*] :post [(every? identity %)]}
  (when *debug-everything* (println "children-of:" (precis row)))
  (let [key #((keyword (:rc %)) (:values row))
	result (reduce into #{}
		       (map
			(fn [dep]
			  (if (key dep)
			    (rows-where (:dt dep) (str (name (:dc dep)) " = " (key dep)))
			    (println "children-of: dep: " dep " (key dep): is nil")))
			    (points-to-table (:table row))))]
    (when *dot*
      (stash! *dot* (make-dot-node row))
      (doseq [x result]
	(stash! *dot* (make-dot-node x))
	(stash! *dot* (make-dot-edge x row :solid))))
    result))

(defn filter-visited
  "Return the set of rows that aren't in the set in the atom visited."
  [rows visited]
  {:pre [(instance? clojure.lang.IDeref visited)]}
  (set (filter #((complement (partial contains? @visited)) %) rows)))

(defn ancestors-of
  "Return the set of ancestors of row that haven't already been visited."
  [visited row]
  {:pre [*db* *dependencies* (instance? clojure.lang.Atom visited) (instance? clojure.lang.PersistentHashSet @visited)]}
  (when *debug-everything* (println "ancestors-of:" (precis row)))
  (let [ps (filter-visited (parents-of row) visited)]
    (stash! visited ps)
    (when *dot*
      (doseq [x ps]
	(stash! *dot* (make-dot-node x))
	(stash! *dot* (make-dot-edge row x :dotted))))
    (doall (map #(ancestors-of visited %) ps)))
  @visited)

(defn root-ancestors-of
  "Return the set of only the root ancestors of row."
  [row]
  {:pre [*db* *dependencies*]}
  (letfn [(doit
	   [row]
	   (if-let [parents (seq (parents-of row))]
	     (map doit parents)
	     row))]
    (set (flatten (doit row)))))

(defn descendents-of
  "Return the set containing row, all the descendents of row, and all the ancestors of each descendent of row."
  [row]
  {:pre [*db* *dependencies*]}
  (when *debug-everything* (println "descendents-of:" (precis row)))
  (letfn [(desc [row] (let [children (children-of row)]
			  (reduce into (set children)
				  (map desc children))))]
      (let [visited (atom (desc row))]
	(stash! visited #{row})
	(doseq [row @visited] (ancestors-of visited row))
	@visited)))


;;;; Use tsort to order the extracted rows 
(defn precis-to-string
  "Convert a precis of a row to a brief string representation, e.g. \"private.people:id:67057214\"."
  [[table column key]]
  (str table ":" (name column) ":" key))

(defn row-to-string
  "Convert a row to a brief string representation, e.g. \"private.people:id:67057214\"."
  [row]
  (precis-to-string (precis row)))

(defn row-parent-pairs
  "Pair row with each parent of row.  If row has no parents, pair with itself.  That's tsort's way of including leaf rows."
  [row]
  (if-let [ps (seq (parents-of row))]
    (map #(vector row %) ps)
    [row row])) ;; tsort allows a special case for rows with no parents -- just add an edge to self

(defn tsort
  [rows]
  (let [dict   (into {} (map #(vector (row-to-string %) %) rows)) ;; look up a row from it's printable string representation
	pairs  (partition 2 (map row-to-string (flatten (map row-parent-pairs rows)))) ;; e.g. (("foo:id:1" "bar:id:2") ("baz:id:3" "xxx:id:5"))
	lines  (apply str (map (fn [[x y]] (str x " " y "\n")) pairs)) ;; one string, ready to feed to tsort
	result (shell-out/sh "tsort" :return-map true :in lines)] ;; a map of :exit, :out, and :err
    (if (zero? (:exit result))
      (map dict (split #"\n" (:out result))) ;; convert tsort's output back into rows
      (throw (Exception. "tsort failed: " (:err result))))))

(defn insert-order
  "Return rows in database insertion order."
  [rows]
  (reverse (tsort rows)))

(defn insert
  [target rows]
  (sql/with-connection target
    (sql/transaction
     (doseq [row rows]
       (when *debug-everything* (println "insert" (precis row)))
       (sql/insert-records (:table row) (merge (:values row) (first (:scrubs row))))))))

(defn transfer
    "Extract the rows of the object graph for the specified table, primary-key, and value,
   scrub and insert into the target db-spec."
    [target table primary-key value]
    (insert target (insert-order (map scrub (descendents-of (row-of table primary-key value)))))) 
