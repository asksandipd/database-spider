(ns
    #^{:author "Craig Ludington",
       :doc "Graph utilities.  Wrappers and convenience functions for working with clojure.contrib.graph."}
  zoo-tool.graph
  (require [clojure.contrib.graph :as graph]))

(defn nodes
  "Return a hash set of the database tables in the customer object graph.
   Each element of the set is a string of the form \"schema.table\".

   Use this as the nodes member of a graph/directed-graph struct."
  [dependencies]
  (set (reduce conj (map :dt dependencies) (map :rt dependencies))))

(defn neighbors
  "Return a map of dependent tables to referred tables.
   Keys are strings of the form \"schema.table\".
   Values are hash sets containing strings of the form \"schema.table\".
   E.g. {\"x.a\" #{\"x.y\" \"x.z\"}}

   Use the result as the neighbors member of a graph/directed-graph struct."
  ([dependencies]
     (neighbors (map #(vector (:dt %) (:rt %)) dependencies)
		{}))
  ([pairs acc]
     (letfn [(insert-pair
	      [pair map]
	      (let [[k v] pair
		    place (conj (or (map k) #{}) v)]
		(conj map {k place})))]
       (if (empty? pairs)
	 acc
	 (recur (rest pairs) (insert-pair (first pairs) acc))))))

(defn post-ordered-nodes
  "Return a sequence of indexes of a post-ordered walk of the graph.
   Precondition: *dependencies* is bound."
  [dependencies]
  (graph/post-ordered-nodes (struct graph/directed-graph (nodes dependencies) (neighbors dependencies))))

(defn insertion-order
  "Return a sequence of table names in the order they must be inserted into the database to avoid foreign key constraint violations.
   Precondition: *dependencies* is bound."
  [dependencies]
  (reverse (post-ordered-nodes dependencies)))

(defn dependency-list
   "Similar to a topological sort, this returns a vector of sets. The
   set of nodes at index 0 are independent.  The set at index 1 depend
   on index 0; those at 2 depend on 0 and 1, and so on.  Those within
   a set have no mutual dependencies.  Assume the input graph (which
   much be acyclic) has an edge a->b when a depends on b."
  [dependencies]
  (graph/dependency-list (graph/remove-loops (struct graph/directed-graph (nodes dependencies) (neighbors dependencies)))))

(defn lazy-walk
  "Return a lazy sequence of the nodes of a graph starting a node n."
  [dependencies table]
  (graph/lazy-walk (graph/reverse-graph (struct graph/directed-graph (nodes dependencies) (neighbors dependencies))) table))
