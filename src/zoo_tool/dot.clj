(ns
    #^{:author "Craig Ludington",
       :doc "Library for emitting DOT."}
  zoo-tool.dot)

(defn- q-name
  "Return the name of x in double quotes."
  [x]
  (str "\"" (name x) "\""))

(defn- attributes
  "Given pairs of attribute names/values,
   return a string suitable for a DOT attributes clause.
   Feel free to use keywords or strings,
   e.g. (attributes :style :bold :label \"100 times\")"
  [& xs]
  {:pre [xs (even? (count xs))]}
  (apply str
	 (interpose ", "
		    (map (fn [[a b]] (str (q-name a) " = " (q-name b)))
			 (partition 2 xs)))))

(defn- pr-attributes
  "Wrap attrs (if any) in brackets for use with a node or edge clause."
  [attrs]
  (when attrs (str " [" (apply attributes attrs) "]")))

;; Public
(defn digraph
  [n & xs]
  (str "digraph " (q-name n) " {\n" (apply str xs) "}\n"))

(defn graph-attributes
  [& xs]
  {:pre [xs (even? (count xs))]}
  (apply str (map (fn [[a b]] (str "\t" (attributes a b) ";\n"))
		  (partition 2 xs))))

(defn node
  [n & attrs]
  (str "\t" (q-name n) (pr-attributes attrs) ";\n"))

(defn edge
  [n1 n2 & attrs]
  (str "\t" (q-name n1) " -> " (q-name n2) (pr-attributes attrs) ";\n"))




(comment
  (digraph "zoo"
	   (node "private.people 1" :label "private.people\\n1" :shape :rectangle)
	   (node "cnu.customers 2" :label "cnu.customers\\n2" :shape :rectangle)
	   (edge "private.people 1" "cnu.customers 2" :style :dotted))
  ;; => 
  ;;    digraph "zoo" {
  ;;          "private.people 1" ["label" = "private.people\n1", "shape" = "rectangle"];
  ;;          "cnu.customers 2" ["label" = "cnu.customers\n2", "shape" = "rectangle"];
  ;;          "private.people 1" -> "cnu.customers 2" ["style" = "dotted"];
  ;;    }


  (digraph "G"
           (graph-attributes :max "400" :min "200")
           (node :top :size :large :color :blue)
           (node :bottom :label "meh who cares?")
           (edge :top :bottom :style :dotted))
  ;; =>
  ;;   digraph G {
  ;;    max="400";
  ;;    min="200";
  ;;    top [size="large",color="blue"];
  ;;    bottom [label="meh who cares?"];
  ;;    top -> bottom [style="dotted"];
  ;;   }
  )
