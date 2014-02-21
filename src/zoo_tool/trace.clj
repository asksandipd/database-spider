(ns zoo-tool.trace
  #^{:author "Craig Ludington"
     :doc "Customer Zoo Tool.  Debugging aid.  Custom implementation of trace function for database rows (using precis to format the rows)."}
  (:use [zoo-tool.core] :reload-all))

(def *trace-depth* 0)
(defn trace-indent
  "Returns an indentation string based on *trace-depth*"
  []
  (apply str (take *trace-depth* (repeat "|   "))))

(defn tracer
  "Return a trace function that works on functions whose arguments and returns are unknown to us."
  [function-name]
  (fn [f & args]
    (let [id (gensym (str function-name " "))]
      (print (trace-indent) id " ==> " args "\n")
      (let [value (binding [*trace-depth* (inc *trace-depth*)]
		    (apply f args))]
	(print (trace-indent) id " <== " value "\n")
	value))))

(defn pp-row-or-rows
  "Print, using precis, a row or a sequence of rows."
  [x]
  (print (if (:table x) (precis x) (map precis x))))

(defn trace-rows
  "Return a trace function that works on functions that accept and return some combination of a row or rows."
  [function-name]
  (fn [f & args]
    (let [id (gensym (str function-name " "))]
      (print (trace-indent) id " ==> ")
      (doseq [x args]
	(pp-row-or-rows x))
      (println)
      (let [value (binding [*trace-depth* (inc *trace-depth*)]
		    (apply f args))]
	(print (trace-indent) id " <== ")
	(pp-row-or-rows value)
	(println)
	value))))

(defn trace-inputs
  "Return a trace function that traces the inputs only"
  [function-name]
  (fn [f & args]
    (let [id (gensym (str function-name " "))]
      (print (trace-indent) id " ==> ")
      (doseq [x args]
	(pp-row-or-rows x))
      (println)
      (let [value (binding [*trace-depth* (inc *trace-depth*)]
		    (apply f args))]
	value))))

(defn trace-input-lengths
  "Return a trace function that traces the inputs as repeated dots so you can see the length of its argument."
  [function-name]
  (fn [f & args]
    (doseq [x args]
      (print (if (:table x) "" (apply str (repeat (count x) ".")))))
    (println)
    (apply f args)))

(defn hooked?
  "Is the target-var (e.g. #'my-func) in a robert.hooke chain?"
  [target-var]
  {:pre [(instance? clojure.lang.Var target-var)]}
  (:robert.hooke/original (meta @target-var)))

(defn unhook
  "Unhook target-var (e.g. #'my-func) from the robert.hooke chain completely and unconditionally."
  [target-var]
  {:pre [(hooked? target-var)]}
  (alter-var-root target-var (constantly (:robert.hooke/original (meta @target-var)))))

;; (unhook #'parents-of)
;; (add-hook #'parents-of   (trace-rows "parents-of"))

;; (unhook #'ancestors-of)
;; (add-hook #'ancestors-of (trace-rows "ancestors-of"))
;; (add-hook #'ancestors-of (trace-inputs "ancestors-of"))
;; (add-hook #'ancestors-of (trace-input-lengths "ancestors-of"))
