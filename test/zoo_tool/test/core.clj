(ns zoo-tool.test.core
  (:use [zoo-tool.core] :reload-all
	[zoo-tool.schema :only (setup)])
  (:use [clojure.test]))

(defn fail-msg
  "Helpful string describes test failures.
   f - function name
   r - the row that is the key to the map of expected results
   m - the map of expected results
   g - the actual results"
  [f r m g]
  (str r " "
	 "\n    expected " (m r)
	 "\n    got      " g
	 "\n    missing  " (clojure.set/difference (m r) g)
	 "\n    extra    " (clojure.set/difference g (m r))
	 "\n"))

(def test-children
     {
      ["a"  :id  1]  #{ ["ba" :id  10] ["bb" :id  30] ["bc" :id 40]  ["bc" :id 50] }
      ["ba" :id 10]  #{ ["ca" :id 100] }
      ["bb" :id 30]  #{ ["ca" :id 100] ["z"  :id 200] }
      ["bc" :id 40]  #{}
      ["bc" :id 50]  #{}

      ["a"  :id  2]  #{ ["ba" :id  20] ["bc" :id  40] }

      ["x"  :id  3]  #{ ["ya" :id  60] ["ya" :id  70] ["yb" :id 80]}
      ["ya" :id 60]  #{ ["z"  :id 200] }
      ["ya" :id 70]  #{}
      ["yb" :id 80]  #{} } )

(defn test-children-of
  []
  (doall
   (map (fn [r]
	  (let [row (apply row-of r)
		children (set (map precis (children-of row)))]
	    (is (= children (test-children r))
		(fail-msg "test-children-of" r test-children children))))
	(keys test-children))))

(def test-parents
     {
      ["ba" :id  10]  #{["a"  :id  1] }
      ["ba" :id  20]  #{["a"  :id  2] }
      ["bb" :id  30]  #{["a"  :id  1] }
      ["bc" :id  40]  #{["a"  :id  1] ["a" :id 2] }
      ["bc" :id  50]  #{["a"  :id  1] }
      ["ca" :id 100]  #{["ba" :id 10] ["bb" :id 30] }
      ["ya" :id  60]  #{["x"  :id  3] }
      ["ya" :id  70]  #{["x"  :id  3] }
      ["yb" :id  80]  #{["x"  :id  3] }
      ["z"  :id 200]  #{["bb" :id 30] ["ya" :id 60] } } )

(defn test-parents-of
  []
  (doall
   (map (fn [r]
	  (let [row (apply row-of r)
		parents (set (map precis (parents-of row)))]
	    (is (= parents (test-parents r))
	      (fail-msg "test-parents-of" r test-parents parents))))
	(keys test-parents))))

(def test-root-ancestors
     {
      ["z"  :id 200] #{["a" :id 1] ["x" :id 3]}
      ["ya" :id  60] #{["x" :id 3]}
      ["ba" :id  20] #{["a" :id 2]}
      ["a"  :id   1] #{} } )

(defn test-root-ancestors-of
  []
  (doall
   (map (fn [r]
	  (let [row (apply row-of r)
		got (set (map precis (root-ancestors-of row)))]
	    (is (= got (test-root-ancestors r))
	      (fail-msg "test-root-ancestors-of" r test-root-ancestors got))))
	(keys test-root-ancestors))))

(def test-ancestors
     {
      ["bc" :id  50] #{["a"  :id  1]}
      ["ca" :id 100] #{["ba" :id 10]
		       ["bb" :id 30]
		       ["a"  :id  1]}
      ["z"  :id 200] #{["bb" :id 30]
		       ["a"  :id  1]
		       ["ya" :id 60]
		       ["x"  :id  3]}})

(defn test-ancestors-of
  []
  (doall
   (map (fn [r]
	  (let [row (apply row-of r)
		ancestors (set (map precis (ancestors-of (atom #{}) row)))]
	    (is (= ancestors (test-ancestors r))
	      (fail-msg "test-ancestors-of" r test-ancestors ancestors))))
	(keys test-ancestors))))

(def test-descendents
     {
      ;; A leaf with one parent and no grandparents
      ["bc" :id  50] #{["bc" :id  50]     ;; self
		       ["a"  :id   1]}    ;; only parent of bc50

      ;; Starting at an intermediate node, we get its parent, because its child has an alternate path to its parent.
      ["ba" :id  10] #{["ba" :id  10]     ;; self
		       ["ca" :id 100]     ;; child of ba10
		       ["bb" :id  30]     ;; parent of ca100
		       ["a"  :id   1]}    ;; parent of bb30

      ;; Starting at a root node with two children, we pick up another root node from the child with two parents.
      ["a"  :id   2] #{["a"  :id   2]     ;; self
		       ["ba" :id  20]     ;; child of a2
		       ["bc" :id  40]     ;; child of a2 and a1
		       ["a"  :id   1]}    ;; parent of bc40

      ;; This test is almost like the previous one, but the path to an alternate root node starts at the bottom tier.
      ["x"  :id   3] #{["x"  :id   3]     ;; self
		       ["yb" :id  80]     ;; child of x3
		       ["ya" :id  70]     ;; child of x3
		       ["ya" :id  60]     ;; child of x3
		       ["z"  :id 200]     ;; child of ya60
		       ["bb" :id  30]     ;; parent of z200
		       ["a"  :id   1]}})  ;; parent of bb30

(defn test-descendents-of
  []
  (doall
   (map (fn [r]
	  (let [row (apply row-of r)
		descendents (set (map precis (descendents-of row)))]
	    (is (= descendents (test-descendents r))
	      (fail-msg "test-descendents-of" r test-descendents descendents))))
	(keys test-descendents))))

(defn run-all
  [key]
  (with-config key
    (setup *db*)
    (test-children-of)
    (test-parents-of)
    (test-root-ancestors-of)
    (test-ancestors-of)
    (test-descendents-of)))

(deftest x
  ()
  (run-all :test))
