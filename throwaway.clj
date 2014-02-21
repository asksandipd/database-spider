(in-ns 'zoo-tool.core)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Throwaway code
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn check-database
  []
  (let [failures (atom {})]
    (doseq [{table :dt column :dc} *dependencies*]
      (try
	(fetch table (str "1=1 ORDER BY " column " DESC LIMIT 1"))
	(println table column)
	(catch Exception e
	  (println e)
	  (swap! failures conj {[table column] e}))))
    @failures))

(def *failures* (check-database))
(defn perms-error [k] (re-find #"ERROR: permission denied for relation" (str (*failures* k))))
(defn timeout-error [k] (re-find #"java.util.concurrent.TimeoutException" (str (*failures* k))))

(def *db-permissions* (filter perms-error (keys *failures*)))
(def *db-timeouts* (filter timeout-error (keys *failures*)))


(comment
  ;; Try using tsort!
  (defn insert-order
    "Return rows in database insertion order."
    [rows]
    {pre [*dependencies*]}
    ;; dictionary of table-name => ordinal number
    ;; the file /tmp/deps was created by (doseq [dep *dependencies*] (println (:dt dep) (:rt dep)))
    (let [tier-of (zipmap (reverse (clojure.contrib.string/split #"\n"
								 (:out  (clojure.contrib.shell/sh "tsort" "/tmp/deps" :return-map true))))
			  (range 0 10000)) ]
      (sort (comparator #(< (tier-of (:table %1))
			    (tier-of (:table %2))))
	    rows)))
  )

;; (filter (fn bad-dep? [{:keys [dt dc]}] (some #{[dt dc]} *db-permissions*)) *dependencies*)


;; Do I care about any of these timeouts?
;;
;; (-> *db-timeouts* sort pprint)
;; (["cnu.addresses" "standardized_address_id"]
;;  ["cnu.approvals" "callcredit_report_id"]
;;  ["cnu.approvals" "callicp_report_id"]
;;  ["cnu.approvals" "callml_report_id"]
;;  ["cnu.approvals" "clarity_report_id"]
;;  ["cnu.approvals" "datax_report_id"]
;;  ["cnu.approvals" "eidcompare_report_id"]
;;  ["cnu.approvals" "long_term_result_id"]
;;  ["cnu.approvals" "oneloan_report_id"]
;;  ["cnu.approvals" "veda_vericheck_aml_report_id"]
;;  ["cnu.approvals" "veritec_report_id"]
;;  ["cnu.loan_tasks_committed" "loan_task_code_id"]
;;  ["cnu.loans" "pay_stub_id"]
;;  ["mef.results" "existing_approval_id"]
;;  ["payment_instruments.default_instrument_history" "created_by_employee_id"])
