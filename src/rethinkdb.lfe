;;; ========================================================== [ rethinkdb.lfe ]

(defmodule rethinkdb
  "RethinkDB driver for the BEAM."
  (behaviour application)
  (behaviour supervisor)
  ;; Application callbacks
  (export (start 2) (stop 1))
  ;; Supervisor callbacks
  (export (init 1))
  ;; API
  (export (start 0) (stop 0))
  (export (use 2) (query 2)))

;;; ================================================== [ Application callbacks ]

(defun start (_type _args)
  (supervisor:start_link #(local rethinkdb_sup) (MODULE) []))

(defun stop (_state) 'ok)

;;; =================================================== [ Supervisor callbacks ]

(defun init
  (['()]
   (let* ((`#(ok ,pools) (application:get_env (MODULE) 'pools))
          (pool-specs    (lists:map #'pool-spec/1 pools))
          (sup-flags     (map 'strategy  'one_for_one
                              'intensity  10
                              'period     10)))
     `#(ok #(,sup-flags ,pool-specs)))))

;;; ==================================================================== [ API ]

(defun start ()
  "Start the application."
  (application:start (MODULE)))

(defun stop ()
  "Stop the application."
  (application:stop 'rethinkdb))

(defun use (pool-name db-name)
  (let* ((worker (poolboy:checkout pool-name 'false))
         (result (rethinkdb_worker:use worker db-name)))
    (poolboy:checkin pool-name worker)))

(defun query (pool-name raw-query)
  (let ((term (rethinkdb_query:build-query raw-query)))
    (poolboy:transaction pool-name (rethinkdb_worker:query term))))

;;; ===================================================== [ Internal functions ]

(defun pool-spec
  ([`#(,name ,size-args ,worker-args)]
   (let ((pool-args (list* `#(name #(local ,name))
                           #(worker_module rethinkdb_worker)
                           size-args)))
     (poolboy:child_spec name pool-args worker-args))))

;;; ==================================================================== [ EOF ]
