(defmodule rethinkdb
  (behaviour application)
  (export (start 0) (start 2) (stop 0) (stop 1)
          (add-pool 2) (add-pool 3) (remove-pool 1)
          (use 2)
          (query 2)))

;;; ==================================================================== [ API ]

(defun start ()
  "Start the application."
  (application:start 'rethinkdb))

(defun start (_type _args) (rethinkdb_sup:start_link))

(defun stop ()
  "Stop the application."
  (application:stop 'rethinkdb))

(defun stop (_state) 'ok)

(defun add-pool
  ([ref num-workers] (when (> num-workers 0))
   (add-pool ref num-workers '())))

(defun add-pool
  ([ref num-workers opts] (when (> num-workers 0))
   (let* (('ok (rethinkdb_server:add-pool ref))
          (sup-opts `#(#(rethinkdb_workers-sup ,ref)
                       #(rethinkdb_workers-sup start_link [])
                       permanent 5000 supervisor [rethinkdb_workers-sup]))
          (`#(ok ,sup-pid) (supervisor:start_child 'rethinkdb_sup sup-opts)))
     (lists:foreach
       (lambda (_) (let ((`#(ok ,_) (supervisor:start_child sup-pid `(,ref ,opts))))))
       (lists:seq 1 num-workers))
     'ok)))

(defun remove-pool (ref)
  (case (supervisor:terminate_child 'rethinkdb_sup `#(rethinkdb_workers-sup ,ref))
    ('ok   (supervisor:delete_child 'rethinkdb_sup `#(rethinkdb_workers-sup ,ref)))
    (error error)))

(defun use
  ([ref name] (when (is_binary name))
   (lists:foreach (lambda (pid) (rethinkdb_worker:use pid name))
     (rethinkdb_server:get-all-workers ref))))

(defun query (ref raw-query)
  (let ((term (rethinkdb_query:build-query raw-query))
        (pid  (rethinkdb_server:get-worker ref)))
    (rethinkdb_worker:query pid term)))

;;; ==================================================================== [ EOF ]
