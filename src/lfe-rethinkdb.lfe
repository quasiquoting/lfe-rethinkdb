(defmodule lfe-rethinkdb
  (export (start 0) (stop 0)
          (add-pool 2) (add-pool 3) (remove-pool 1)
          (use 2)
          (query 2)))

(defun start ()
  (application:start 'lfe_rethinkdb)
  'ok)

(defun stop ()
  (application:stop 'lfe_rethinkdb)
  'ok)

(defun add-pool
  ([ref num-workers] (when (> num-workers 0))
   (add-pool ref num-workers '())))

(defun add-pool
  ([ref num-workers opts] (when (> num-workers 0))
   (let* (('ok (lr-server:add-pool ref))
          (sup-opts `#(#(lr-workers-sup ,ref) #(lr-workers-sup start_link [])
                       permanent 5000 supervisor [lr-workers-sup]))
          (`#(ok ,sup-pid) (supervisor:start_child 'lr-sup sup-opts)))
     (lists:foreach
      (lambda (_) (let ((`#(ok ,_) (supervisor:start_child sup-pid `(,ref ,opts))))))
      (lists:seq 1 num-workers))
     'ok)))

(defun remove-pool (ref)
  (case (supervisor:terminate_child 'lr-sup `#(lr-workers-sup ,ref))
    ('ok   (supervisor:delete_child 'lr-sup `#(lr-workers-sup ,ref)))
    (error error)))

(defun use
  ([ref name] (when (is_binary name))
   (lists:foreach (lambda (pid) (lr-worker:use pid name))
                  (lr-server:get-all-workers ref))))

(defun query (ref raw-query)
  (let ((term (lr-query:build-query raw-query))
        (pid  (lr-server:get-worker ref)))
    (lr-worker:query pid term)))
