(defmodule lefink
  (export (start 0) (stop 0)
          (add-pool 2) (add-pool 3) (remove-pool 1)
          (db 2)
          (query 2)))

(defun start ()
  (application:start 'lefink)
  'ok)

(defun stop ()
  (application:stop 'lefink)
  'ok)

(defun add-pool
  ([ref num-workers] (when (> num-workers 0))
   (add-pool ref num-workers '())))

(defun add-pool
  ([ref num-workers opts] (when (> num-workers 0))
   (let* (('ok (lf-server:add-pool ref))
          (sup-opts `#(#(lf-workers-sup ,ref) #(lf-workers-sup start_link [])
                       permanent 5000 supervisor [lf-workers-sup]))
          (`#(ok ,sup-pid) (supervisor:start_child 'lf-sup sup-opts)))
     (lists:foreach
      (lambda (_) (let ((`#(ok ,_) (supervisor:start_child sup-pid `(,ref ,opts))))))
      (lists:seq 1 num-workers))
     'ok)))

(defun remove-pool (ref)
  (case (supervisor:terminate_child 'lf-sup `#(lf-workers-sup ,ref))
    ('ok   (supervisor:delete_child 'lf-sup `#(lf-workers-sup ,ref)))
    (error error)))

(defun db
  ([ref name] (when (is_binary name))
   (lists:foreach (lambda (pid) (lf-worker:db pid name))
                  (lf-server:get-all-workers ref))))

(defun query (ref raw-query)
  (let ((term (relang_ast:build_query raw-query))
        (pid  (lf-server:get-worker ref)))
    (lf-worker:query pid term)))
