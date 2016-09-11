(defmodule rethinkdb_sup
  (behaviour supervisor)
  (export (start_link 0))
  (export (init 1)))

(defun start_link () (supervisor:start_link `#(local ,(MODULE)) (MODULE) '()))

(defun init
  (['()]
   (let* ((opts '(ordered_set public named_table #(read_concurrency true)))
          ('rethinkdb_server (ets:new 'rethinkdb_server opts))
          (procs '(#(rethinkdb_server
                     #(rethinkdb_server start_link [])
                     permanent 5000 worker [rethinkdb_server]))))
     `#(ok #(#(one_for_one 10 10) ,procs)))))
