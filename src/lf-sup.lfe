(defmodule lf-sup
  (behaviour supervisor)
  (export (start_link 0))
  (export (init 1)))

(defun start_link () (supervisor:start_link `#(local ,(MODULE)) (MODULE) '()))

(defun init
  (['()]
   (let* ((opts '(ordered_set public named_table #(read_concurrency true)))
          ('lf-server (ets:new 'lf-server opts))
          (procs '(#(lf-server #(lf-server start_link [])
                               permanent 5000 worker [lf-server]))))
     `#(ok #(#(one_for_one 10 10) ,procs)))))
