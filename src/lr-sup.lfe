(defmodule lr-sup
  (behaviour supervisor)
  (export (start_link 0))
  (export (init 1)))

(defun start_link () (supervisor:start_link `#(local ,(MODULE)) (MODULE) '()))

(defun init
  (['()]
   (let* ((opts '(ordered_set public named_table #(read_concurrency true)))
          ('lr-server (ets:new 'lr-server opts))
          (procs '(#(lr-server #(lr-server start_link [])
                               permanent 5000 worker [lr-server]))))
     `#(ok #(#(one_for_one 10 10) ,procs)))))
