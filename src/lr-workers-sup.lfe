(defmodule lr-workers-sup
  (behaviour supervisor)
  (export (start_link 0))
  (export (init 1)))

(defun start_link () (supervisor:start_link (MODULE) '()))

(defun init
  (['()]
   (let ((procs '(#(lr-worker #(lr-worker start_link [])
                              transient 5000 worker [lr-worker]))))
     `#(ok #(#(simple_one_for_one 1000 10) ,procs)))))
