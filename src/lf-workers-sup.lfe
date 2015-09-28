(defmodule lf-workers-sup
  (behaviour supervisor)
  (export (start_link 0))
  (export (init 1)))

(defun start_link () (supervisor:start_link (MODULE) '()))

(defun init
  (['()]
   (let ((procs '(#(lf-worker #(lf-worker start_link [])
                              transient 5000 worker [lf-worker]))))
     `#(ok #(#(simple_one_for_one 1000 10) ,procs)))))
