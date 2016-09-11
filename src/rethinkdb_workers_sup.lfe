(defmodule rethinkdb_workers_sup
  (behaviour supervisor)
  (export (start_link 0))
  (export (init 1)))

;;; ==================================================================== [ API ]

(defun start_link () (supervisor:start_link (MODULE) '()))

(defun init
  (['()]
   (let ((procs '(#(rethinkdb_worker #(rethinkdb_worker start_link [])
                              transient 5000 worker [rethinkdb_worker]))))
     `#(ok #(#(simple_one_for_one 1000 10) ,procs)))))

;;; ==================================================================== [ EOF ]
