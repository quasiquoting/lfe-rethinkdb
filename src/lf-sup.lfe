(defmodule lf-sup
  (behaviour supervisor)
  (export (start_link 0))
  (export (init 1)))

(defun start_link ()  (supervisor:start_link `#(local ,(MODULE)) (MODULE) '()))

(defun init (['()] '#(ok #(#(one_for_one 5 10) []))))
