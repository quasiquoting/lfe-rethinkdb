(defmodule rethinkdb_data
  (export (len 1) (le-32-int 1)
          (rand-int 1)
          (rand-nth 1)))

(defun len (data) `#(,(le-32-int (iolist_size data)) ,data))

;; number => little-endian 32-bit integer
(defun le-32-int (x) (binary (x (size 32) little)))

(defun rand-int (n)
  (apply #'random:seed/3 (tuple_to_list (os:timestamp)))
  (random:uniform n))

(defun rand-nth (lst) (lists:nth (rand-int (length lst)) lst))
