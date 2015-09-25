(defmodule lf-data
  (export (len 1) (le-32-int 1)))

(defun len (data) `#(,(le-32-int (iolist_size data)) ,data))

;; number => little-endian 32-bit integer
(defun le-32-int (x) (binary (x (size 32) little)))
