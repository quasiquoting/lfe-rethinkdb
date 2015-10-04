(defmodule lr-app
  (behaviour application)
  (export (start 2) (stop 1)))

(defun start (_type _args) (lr-sup:start_link))

(defun stop (_state) 'ok)
