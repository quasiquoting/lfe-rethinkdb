(defmodule lf-query
  (export (query 1) (query 2) (query 3)))

(defun START () (ql2:enum_value_by_symbol 'Query.QueryType 'START))


;;; ============================================================================
;;; ===                            PUBLIC API                                ===
;;; ============================================================================

;; I definitely don't love this...
(defun query (raw-query) (query raw-query '[#()]))

(defun query
  ([raw-query opts] (when (is_list raw-query))
   (let ((query-ast (relang_ast:make raw-query)))
     (ljson:encode `(,(START) ,(++ query-ast opts)))))
  ([socket raw-query]
   ;; Build and run query when passing `socket'
   (query socket raw-query '[#()])))

(defun query (socket raw-query opts)
  (let* ((token              (generate-token))
         (`#(,length ,query) (lf-data:len (query raw-query opts))))
    (case (gen_tcp:send socket `(,token ,length ,query))
      ((= `#(error ,_reason) failure) failure)
      ('ok
       (case (lf-net:recv socket)
         ((= `#(error ,_reason) failure) failure)
         (`#(ok ,packet)
          (lf-response:handle socket token packet)))))))


;;; ============================================================================
;;; ===                           PRIVATE API                                ===
;;; ============================================================================

(defun generate-token ()
  (binary ((lf-data:rand-int 3709551616) (size 64) little)))
