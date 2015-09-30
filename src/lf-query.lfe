(defmodule lf-query
  (export (query 1) (query 2) (query 3)
          (encode-query 1)))

(include-file "include/ql2.hrl")

(defun START () (ql2:enum_value_by_symbol 'Query.QueryType 'START))


;;; ============================================================================
;;; ===                            PUBLIC API                                ===
;;; ============================================================================

;; I definitely don't love this...
(defun query (raw-query) (query raw-query '[#()]))

(defun query
  ([raw-query opts] (when (is_list raw-query))
   (let ((query-ast (lf-ast:build-query raw-query)))
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

(defun encode-query
  ([(match-Query type 'START query term)]
   (ljson:encode `(,(START) ,(++ (encode-term term) '[#()])))))

(defun encode-term
  ([(match-Term type 'DATUM datum datum)] (encode-datum datum))
  ([(match-Term type type args terms)]
   `(,(ql2:enum_value_by_symbol 'Term.TermType type)
     ,(lists:map #'encode-term/1 terms)))
  ([_] []))

(defun encode-datum
  ([(match-Datum type 'R_NULL)]                                'null)
  ([(match-Datum type 'R_BOOL r_bool v)] (when (is_boolean v)) v)
  ([(match-Datum type 'R_NUM  r_num v)]  (when (is_number v))  v)
  ([(match-Datum type 'R_STR  r_str v)]  (when (is_binary v))  v))

;; (defun q (term) (make-Query type 'START query term token (generate-token)))


;;; ============================================================================
;;; ===                           PRIVATE API                                ===
;;; ============================================================================

(defun generate-token ()
  (binary ((lf-data:rand-int 3709551616) (size 64) little)))
