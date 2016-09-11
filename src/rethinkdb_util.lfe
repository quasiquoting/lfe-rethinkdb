(defmodule rethinkdb_util
  (export (datum-value 1) (global-db 1)))

(include-file "include/ql2.hrl")

(defun datum-value
  ([(match-Datum type 'R_NULL)]                 'null)
  ([(match-Datum type 'R_BOOL   r_bool   bool)] bool)
  ([(match-Datum type 'R_NUM    r_num    num)]  num)
  ([(match-Datum type 'R_STR    r_str    str)]  (list_to_binary str))
  ([(match-Datum type 'R_ARRAY  r_array  arr)]  (lists:map #'datum-value/1 arr))
  ([(match-Datum type 'R_OBJECT r_object objects)]
   `#(,(lists:map #'datum-assocpair-tuple/1 objects))))

(defun datum-assocpair-tuple (obj)
  `#(,(list_to_binary (Datum.AssocPair-key obj))
     ,(datum-value (Datum.AssocPair-val obj))))

(defun global-db (value)
  (let ((val (make-Term type 'DB args `(,(rethinkdb_query:expr value)))))
    (make-Query.AssocPair key #"db" val val)))
