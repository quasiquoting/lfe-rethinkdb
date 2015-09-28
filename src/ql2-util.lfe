(defmodule ql2-util
  (export (datum-value 1) (global-db 1)))

(include-file "include/ql2_pb.hrl")

(defun datum-value
  ([(match-datum type 'R_NULL)]                 'null)
  ([(match-datum type 'R_BOOL   r_bool   bool)] bool)
  ([(match-datum type 'R_NUM    r_num    num)]  num)
  ([(match-datum type 'R_STR    r_str    str)]  (list_to_binary str))
  ([(match-datum type 'R_ARRAY  r_array  arr)]  (lists:map #'datum-value/1 arr))
  ([(match-datum type 'R_OBJECT r_object objects)]
   `#(,(lists:map #'datum-assocpair-tuple/1 objects))))

(defun datum-assocpair-tuple (obj)
  `#(,(list_to_binary (datum_assocpair-key obj))
     ,(datum-value (datum_assocpair-val obj))))

(defun global-db (value)
  (let ((val (make-term type 'DB args (relang_ast:expr value))))
    (make-query_assocpair key #"db" val val)))
