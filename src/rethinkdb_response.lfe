(defmodule rethinkdb_response
  (export (handle 3) (get-type 1) (get-response 1)))

;;; ==================================================================== [ API ]

(defun handle (socket token packet)
  (let ((response (jsx:decode packet)))
    (do-handle socket token (get-type response) (get-response response))))

(defun get-type (response)
  (case (proplists:get_value #"t" response)
    ('undefined 'undefined)
    (value      (val->symbol value))))

(defun get-response (response) (proplists:get_value #"r" response))

;;; ===================================================== [ Internal functions ]

(defun do-handle
  ([_socket _token t r] (when (orelse (=:= 'SUCCESS_ATOM     t)
                                      (=:= 'SUCCESS_SEQUENCE t)))
   `#(ok ,r))
  ([socket token 'SUCCESS_PARTIAL r]
   (let* ((recv (spawn 'rethinkdb_net 'stream-recv `(,socket ,token)))
          (pid  (spawn 'rethinkdb_net 'stream-poll `(#(,socket ,token) ,recv))))
     `#(ok #(pid ,pid) ,r)))
  ([_socket _token t r]
   (error (if (known-error? t) t 'unknown-response) (list r))))

(defun val->symbol (val)
  (try
    (ql2:enum_symbol_by_value 'Response.ResponseType val)
    (catch (_ 'undefined))))

(defun known-error?
  (['CLIENT_ERROR]  'true)
  (['COMPILE_ERROR] 'true)
  (['RUNTIME_ERROR] 'true)
  ([_type]          'false))

;;; ==================================================================== [ EOF ]
