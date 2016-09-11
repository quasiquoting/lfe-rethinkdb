(defmodule rethinkdb_response
  (export (handle 3) (get-type 1) (get-response 1)))

;;; ==================================================================== [ API ]

(defun handle (socket token packet)
  (let* ((response (jsx:decode packet))
         (t (get-type response))
         (r (get-response response)))
    (cond
     ((success? t) `#(ok ,r))
     ((partial? t)
      ;; (io:format "Partial response <<< Get more data~n")
      (let* ((recv (spawn 'rethinkdb_net 'stream-recv `(,socket ,token)))
             (pid  (spawn 'rethinkdb_net 'stream-poll `(#(,socket ,token) ,recv))))
        ;; (io:format "Do something else here~n")
        `#(ok #(pid ,pid) ,r)))
     ((error? t)  `#(error ,r))
     ('true       '#(error unknown-response)))))

(defun get-type (response) (val->symbol (proplists:get_value #"t" response)))

(defun get-response (response) (proplists:get_value #"r" response))

;;; ===================================================== [ Internal functions ]

(defun val->symbol (val)
  (try
    (ql2:enum_symbol_by_value 'Response.ResponseType val)
    (catch (_ 'undefined))))

(defun error? (type)
  (lists:member type '(CLIENT_ERROR COMPILE_ERROR RUNTIME_ERROR)))

(defun partial?
  (['SUCCESS_PARTIAL] 'true)
  ([_]                'false))

(defun proplist?
  (['()]                                 'true)
  ([`(#(,_ ,_) . ,t)]                    (proplist? t))
  ([`(,h       . ,t)] (when (is_atom h)) (proplist? t))
  ([_]                                   'false))

(defun success?
  (['SUCCESS_ATOM]     'true)
  (['SUCCESS_SEQUENCE] 'true)
  ([_]                 'false))

;;; ==================================================================== [ EOF ]
