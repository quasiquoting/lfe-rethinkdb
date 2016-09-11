(defmodule rethinkdb_net
  (export (handshake 2)
          (recv 1)
          (stream-recv 2) (stream-poll 2))
  (import (from rethinkdb_data (le-32-int 1) (len 1))))

;;; http://rethinkdb.com/docs/writing-drivers/

(defun V0_4 () (le-32-int (ql2:enum_value_by_symbol 'VersionDummy.Version 'V0_4)))

(defun JSON () (le-32-int (ql2:enum_value_by_symbol 'VersionDummy.Protocol 'JSON)))

(defun STOP () (ql2:enum_value_by_symbol 'Query.QueryType 'STOP))

;;; ==================================================================== [ API ]

(defun handshake (sock auth-key)
  (let (('ok (gen_tcp:send sock (V0_4)))
        ('ok (gen_tcp:send sock (key-size auth-key)))
        ;; TODO: send auth-key iff auth-key =/= #""
        ('ok (gen_tcp:send sock (JSON))))
    (case (read-until-null sock)
      ('#(ok #"SUCCESS\x0;") 'ok)
      (error                   `#(error ,error)))))

(defun recv (socket)
  (case (gen_tcp:recv socket 8)
    ((= `#(error ,_reason) failure) failure)
    (`#(ok ,_token)
     (case (gen_tcp:recv socket 4)
       ((= `#(error ,_reason) failure) failure)
       (`#(ok ,length)
        (case (gen_tcp:recv socket (binary:decode_unsigned length 'little))
          ((= `#(error ,_reason) failure) failure)
          ((= `#(ok    ,_packet) success) success)))))))

(defun stream-recv (socket token)
  (receive (r (io:fwrite "Change feed received: ~p~n" `(,r))))
  (stream-recv socket token))

;; TODO: may want to loop on failure too...
(defun stream-poll
  ([`#(,socket ,token) pid]
   (let ((`#(,length ,query) (len '("[2]"))))
     ;; (io:format "Block socket <<< waiting for more data from stream~n")
     (gen_tcp:send socket `(,token ,length ,query))
     (case (gen_tcp:send socket `(,token ,length ,query))
       ((= `#(error ,_reason) failure) failure)
       ('ok
        (case (recv socket)
          ((= `#(error ,_reason) failure) failure)
          (`#(ok ,packet)
           ;; TODO: validate type
           (spawn (lambda () (! pid (rethinkdb_response:get-response (jsx:decode packet)))))
           (stream-poll `#(,socket ,token) pid))))))))

;;; ===================================================== [ Internal functions ]

;; TODO: should this use =:= instead of == ?
(defun is-null-terminated? (b) (== (binary:at b (- (iolist_size b) 1)) 0))

(defun key-size (auth-key) (le-32-int (iolist_size auth-key)))

(defun read-until-null (socket) (read-until-null socket '[]))

(defun read-until-null (socket acc)
  (case (gen_tcp:recv socket 0)
    ((= `#(error ,_reason) failure)
     ;; (! client (self) `#(error_sending ,error))
     (gen_tcp:close socket)
     failure)
    (`#(ok ,response)
     (let ((result `(,acc ,response)))
       (case (is-null-terminated? response)
         ('true  `#(ok ,(iolist_to_binary result)))
         ('false (read-until-null socket result)))))))

;; When the response_type is SUCCESS_PARTIAL=3, we can call next to send more data
(defun next (_query) 'continue)

(defun stream-stop (socket token)
  (let* ((`#(,length ,query) (len (jsx:encode `(,(STOP)))))
         ('ok     (gen_tcp:send socket `(,token ,length ,query))))))

;;; ==================================================================== [ EOF ]
