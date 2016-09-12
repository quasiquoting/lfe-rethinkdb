(defmodule rethinkdb_net
  (export (connect 2) (handshake 3)
          (send 2) (send-and-recv 2)
          (recv 1)
          (stream-recv 2) (stream-poll 2))
  (import (from rethinkdb_data (le-32-int 1) (len 1))
          (rename ql2
            ((enum_value_by_symbol 2) symbol->value))))

;;; http://rethinkdb.com/docs/writing-drivers/

(defun V1_0 () (le-32-int (symbol->value 'VersionDummy.Version 'V1_0)))

(defun STOP () (symbol->value 'Query.QueryType 'STOP))

;;; ==================================================================== [ API ]

(defun connect (host port)
  (let ((tcp-opts '[binary #(packet 0) #(active false)]))
    (gen_tcp:connect host port tcp-opts)))

(defun handshake (sock username password)
  (let (('ok            (gen_tcp:send sock (V1_0)))
        (first-response (read-until-null sock)))
    ;; TODO: verify protocol version
    (if (not (maps:get #"success" (parse first-response)))
      (error 'handshake (list first-response))
      (let* ((client-nonce (gen-nonce))
             (client-first-message-bare
              (binary "n=" (username binary) ",r=" (client-nonce binary)))
             (msg (map #"protocol_version"      0
                       #"authentication_method" #"SCRAM-SHA-256"
                       #"authentication"
                       (binary "n,," (client-first-message-bare binary))))
             ('ok (send-json sock msg))
             (response (read-until-null sock))
             ((map #"success"        success?
                   #"authentication" server-first-message)
              (parse response)))
        (if (not success?)
          (error 'client-first-message (list response))
          (let* (((map #"server-nonce"    server-nonce
                       #"salt"            salt
                       #"iteration-count" iter)
                  (read-first-message server-first-message))
                 (salted (salt-password password salt iter))
                 (auth-msg (binary (client-first-message-bare binary)
                             "," (server-first-message binary)
                             ",c=biws,r=" (server-nonce binary)))
                 (client-proof (calculate-proof salted auth-msg))
                 (server-key (crypto:hmac 'sha256 salted #"Server Key"))
                 (server-sig (base64:encode
                              (crypto:hmac 'sha256 server-key auth-msg)))
                 (final-msg (binary "{\"authentication\": \"c=biws,r="
                              (server-nonce binary)
                              ",p=" (client-proof binary) "\"}"))
                 ('ok (gen_tcp:send sock (list final-msg #"\x0;")))
                 (final-response (read-until-null sock))
                 (final (parse final-response)))
            (if (=/= (maps:get #"authentication" final)
                     (binary "v=" (server-sig binary)))
              (error 'invalid-server-signature)
              'ok)))))))

(defun send (socket query)
  (let* ((token             (rethinkdb_query:token query))
         (encoded           (rethinkdb_query:encode-query query))
         (`#(,length ,data) (rethinkdb_data:len encoded)))
    (gen_tcp:send socket `[,token ,length ,data])))

(defun send-and-recv (socket query)
  (send socket query)
  (jsx:decode (recv socket) '[return_maps]))

(defun recv (socket)
  (case (gen_tcp:recv socket 8)
    (`#(error ,reason) (error reason))
    (`#(ok ,_token)
     (case (gen_tcp:recv socket 4)
       (`#(error ,reason) (error reason))
       (`#(ok ,length)
        (case (gen_tcp:recv socket (binary:decode_unsigned length 'little))
          (`#(error ,reason) (error reason))
          (`#(ok    ,packet) packet)))))))

;;; ================================================================ [ Streams ]

(defun stream-recv (socket token)
  (receive (r (io:fwrite "Change feed received: ~p~n" `(,r))))
  (stream-recv socket token))

;; TODO: may want to loop on failure too...
(defun stream-poll
  ([`#(,socket ,token) pid]
   (let* ((`#(,length ,query) (len '("[2]")))
          ('ok (gen_tcp:send socket `(,token ,length ,query)))
          ('ok (gen_tcp:send socket `(,token ,length ,query))))
     (case (recv socket)
       ((= `#(error ,_reason) failure) failure)
       (`#(ok ,packet)
        ;; TODO: validate type
        (spawn (lambda () (! pid (rethinkdb_response:get-response (jsx:decode packet)))))
        (stream-poll `#(,socket ,token) pid))))))

;;; ===================================================== [ Internal functions ]

(defun null-terminated? (b) (== (binary:at b (- (iolist_size b) 1)) 0))

(defun read-until-null (socket) (do-read-until-null socket []))

(defun do-read-until-null (socket acc)
  (case (gen_tcp:recv socket 0)
    ((= `#(error ,reason) failure)
     (gen_tcp:close socket)
     (error 'reason (list acc)) failure)
    (`#(ok ,response)
     (let ((result `[,acc ,response]))
       (if (null-terminated? response)
         (iolist_to_binary result)
         (do-read-until-null socket result))))))

(defun read-first-message (bin)
  (lists:foldl
    (lambda (kv m)
      (let ((`[,k ,v] (binary:split kv #"=")))
        (do-read-first-message k v m)))
    (maps:new)
    (binary:split bin #"," '[global])))

(defun do-read-first-message
  ([#"r" r m] (maps:put #"server-nonce" r m))
  ([#"s" s m] (maps:put #"salt" (base64:decode s) m))
  ([#"i" i m] (maps:put #"iteration-count" (binary_to_integer i) m)))

(defun calculate-proof (salted-pw auth-msg)
  (let* ((client-key   (crypto:hmac 'sha256 salted-pw #"Client Key"))
         (stored-key   (crypto:hash 'sha256 client-key))
         (client-sig   (crypto:hmac 'sha256 stored-key auth-msg))
         (client-proof (crypto:exor client-key client-sig)))
    (base64:encode client-proof)))

(defun salt-password (password salt iter)
  (let ((`#(ok ,salted) (pbkdf2:pbkdf2 'sha256 password salt iter)))
    salted))

(defun gen-nonce () (base64:encode (crypto:strong_rand_bytes 24)))

(defun send-json (sock json)
  (gen_tcp:send sock (list (jsx:encode json) #"\x0;")))

(defun parse
  ([bin] (when (=:= #"\x0;" (binary_part bin (byte_size bin) -1)))
   (try
     (let ((response (binary_part bin 0 (- (byte_size bin) 1))))
       (jsx:decode response '[return_maps]))
     (catch
       (_
        (error 'bad_response (list bin)))))))
;; When the response_type is SUCCESS_PARTIAL=3,
;; we can call next to send more data
(defun next (_query) 'continue)

(defun stream-stop (socket token)
  (let* ((`#(,length ,query) (len (jsx:encode `(,(STOP)))))
         ('ok (gen_tcp:send socket `(,token ,length ,query))))))

;;; ==================================================================== [ EOF ]
