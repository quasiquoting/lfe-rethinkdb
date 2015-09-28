(defmodule lf-worker
  (behaviour gen_server)
  ;; API
  (export (start_link 2) (db 2) (query 2))
  ;; gen_server
  (export (init 1)
          (handle_call 3) (handle_cast 2) (handle_info 2)
          (terminate 2) (code_change 3)))

(include-file "include/ql2_pb.hrl")

(defrecord state socket database (token 1))


;;; http://rethinkdb.com/docs/writing-drivers/

(defun V0_4 () (lf-data:le-32-int (ql2:enum_value_by_symbol 'VersionDummy.Version 'V0_4)))

;; (defun JSON () (le-32-int (ql2:enum_value_by_symbol 'VersionDummy.Protocol 'JSON)))

;; (defun STOP () (ql2:enum_value_by_symbol 'Query.QueryType 'STOP))


;;; ============================================================================
;;; ===                            PUBLIC API                                ===
;;; ============================================================================

(defun start_link (ref opts)
  (let ((`#(ok ,pid) (gen_server:start_link (MODULE) `(,opts) '())))
    (lf-server:add_worker ref pid)
    `#(ok ,pid)))

(defun db
  ([pid name] (when (is_binary name))
   (gen_server:cast pid `#(use ,name))))

(defun query (pid query)
  (let ((timeout (application:get_env 'lefink 'timout 30000)))
    (gen_server:call pid `#(query ,query) timeout)))


;;; ============================================================================
;;; ===                       gen_server callbacks                           ===
;;; ============================================================================

(defun init
  ([`(,opts)]
   (let* ((host           (proplists:get_value 'host     opts '#(127 0 0 1)))
          (port           (proplists:get_value 'port     opts 28015))
          (database       (proplists:get_value 'database opts #"test"))
          (auth-key       (proplists:get_value 'auth-key opts #""))
          (tcp-opts       '[binary #(packet 0) #(active false)])
          (`#(ok ,socket) (gen_tcp:connect host port tcp-opts))
          ('ok            (login socket auth-key))
          (state          (make-state socket socket database database)))
     `#(ok ,state))))

(defun handle_call
  ([`#(query ,term) _from (= state (match-state database database
                                                socket   socket
                                                token    token))]
   (let* ((query (make-query type 'START
                             query term
                             token token
                             global_optargs `(,(ql2-util:global-db database))))
          (reply (send-and-recv socket query)))
     `#(reply ,reply ,(increment-token state))))
  ([_message _from state]
   `#(reply ok ,state)))

(defun handle_cast
  ([`#(use ,name) state] `#(noreply ,(set-state-database state name)))
  ([_message state]      `#(noreply ,state)))

(defun handle_info (info state)
  (io:fwrite "Info: ~p~n" `(,info))
  `#(noreply ,state))

(defun terminate (reason state)
  (io:fwrite "Terminating: ~p~n" `(,reason))
  (gen_tcp:close (state-socket state))
  'ok)

(defun code_change (_old-version state _extra) `#(ok ,state))


;;; ============================================================================
;;; ===                           PRIVATE API                                ===
;;; ============================================================================

(defun send-and-recv (socket query)
  (send socket query)
  (let ((response (recv socket)))
    (handle-response (ql2_pb:decode_response response))))

(defun send (socket query)
  (gen_tcp:send socket (tuple_to_list (lf-data:len (lf-query:query query)))))

(defun recv (socket)
  (let* ((`#(ok ,response-length) (gen_tcp:recv socket 4))
         (response-length* (binary:decode_unsigned response-length 'little))
         (`#(ok ,response) (gen_tcp:recv socket response-length*)))
    response))

(defun handle-response
  ([(match-response type 'SUCCESS_ATOM response `(,datum))]
   `#(ok ,(ql2-util:datum-value datum)))
  ([(match-response type 'SUCCESS_SEQUENCE response data)]
   `#(ok ,(lists:map #'ql2-util:datum-value/1 data)))
  ([(match-response type 'SUCCESS_PARTIAL response `(,datum))]
   `#(ok ,(ql2-util:datum-value datum)))
  ([(= (match-response type 'CLIENT_ERROR)  response)] (handle-error response))
  ([(= (match-response type 'COMPILE_ERROR) response)] (handle-error response))
  ([(= (match-response type 'RUNTIME_ERROR) response)] (handle-error response)))

(defun handle-error
  ([(match-response response `(,datum) type type backtrace backtrace)]
   (let ((error-msg (ql2-util:datum-value datum)))
     `#(error ,error-msg ,type ,backtrace))))

(defun login (socket auth-key)
  (let (('ok (gen_tcp:send socket (V0_4)))
        ('ok (gen_tcp:send socket (tuple_to_list (lf-data:len auth-key))))
        (`#(ok ,response) (read-until-null socket)))
    (case (== response #"SUCCESS\x0;")
      ('true  'ok)
      ('false
       (io:fwrite "Error: ~s~n" `(,response))
       `#(error ,response)))))

(defun increment-token
  ([(= (match-state token token) state)]
   (set-state-token state (+ (state-token state) 1))))

(defun is-null-terminated? (b) (== (binary:at b (- (iolist_size b) 1)) 0))

(defun read-until-null (socket) (read-until-null socket '[]))

(defun read-until-null (socket acc)
  (let* ((`#(ok ,response) (gen_tcp:recv socket 0))
         (result           `(,acc ,response)))
    (case (is-null-terminated? response)
      ('true  `#(ok ,(iolist_to_binary result)))
      ('false (read-until-null socket result)))))




;; (defun stream-recv (socket token)
;;   (receive (r (io:fwrite "Change feed received: ~p~n" `(,r))))
;;   (stream-recv socket token))

;; ;; TODO: may want to loop on failure too...
;; (defun stream-poll
;;   ([`#(,socket ,token) pid]
;;    (let ((`#(,length ,query) (lf-data:len '("[2]"))))
;;      ;; (io:format "Block socket <<< waiting for more data from stream~n")
;;      (gen_tcp:send socket `(,token ,length ,query))
;;      (case (gen_tcp:send socket `(,token ,length ,query))
;;        ((= `#(error ,_reason) failure) failure)
;;        ('ok
;;         (case (recv socket)
;;           ((= `#(error ,_reason) failure) failure)
;;           (`#(ok ,packet)
;;            ;; TODO: validate type
;;            (spawn (lambda () (! pid (lf-response:get-response (ljson:decode packet)))))
;;            (stream-poll `#(,socket ,token) pid))))))))

;; When the response_type is SUCCESS_PARTIAL=3, we can call next to send more data
;; (defun next (_query) 'continue)

;; (defun stream-stop (socket token)
;;   (let* ((`#(,length ,query) (lf-data:len (ljson:encode `(,(STOP)))))
;;          ('ok     (gen_tcp:send socket `(,token ,length ,query))))))
