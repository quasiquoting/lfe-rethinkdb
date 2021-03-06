(defmodule lr-worker
  (behaviour gen_server)
  ;; API
  (export (start_link 2) (use 2) (query 2))
  ;; gen_server
  (export (init 1)
          (handle_call 3) (handle_cast 2) (handle_info 2)
          (terminate 2) (code_change 3)))

(include-file "include/ql2.hrl")

(defrecord state socket database (token 1))


;;; http://rethinkdb.com/docs/writing-drivers/

(defun V0_4 () (lr-data:le-32-int (ql2:enum_value_by_symbol 'VersionDummy.Version 'V0_4)))

(defun JSON () (lr-data:le-32-int (ql2:enum_value_by_symbol 'VersionDummy.Protocol 'JSON)))

;; (defun STOP () (ql2:enum_value_by_symbol 'Query.QueryType 'STOP))


;;; ============================================================================
;;; ===                            PUBLIC API                                ===
;;; ============================================================================

(defun start_link (ref opts)
  (let ((`#(ok ,pid) (gen_server:start_link (MODULE) `(,opts) '())))
    (lr-server:add-worker ref pid)
    `#(ok ,pid)))

(defun use
  ([pid name] (when (is_binary name))
   (gen_server:cast pid `#(use ,name))))

(defun query (pid query)
  (let ((timeout (application:get_env 'lfe-rethinkdb 'timeout 30000)))
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
   (let* ((query (make-Query type 'START
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
    ;; (handle-response (ql2_pb:decode_response response))
    response))

(defun send (socket query)
  (let* ((token (binary ((Query-token query) (size 64) little)))
         (`#(,length ,data) (lr-data:len (lr-query:encode-query query))))
    (gen_tcp:send socket `(,token ,length ,data))))

(defun recv (socket)
  (let* ((`#(ok ,_token)          (gen_tcp:recv socket 8))
         (`#(ok ,response-length) (gen_tcp:recv socket 4))
         (response-length* (binary:decode_unsigned response-length 'little))
         (`#(ok ,response) (gen_tcp:recv socket response-length*)))
    response))

(defun handle-response
  ([(match-Response type 'SUCCESS_ATOM response `(,datum))]
   `#(ok ,(ql2-util:datum-value datum)))
  ([(match-Response type 'SUCCESS_SEQUENCE response data)]
   `#(ok ,(lists:map #'ql2-util:datum-value/1 data)))
  ([(match-Response type 'SUCCESS_PARTIAL response `(,datum))]
   `#(ok ,(ql2-util:datum-value datum)))
  ([(= (match-Response type 'CLIENT_ERROR)  response)] (handle-error response))
  ([(= (match-Response type 'COMPILE_ERROR) response)] (handle-error response))
  ([(= (match-Response type 'RUNTIME_ERROR) response)] (handle-error response)))

(defun handle-error
  ([(match-Response response `(,datum) type type backtrace backtrace)]
   (let ((error-msg (ql2-util:datum-value datum)))
     `#(error ,error-msg ,type ,backtrace))))

(defun login (socket auth-key)
  (let (('ok (gen_tcp:send socket (V0_4)))
        ('ok (gen_tcp:send socket (tuple_to_list (lr-data:len auth-key))))
        ('ok (gen_tcp:send socket (JSON)))
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
