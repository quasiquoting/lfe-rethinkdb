;;; =================================================== [ rethinkdb_worker.lfe ]

(defmodule rethinkdb_worker
  (behaviour gen_server)
  (behaviour poolboy_worker)
  ;; API
  (export (start_link 1) (use 1) (use 2) (query 1) (query 2))
  ;; gen_server callbacks
  (export (init 1)
          (handle_call 3) (handle_cast 2) (handle_info 2)
          (terminate 2) (code_change 3))
  (import (rename ql2
            ((enum_value_by_symbol 2) symbol->value))))

(include-file "include/ql2.hrl")

(defrecord state socket database (token 1))

;; Macros
(defmacro DEFAULT-TIMEOUT () 30000)

;;; ==================================================================== [ API ]

(defun start_link (opts) (gen_server:start_link (MODULE) opts []))

(defun use (db-name)
  (lambda (worker)
    (use worker db-name)))

(defun use (worker db-name)
  (gen_server:cast worker `#(use ,db-name)))

(defun query (query)
  (lambda (worker)
    (query worker query)))

(defun query (worker query)
  (let ((timeout (application:get_env 'rethinkdb 'timeout (DEFAULT-TIMEOUT))))
    (gen_server:call worker `#(query ,query) timeout)))

;;; =================================================== [ gen_server callbacks ]

(defun init
  ([opts]
   (process_flag 'trap_exit 'true)
   (let* ((host           (proplists:get_value 'host     opts))
          (port           (proplists:get_value 'port     opts))
          (database       (proplists:get_value 'database opts))
          (username       (proplists:get_value 'username opts))
          (password       (proplists:get_value 'password opts))
          (`#(ok ,socket) (rethinkdb_net:connect host port))
          ('ok            (rethinkdb_net:handshake socket username password))
          (state          (make-state socket socket database database)))
     `#(ok ,state))))

(defun handle_call
  ([`#(query ,term) _from (= (match-state database database
                                          socket   socket
                                          token    token)
                             state)]
   (let* ((query (make-Query type           'START
                             query           term
                             token           token
                             global_optargs `[,(rethinkdb_util:global-db database)]))
          (reply (rethinkdb_net:send-and-recv socket query)))
     `#(reply ,reply ,(increment-token state))))
  ([_message _from state]
   `#(reply ok ,state)))

(defun handle_cast
  ([`#(use ,name) state] `#(noreply ,(set-state-database state name)))
  ([_message state]      `#(noreply ,state)))

(defun handle_info (info state) `#(noreply ,state))

(defun terminate
  ([reason (match-state socket socket)]
   (error_logger:info_msg "Terminating: ~p~n" `[,reason])
   (let (('ok (gen_tcp:close socket)))
     'ok)))

(defun code_change (_old-version state _extra) `#(ok ,state))

;;; ===================================================== [ Internal functions ]

(defun handle-response
  ([(match-Response type 'SUCCESS_ATOM response `(,datum))]
   `#(ok ,(rethinkdb_util:datum-value datum)))
  ([(match-Response type 'SUCCESS_SEQUENCE response data)]
   `#(ok ,(lists:map #'rethinkdb_util:datum-value/1 data)))
  ([(match-Response type 'SUCCESS_PARTIAL response `(,datum))]
   `#(ok ,(rethinkdb_util:datum-value datum)))
  ([(= (match-Response type 'CLIENT_ERROR)  response)] (handle-error response))
  ([(= (match-Response type 'COMPILE_ERROR) response)] (handle-error response))
  ([(= (match-Response type 'RUNTIME_ERROR) response)] (handle-error response)))

(defun handle-error
  ([(match-Response response `(,datum) type type backtrace backtrace)]
   (let ((error-msg (rethinkdb_util:datum-value datum)))
     `#(error ,error-msg ,type ,backtrace))))

(defun increment-token
  ([(= (match-state token token) state)]
   (set-state-token state (+ (state-token state) 1))))

;;; ==================================================================== [ EOF ]
