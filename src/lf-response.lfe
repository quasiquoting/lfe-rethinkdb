(defmodule lf-response
  (export (get-type 1) (get-response 1)
          (handle 3)))

;;; ============================================================================
;;; ===                            PUBLIC API                                ===
;;; ============================================================================

(defun handle (socket token packet)
  (let* ((response (ljson:decode packet))
         (t (get-type response))
         (r (get-response response)))
    (cond
     ((success? t) `#(ok ,r))
     ((partial? t)
      ;; (io:format "Partial response <<< Get more data~n")
      (let* ((recv (spawn 'lf-net 'stream-recv `(,socket ,token)))
             (pid  (spawn 'lf-net 'stream-poll `(#(,socket ,token) ,recv))))
        ;; (io:format "Do something else here~n")
        `#(ok #(pid ,pid) ,r)))
     ((error? t)  `#(error ,r))
     ('true       '#(error unknown-response)))))


(defun get-type (response) (val->symbol (proplists:get_value #"t" response)))

(defun get-response (response) (proplists:get_value #"r" response))


;;; ============================================================================
;;; ===                           PRIVATE API                                ===
;;; ============================================================================

(defun val->symbol (val)
  (try (ql2:enum_symbol_by_value 'Response.ResponseType val)
    (catch (_ 'undefined))))

(defun success? (type) (lists:member type '(SUCCESS_ATOM SUCCESS_SEQUENCE)))

(defun partial? (type) (=:= type 'SUCCESS_PARTIAL))

(defun error? (type)
  (lists:member type '(CLIENT_ERROR COMPILE_ERROR RUNTIME_ERROR)))
