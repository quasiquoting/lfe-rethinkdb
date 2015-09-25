(defmodule lf-server
  (behaviour gen_server)
  ;; API
  (export (start_link 0) (stop 0)
          (add-pool 1) (remove-pool 1)
          (add-worker 2) (get-worker 1)
          (get-all-workers 1))
  ;; gen_server
  (export (init 1)
          (handle_call 3) (handle_cast 2) (handle_info 2)
          (terminate 2) (code_change 3)))

(defrecord state (pools '[]))


;;; ============================================================================
;;; ===                               API                                    ===
;;; ============================================================================

(defun start_link ()
  (gen_server:start_link `#(local ,(MODULE)) (MODULE) '[] '[]))

(defun stop () (gen_server:call (MODULE) 'stop))

(defun add-pool (ref) (gen_server:cast (MODULE) `#(add-pool ,ref)))

(defun remove-pool (ref) (gen_server:cast (MODULE) `#(remove-pool ,ref)))

(defun add-worker (ref pid) (gen_server:cast (MODULE) `#(add-worker ,ref ,pid)))

(defun get-worker (ref) (lf-data:rand-nth (get-all-workers ref)))

(defun get-all-workers (ref) (ets:lookup_element (MODULE) `#(pool ,ref) 2))


;;; ============================================================================
;;; ===                       gen_server callbacks                           ===
;;; ============================================================================

(defun init (['()] `#(ok (make-state))))

(defun handle_call
  (['stop _from state] `#(stop normal stopped ,state))
  ([_req  _from state] `#(reply ignored ,state)))

(defun handle_cast
  ([`#(add-pool ,ref) (= (match-state pools pools) state)]
   (let (('true (insert-pool ref '[])))
     `#(noreply ,(set-state-pools state `(,ref . ,pools)))))
  ([`#(remote-pool ,ref) (= (match-state pools pools) state)]
   (let (('true (ets:delete (MODULE) `#(pool ,ref))))
     `#(noreply ,(set-state-pools state (lists:delete ref pools)))))
  ([`#(add-worker ,ref ,pid) state]
   (let* ((workers (get-all-workers ref))
          ('true   (insert-pool ref `(,pid . ,workers))))
     (erlang:monitor 'process pid)
     `#(noreply ,state)))
  ([_request state]
   `#(noreply ,state)))

(defun handle_info
  ([`#(DOWN ,_ process ,pid ,_) (= (match-state pools pools) state)]
   (lists:foreach
    (lambda (ref)
      (let ((workers (get-all-workers ref)))
        (case (lists:member pid workers)
          ('false 'false)
          ('true
           (let (('true (insert-pool ref (lists:delete pid workers)))))))))
    pools)
   `#(noreply ,state))
  ([_info state]
   `#(noreply ,state)))

(defun terminate (_reason _state) 'ok)

(defun code_change (_old-version state _extra) `#(ok ,state))


;;; ============================================================================
;;; ===                           PRIVATE API                                ===
;;; ============================================================================

(defun insert-pool (ref workers) (ets:insert (MODULE) `#(#(pool ,ref) ,workers)))
