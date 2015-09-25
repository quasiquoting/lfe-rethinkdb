(defmodule lefink
  (export (connect 0) (connect 1) (connect 2)
          (close 1)
          (r 1) (r 2)))

;;; ============================================================================
;;; ===                               API                                    ===
;;; ============================================================================

(defun close (sock) (gen_tcp:close sock))

(defun connect ()                   (connect "127.0.0.1"))
(defun connect (host)               (connect host 28015))
(defun connect (host port)          (connect host port #""))
(defun connect (host port auth-key)
  (let* ((opts         '[binary #(packet 0) #(active false)])
         ;; TODO: parameterize timeout?
         (`#(ok ,sock) (gen_tcp:connect host port opts 5000)) ; timeout in ms
         ('ok          (lf-net:handshake sock auth-key)))
    sock))


;;; ============================================================================
;;; ===                          RethinkDB API                               ===
;;; ============================================================================

(defun r (raw-query)        (lf-query:query raw-query))
(defun r (socket raw-query) (lf-query:query socket raw-query))
