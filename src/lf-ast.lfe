(defmodule lf-ast
  (export all))

(include-file "include/ql2.hrl")

(defun build-query (query-list) (apply-seq query-list '()))

(defun apply-seq
  ([_ `#(error ,reason)]
   `#(error ,reason))
  ([`(,t . ,ts) term]
   (let ((`(,f . ,args) (tuple_to_list (erlang:append_element t term))))
     (apply-seq ts (apply (MODULE) f args))))
  (['() result]
   result))

(defun db-create
  ([name '()] (when (is_binary name))
   (make-Term type 'DB_CREATE args `(,(expr name))))
  ([name _] (when (is_list name))
   '#(error #"db-create name must be binary"))
  ([_ _]
   '#(error #"db-create stands alone")))

(defun db-drop
  ([name '()] (when (is_binary name))
   (make-Term type 'DB_DROP args `(,(expr name)))))

(defun db-list (['()] (make-Term type 'DB_LIST)))

(defun table-create (name term) (table-create name '() term))

(defun table-create
  ([name options '()] (when (is_binary name))
   (make-Term type    'TABLE_CREATE
              args    `(,(expr name))
              optargs (lists:map #'table-option-term/1 options)))
  ([name options (= db (match-Term type 'DB))] (when (is_binary name))
   (make-Term type    'TABLE_CREATE
              args    `(,db ,(expr name))
              optargs (lists:map #'table-option-term/1 options))))

(defun table-option-term
  ([`#(datacenter ,value)]  (when (is_binary value))
   (term_assocpair #"datacenter" value))
  ([`#(primary-key ,value)] (when (is_binary value))
   (term_assocpair #"primary_key" value))
  ([`#(cache-size ,value)] (when (is_integer value))
   (term_assocpair #"cache_size" value)))

(defun table-drop
  ([name '()] (when (is_binary name))
   (make-Term type 'TYPE_DROP args `(,(expr name))))
  ([name (= db (match-Term type 'DB))] (when (is_binary name))
   (make-Term type 'TABLE_DROP args `(,db ,(expr name)))))

(defun table-list
  (['()]                          (make-Term type 'TABLE_LIST))
  ([(= db (match-Term type 'DB))] (make-Term type 'TABLE_LIST args db)))

(defun count
  ([(= table (match-Term type 'TABLE))]
   (make-Term type 'COUNT args `(,table)))
  ([_]
   '#(error #"count must follow table operator")))


(defun db
  ([name '()] (when (is_binary name))
   (make-Term type 'DB args `(,(expr name))))
  ([name _] (when (is_list name))
   '#(error #"DB name must be binary"))
  ([_ _]
   '#(error #"DB must be first operation in query list")))

(defun table
  ([name '()] (when (is_binary name))
   (make-Term type 'TABLE args `(,(expr name))))
  ([name (= db (match-Term type 'DB))] (when (is_binary name))
   (make-Term type 'TABLE args (++ `(,db) `(,(expr name)))))
  ([name _] (when (is_list name))
   '#(error #"Table name must be binary"))
  ([_ _]
   '#(error #"Table can either start of follow DB operation")))

(defun insert
  ([data (= table (match-Term type 'TABLE))]
   (make-Term type 'INSERT args (++ `(,table) `(,(expr data)))))
  ([_ _]
   '#(error #"insert must follow table operator")))

(defun insert
  ([data options (= table (match-Term type 'TABLE))]
   (make-Term type    'INSERT
              args    (++ `(,table) `(,(expr data)))
              optargs (lists:map #'insert-option-term/1 options)))
  ([_ _ _]
   '#(error #"insert must follow table operator")))

(defun insert-option-term
  ([`#(upsert ,value)] (when (is_binary value))
   (term_assocpair #"upsert" value)))

(defun get
  ([key (= table (match-Term type 'TABLE))] (when (orelse (is_binary key)
                                                          (is_number key)))
   (make-Term type 'GET args (++ `(,table) `(,(expr key)))))
  ([key _] (when (is_list key))
   '#(error #"get key must be binary or number"))
  ([_ _]
   '#(error #"get must follow table operator")))

(defun update
  ([data (= selection (match-Term type type))]
   (when (orelse (== type 'TABLE)   (== type 'GET)
                 (== type 'BETWEEN) (== type 'FILTER)))
   (make-Term type 'UPDATE args (++ `(,selection) `(,(func-wrap data))))))

(defun row (['()] (make-Term type 'IMPLICIT_VAR)))

(defun get-field (attr term)
  (make-Term type 'GET_FIELD args (++ `(,term) `(,(expr attr)))))

(defun add
  ([value term] (when (orelse (is_number value) (is_binary value)))
   (make-Term type 'ADD args (++ `(,term) `(,(expr value))))))

(defun sub
  ([value term] (when (is_number value))
   (make-Term type 'SUB args (++ `(,term) `(,(expr value))))))

(defun div
  ([value term] (when (is_number value))
   (make-Term type 'DIV args (++ `(,term) `(,(expr value))))))

(defun mod
  ([value term] (when (is_number value))
   (make-Term type 'MOD args (++ `(,term) `(,(expr value))))))

(defun and
  ([value term] (when (is_boolean value))
   (make-Term type 'AND args (++ `(,term) `(,(expr value))))))

(defun or
  ([value term] (when (is_boolean value))
   (make-Term type 'OR args (++ `(,term) `(,(expr value))))))

(defun eq (value term) (make-Term type 'EQ args (++ `(,term) `(,(expr value)))))

(defun ne (value term) (make-Term type 'NE args (++ `(,term) `(,(expr value)))))

(defun gt (value term) (make-Term type 'GT args (++ `(,term) `(,(expr value)))))

(defun ge (value term) (make-Term type 'GE args (++ `(,term) `(,(expr value)))))

(defun lt (value term) (make-Term type 'LT args (++ `(,term) `(,(expr value)))))

(defun le (value term) (make-Term type 'LE args (++ `(,term) `(,(expr value)))))

(defun not (term) (make-Term type 'NOT args `(,term)))

(defun limit
  ([value term] (when (orelse (is_number value) (is_binary value)))
   (make-Term type 'LIMIT args (++ `(,term) `(,(expr value))))))

(defun expr ([data '()] (expr data)))

(defun expr
  ([(= item (match-Term))]             item)
  ([(= item (match-Term.AssocPair))]   item)
  ([`#(,items)] (when (is_list items))
   (make-Term type 'MAKE_OBJ optargs (lists:map #'expr/1 items)))
  ([`#(,key ,value)]                   (term_assocpair key value))
  ([items] (when (is_list items))
   (case (lists:all #'json?/1 items)
     ('true  (make-array items))
     ('false (build-query items))))
  ([f] (when (is_function f))          (func f))
  ([value]                             (make-Term type  'DATUM
                                                  datum (datum value))))

(defun make-array
  ([items] (when (is_list items))
   (make-Term type 'MAKE_ARRAY args (lists:map #'expr/1 items))))

(defun datum
  (['null]                   (make-Datum type 'R_NULL))
  ([v] (when (is_boolean v)) (make-Datum type 'R_BOOL r_bool v))
  ([v] (when (is_number v))  (make-Datum type 'R_NUM  r_num v))
  ([v] (when (is_binary v))  (make-Datum type 'R_STR  r_str v)))

(defun var ([n '()] (make-Term type 'VAR args (expr n))))

(defun func (f)
  (let* ((`#(,_ ,arity) (erlang:fun_info f 'arity))
         (args (lc ((<- n (lists:seq 1 arity))) `#(var ,n))))
    (make-Term type 'FUNC
               args (++ `(,(make-array (lists:seq 1 arity)))
                        `(,(expr (apply f args)))))))

(defun term_assocpair (key val) (make-Term.AssocPair key key val (expr val)))

(defun func-wrap (data)
  (let ((value (expr data)))
    (case (ivar-scan value)
      ('true  (func (lambda (_) value)))
      ('false value))))

(defun ivar-scan
  ([(match-Term type 'IMPLICIT_VAR)] 'true)
  ([(match-Term args args optargs optargs)]
   (orelse (case (is_list args)
             ('true  (lists:any #'ivar-scan/1 args))
             ('false (ivar-scan args)))
           (case (is_list optargs)
             ('true  (lists:any #'ivar-scan/1 optargs))
             ('false (ivar-scan args)))))
  ([(match-Term.AssocPair val val)]  (ivar-scan val))
  ([_]                               'false))

(defun json?
  (['null]                               'true)
  ([item]       (when (is_boolean item)) 'true)
  ([item]       (when (is_number item)) 'true)
  ([item]       (when (is_binary item)) 'true)
  ([`#(,lst)]   (when (is_list lst))    'true)
  ([`#(,k ,_v)] (when (is_binary k))    'true)
  ([_]                                  'false))
