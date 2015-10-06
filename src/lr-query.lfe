(defmodule lr-query
  (export (query 1) (query 2)
          (encode-query 1)
          (build-query 1)
          (expr 1))
  (export all)) ;; For now... build-query/1 fails otherwise

(include-file "include/ql2.hrl")

(defun START () (ql2:enum_value_by_symbol 'Query.QueryType 'START))


;;; ============================================================================
;;; ===                            PUBLIC API                                ===
;;; ============================================================================

;; I definitely don't love this...
(defun query (raw-query) (query raw-query '[#()]))

;; TODO optargs
(defun query
  ([raw-query _opts] (when (is_list raw-query))
   (make-Query type 'START
               query (build-query raw-query)
               token (generate-token)
               ;; global_optargs `(,(ql2-util:global-db database))
               )))

(defun encode-query
  ([(match-Query type 'START query term)]
   (ljson:encode `(,(START) ,(++ (encode-term term) '[#()])))))


;;; ============================================================================
;;; ===                           PRIVATE API                                ===
;;; ============================================================================

(defun build-query (query-list) (apply-seq query-list '()))

(defun apply-seq
  ([_ `#(error ,reason)]
   `#(error ,reason))
  ([`(,t . ,ts) term]
   (let ((`(,f . ,args) (tuple_to_list (erlang:append_element t term))))
     (apply-seq ts (apply (MODULE) f args))))
  (['() result]
   result))

(defun generate-token ()
  (binary ((lr-data:rand-int 3709551616) (size 64) little)))


;;; PREDICATES

(defun id? (id) (orelse (is_binary id) (is_number id)))

(defun json?
  (['null]                               'true)
  ([item]       (when (is_boolean item)) 'true)
  ([item]       (when (is_number item)) 'true)
  ([item]       (when (is_binary item)) 'true)
  ([`#(,lst)]   (when (is_list lst))    'true)
  ([`#(,k ,_v)] (when (is_binary k))    'true)
  ([_]                                  'false))


;;; REQL

(defun db-create
  ([name '()] (when (is_binary name))
   (term 'DB_CREATE `(,(expr name))))
  ([name _] (when (is_list name))
   '#(error #"db-create name must be binary"))
  ([_ _] '#(error #"db-create stands alone")))

(defun db-drop
  ([name '()] (when (is_binary name))
   (term 'DB_DROP `(,(expr name)))))

(defun db-list (['()] (term 'DB_LIST)))

(defun table-create (name term) (table-create name '() term))

(defun table-create
  ([name options '()] (when (is_binary name))
   (let ((args    `(,(expr name)))
         (optargs (lists:map #'table-option-term/1 options)))
     (term 'TABLE_CREATE args optargs)))
  ([name options (= db (match-Term type 'DB))] (when (is_binary name))
   (let ((args    `(,db ,(expr name)))
         (optargs (lists:map #'table-option-term/1 options)))
     (term 'TABLE_CREATE args optargs))))

(defun table-drop
  ([name '()] (when (is_binary name))
   (term 'TABLE_DROP `(,(expr name))))
  ([name (= db (match-Term type 'DB))] (when (is_binary name))
   (term 'TABLE_DROP `(,db ,(expr name)))))

(defun table-list
  (['()]                          (term 'TABLE_LIST))
  ([(= db (match-Term type 'DB))] (term 'TABLE_LIST `(,db))))

;; TODO: index-{create,drop,list,rename,status,wait}

(defun changes ([(= table (match-Term type 'TABLE))] (term 'CHANGES `(,table))))

(defun insert
  ([data (= table (match-Term type 'TABLE))]
   (term 'INSERT `(,table . (,(expr data)))))
  ([_ _]
   '#(error #"insert must follow table operator")))

(defun insert
  ([data options (= table (match-Term type 'TABLE))]
   (let ((args    (++ `(,table) `(,(expr data))))
         (optargs (lists:map #'insert-option-term/1 options)))
     (term 'INSERT args optargs)))
  ([_ _ _]
   '#(error #"insert must follow table operator")))

;; TODO: optargs
(defun update
  ([data (= selection (match-Term type type))]
   (when (orelse (== type 'TABLE)   (== type 'GET)
                 (== type 'BETWEEN) (== type 'FILTER)))
   (let ((args (++ `(,selection) `(,(func-wrap data)))))
     (term 'UPDATE args))))

;; TODO: verify, optargs(, generalize?)
(defun replace
  ([data (= selection (match-Term type type))]
   (when (orelse (== type 'TABLE)   (== type 'GET)
                 (== type 'BETWEEN) (== type 'FILTER)))
   (let ((args `(,selection . (,(func-wrap data)))))
     (term 'REPLACE args))))

;; TODO: optargs
(defun delete
  ([obj-or-sq]
   (term 'DELETE `(,obj-or-sq))))


;; TODO: verify
(defun sync
  ([(= table (match-Term type 'TABLE))] (term 'SYNC `(,table)))
  ([_] #(error #"sync must follow table operator")))

(defun db
  ([name '()] (when (is_binary name)) (term 'DB `(,(expr name))))
  ([name _] (when (is_list name))     '#(error #"DB name must be binary"))
  ([_ _] '#(error #"DB must be first operation in query list")))

(defun table
  ([table-name '()] (when (is_binary table-name))
   (term 'TABLE `(,(expr table-name))))
  ([table-name (= db (match-Term type 'DB))] (when (is_binary table-name))
   (term 'TABLE `(,db ,(expr table-name))))
  ([table-name _] (when (is_list table-name))
   '#(error #"Table name must be binary"))
  ([_ _] '#(error #"Table can either start of follow DB operation")))

(defun get
  ([id (= table (match-Term type 'TABLE))]
   (case (id? id)
     ('true  (term 'GET `(,table . (,(expr id)))))
     ('false '#(error #"id must be binary or number"))))
  ([_ _] '#(error #"get must follow table operator")))

;; TODO: verify, optargs
(defun get-all
  ([ids (= table (match-Term type 'TABLE))]
   (case (lists:all #'id?/1 ids)
     ('true  (term 'GET_ALL `(,table . ,ids)))
     ('false '#(error #"all ids must be binary or number")))))

(defun get-field (field-name sel) (term 'GET_FIELD `(,sel ,(expr field-name))))

;; TODO: verify, optargs
(defun between
  ([lower-key upper-key sel]
   (term 'BETWEEN `(,sel ,(expr lower-key) ,(expr upper-key)))))

;; TODO: optargs
(defun filter (obj-or-func sq) (term 'FILTER `(,sq ,(expr obj-or-func))))

;; TODO: inner-join, outer-join, eq-join

;; TODO: zip, map, with-fields, concat-map

;; TODO order-by

(defun limit ([n sq] (when (is_number n)) (term 'LIMIT `(,sq ,(expr n)))))

(defun skip ([n sel] (when (is_number n)) (term 'SKIP `(,sel ,(expr n)))))

(defun slice
  ([n1 n2 sq] (when (andalso (is_number n1) (is_number n2)))
   (term 'SLICE `(,sq ,(expr n1) ,(expr n2)))))

(defun nth ([n sq] (when (is_number n)) (term 'NTH `(,sq ,(expr n)))))

(defun indexes-of (obj-or-func sq) (term 'INDEXES_OF `(,sq ,(expr obj-or-func))))

(defun empty? (sq) (term 'IS_EMPTY `(,sq)))

(defun union ([sqs] (when (is_list sqs)) (term 'UNION sqs)))

(defun sample ([n sq] (when (is_number n)) (term 'SAMPLE `(,sq ,(expr n)))))

(defun group (s sq) (term 'GROUP `(,sq ,s)))

(defun ungroup (grouped) (term 'UNGROUP `(,grouped)))

(defun reduce (f sq) (term 'REDUCE `(,sq ,(expr f))))

(defun count (sq) (term 'COUNT `(,sq)))

(defun count (x-or-func sq) (term 'COUNT `(,sq ,(expr x-or-func))))

(defun sum (sq) (term 'SUM `(,sq)))

(defun sum (field-or-func sq) (term 'SUM `(,sq ,(expr field-or-func))))

(defun avg (sq) (term 'AVG `(,sq)))

(defun avg (field-or-func sq) (term 'AVG `(,sq ,(expr field-or-func))))

(defun min (sq) (term 'MIN `(,sq)))

(defun min (field-or-func sq) (term 'MIN `(,sq ,(expr field-or-func))))

(defun max (sq) (term 'MAX `(,sq)))

(defun max (field-or-func sq) (term 'MAX `(,sq ,(expr field-or-func))))

(defun distinct (sq) (term 'DISTINCT `(,sq)))

(defun contains? (x-or-func sq) (term 'CONTAINS `(,sq ,(expr x-or-func))))

;; TODO: pluck, without, merge

;; TODO: append, prepend, difference

;; TODO: set-{insert,union,intersection,difference}

(defun has-fields? (x obj-or-sq) (term 'HAS_FIELDS `(,obj-or-sq ,(expr x))))

;; TODO {insert,splice,delete,change}-at

(defun keys (obj) (term 'KEYS `(,obj)))

(defun literal (x) (term 'LITERAL `(,x)))

;; TODO: verify
(defun object (plist)
  (let ((args (lists:flatmap (lambda (t) (expr (tuple_to_list t))) plist)))
    (term 'OBJECT args)))

;; TODO: match, split, upcase, downcase

(defun add
  ([value term] (when (orelse (is_number value) (is_binary value)))
   (term 'ADD `(,term . (,(expr value))))))

(defun sub
  ([value term] (when (is_number value))
   (term 'SUB `(,term . (,(expr value))))))

(defun mul
  ([value term] (when (is_number value))
   (term 'MUL `(,term . (,(expr value))))))

(defun div
  ([value term] (when (is_number value))
   (term 'DIV `(,term . (,(expr value))))))

(defun mod
  ([value term] (when (is_number value))
   (term 'MOD `(,term . (,(expr value))))))

(defun eq (value term) (term 'EQ `(,term . (,(expr value)))))

(defun ne (value term) (term 'NE `(,term . (,(expr value)))))

(defun gt (value term) (term 'GT `(,term . (,(expr value)))))

(defun ge (value term) (term 'GE `(,term . (,(expr value)))))

(defun lt (value term) (term 'LT `(,term . (,(expr value)))))

(defun le (value term) (term 'LE `(,term . (,(expr value)))))

(defun not (bool) (term 'NOT `(,bool)))

;; TODO: optargs
(defun random
  ([n1 n2] (when (andalso (is_number n1) (is_number n2)))
   (term 'RANDOM `(,n1 ,n2))))

;; TODO verify
(defun round (n) (term 'ROUND `(,n)))

;; TODO verify
(defun ceil (n) (term 'CEIL `(,n)))

;; TODO verify
(defun floor (n) (term 'FLOOR `(,n)))

;; TODO: epoch-time, iso8601, in-timezone, timezone, during
;;       date, time-of-day, year, month, day, day-of-{week,year}
;;       hours, minutes, seconds, to-iso8601, to-epoch-time

(defun and
  ([value term] (when (is_boolean value))
   (term 'AND `(,term . (,(expr value))))))

(defun or
  ([value term] (when (is_boolean value))
   (term 'OR `(,term . (,(expr value))))))

(defun all (value term) (and value term))

(defun any (value term) (or value term))

;; TODO: progn, branch, for-each

;; TODO: error

;; TODO: coerce-to, type-of

;; TODO: verify
(defun info (x) (term 'INFO `(,x)))

;; TODO: verify
(defun json (s) (term 'JSON `(,s)))

;; TODO: http

;; TODO: uuid

;; TODO: asc, desc

;; TODO: circle, distance, fill, {to-,}geojson, get-{intersection,nearest}
;;       includes?, intersects?, line, point, polygon-sub

;; TODO: verify
(defun config
  ([(= table (match-Term type 'TABLE))] (term 'CONFIG `(,table)))
  ([(= db    (match-Term type 'DB))]    (term 'CONFIG `(,db))))

;; TODO: verify
(defun rebalance
  ([(= table (match-Term type 'TABLE))] (term 'REBALANCE `(,table)))
  ([(= db    (match-Term type 'DB))]    (term 'REBALANCE `(,db))))

;; TODO: verify
(defun status
  ([(= table (match-Term type 'TABLE))] (term 'STATUS `(,table))))

;; TODO: minval, maxval

(defun row (['()] (term 'IMPLICIT_VAR)))

(defun var ([n '()] (term 'VAR `(,(expr n)))))


;;; ENCODING

(defun datum
  (['null]                   (make-Datum type 'R_NULL))
  ([v] (when (is_boolean v)) (make-Datum type 'R_BOOL r_bool v))
  ([v] (when (is_number v))  (make-Datum type 'R_NUM  r_num v))
  ([v] (when (is_binary v))  (make-Datum type 'R_STR  r_str v)))

(defun expr ([data '()] (expr data)))

(defun expr
  ([(= item (match-Term))]             item)
  ([(= item (match-Term.AssocPair))]   item)
  ([`#(,items)] (when (is_list items))
   (term 'MAKE_OBJ '() (lists:map #'expr/1 items)))
  ([`#(,key ,value)]                   (term-assocpair key value))
  ([items] (when (is_list items))
   (case (lists:all #'json?/1 items)
     ('true  (make-array items))
     ('false (build-query items))))
  ([f] (when (is_function f))          (func f))
  ([value]                             (term 'DATUM (datum value) '() '())))

(defun func (f)
  (let* ((`#(,_ ,arity) (erlang:fun_info f 'arity))
         (args (lc ((<- n (lists:seq 1 arity))) `#(var ,n))))
    (let ((args `(,(make-array (lists:seq 1 arity)) . `(,(expr (apply f args))))))
      (term 'FUNC args))))

(defun func-wrap (data)
  (let ((value (expr data)))
    (case (ivar-scan value)
      ('true  (func (lambda (_) value)))
      ('false value))))

(defun insert-option-term
  ([`#(upsert ,value)] (when (is_binary value))
   (term-assocpair #"upsert" value)))

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

(defun make-array
  ([items] (when (is_list items))
   (term 'MAKE_ARRAY (lists:map #'expr/1 items))))

(defun table-option-term
  ([`#(datacenter ,value)]  (when (is_binary value))
   (term-assocpair #"datacenter" value))
  ([`#(primary-key ,value)] (when (is_binary value))
   (term-assocpair #"primary_key" value))
  ([`#(cache-size ,value)] (when (is_integer value))
   (term-assocpair #"cache_size" value)))

(defun term
  ([type] (when (is_atom type))
   (make-Term type type)))

(defun term
  ([type args] (when (is_atom type))
   (make-Term type type args args)))

(defun term
  ([type args optargs] (when (is_atom type))
   (make-Term type type args args optargs optargs)))

(defun term
  ([type datum args optargs] (when (is_atom type))
   (make-Term type type args args datum datum optargs optargs)))

(defun term-assocpair (key val) (make-Term.AssocPair key key val (expr val)))

(defun encode-term
  ([(match-Term type 'DATUM datum datum)] (encode-datum datum))
  ([(match-Term type type args terms)]
   `(,(ql2:enum_value_by_symbol 'Term.TermType type)
     ,(lists:map #'encode-term/1 terms)))
  ([(match-Term.AssocPair key key val val)]
   `(#(,key ,(encode-term val))))
  ([(match-Query.AssocPair key key val val)]
   `(#(,key ,(encode-term val))))
  ([_] []))

(defun encode-datum
  ([(match-Datum type 'R_NULL)]                                'null)
  ([(match-Datum type 'R_BOOL r_bool v)] (when (is_boolean v)) v)
  ([(match-Datum type 'R_NUM  r_num v)]  (when (is_number  v)) v)
  ([(match-Datum type 'R_STR  r_str v)]  (when (is_binary  v)) v))
