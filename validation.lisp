(in-package :cosmos-db)

(define-condition validation-error (error)
  ((errors :initarg :errors
           :reader validation-error-errors
           :documentation "A list of validation errors"))
  (:report (lambda (condition stream)
             (format stream "Validation failed:~%~{~A~%~}"
                     (validation-error-errors condition))))
  
  (:documentation "Condition for validation errors in struct creation"))

(defmacro define-type (name &body predicates)
  "Define a custom type with associated predicates for validation.
Parameters:
  NAME: A symbol representing the name of the new type.
  PREDICATES: A list of predicate forms, each of which should be a function call
              that takes the value being checked as its first argument, followed
              by any additional arguments specific to that predicate.

Behavior:
  1. Creates a predicate function named '<NAME>-PREDICATE' that applies all
     given predicates to a value and collects any validation errors.
  2. Defines a new type using Common Lisp's deftype, which uses the created
     predicate function for type checking.
  3. Returns the name of the newly defined type.

The created predicate function returns two values:
  - A boolean indicating whether all predicates passed (T) or not (NIL).
  - NIL if all predicates passed, or a string describing all failed predicates.

Usage example:
  (define-type positive-integer
    (integerp)
    (number-min-value 1))

  This defines a 'positive-integer' type that can be used in type declarations,
  typep checks, and with the define-class macro."
  (let ((predicate-name (intern (format nil "~A-P" name))))
    `(progn
       (defun ,predicate-name (value)
         (let ((errors
                 (loop for (pred . args) in ',predicates
                       unless (apply pred value args)
                         collect (format nil "Failed predicate: ~A~@[ ~{~A~^ ~}~]" pred args))))
           (if errors
               (values nil (format nil "~{~A~^, ~}" errors))
               (values t nil))))
       (deftype ,name () '(satisfies ,predicate-name))
       ',name)))

(defmacro define-validated-struct (name &rest slots)
  (let* ((constructor-name (intern (format nil "MAKE-~A" name)))
         (internal-constructor-name (intern (format nil "%MAKE-~A" name)))
         (slot-names (mapcar #'car slots))
         (slot-types (mapcar #'(lambda (slot) (getf (cdr slot) :type)) slots)))
    `(progn
       (defstruct (,name (:constructor ,internal-constructor-name))
         ,@(mapcar (lambda (slot)
                     (if (getf (cdr slot) :type)
                         `(,(car slot))
                         slot))
            slots))
       (defun ,constructor-name (&key ,@slot-names)
         (flet ((validate-type (value type)
                  (if (symbolp type)
                      (multiple-value-bind (valid error)
                          (funcall (intern (format nil "~A-P" type)
                                           (or (find-package :cosmos-db)
                                               *package*))
                                   value)
                        (unless valid
                          (format nil "Invalid ~A: ~A" type error)))
                      (unless (typep value type)
                        (format nil "Invalid type: expected ~A, got ~A" type (type-of value))))))
           (let ((errors (remove nil 
                                 (list 
                                  ,@(loop for name in slot-names
                                          for type in slot-types
                                          when type
                                            collect `(validate-type ,name ',type))))))
             (if errors
                 (error 'validation-error :errors errors)
                 (,internal-constructor-name 
                  ,@(loop for name in slot-names
                          collect (intern (symbol-name name) "KEYWORD")
                          collect name))))))
       ',name)))

;;;;;;;;;;;;;;;;;
;; predicates  ;;
;;;;;;;;;;;;;;;;;

(defun string-min-length (s min)
  (and (stringp s) (>= (length s) min)))

(defun string-max-length (s max)
  (and (stringp s) (<= (length s) max)))

(defun string-matches-pattern (s pattern)
  (and (stringp s)
       (not (null (cl-ppcre:scan pattern s)))))

(defun number-min-value (n min)
  (and (numberp n) (>= n min)))

(defun number-max-value (n max)
  (and (numberp n) (<= n max)))

(defun list-min-length (l min)
  (and (listp l) (>= (length l) min)))

(defun list-max-length (l max)
  (and (listp l) (<= (length l) max)))

(define-type username-string
  (stringp)
  (string-min-length 3)
  (string-max-length 20)
  (string-matches-pattern "^[a-zA-Z0-9_-]+$"))

(define-type id
  (stringp)
  (string-min-length 1))

(define-validated-struct user
    (id :type id)
  (username :type username-string)
  (age :type (integer 0 150)))

(defun struct-slot-names (struct)
  "Return a list of slot names for the given struct."
  (mapcar #'sb-mop:slot-definition-name 
          (sb-mop:class-slots (class-of struct))))

(defmethod cl-json:encode-json ((obj structure-object) &optional (stream cl-json:*json-output*))
  "Encode a defstruct instance to JSON without including prototype information."
  (cl-json:with-object (stream)
    ;; Encode each slot
    (loop for slot in (struct-slot-names obj)
          for slot-name = (cl-json:lisp-to-camel-case (symbol-name slot))
          for slot-value = (slot-value obj slot)
          do (cl-json:encode-object-member slot-name slot-value stream)))
  ;; Ensure the method returns NIL
  nil)

(defun decode-json-to-struct (json-string struct-type)
  "Decode a JSON string to a struct of the given STRUCT-TYPE."
  (let ((json-object (cl-json:decode-json-from-string json-string)))
    (if (and (listp json-object)
             (symbolp struct-type))
        (let* ((constructor-name (string-upcase (format nil "MAKE-~A" (symbol-name struct-type))))
               (constructor (intern constructor-name (symbol-package struct-type)))
               ;; Collect keyword arguments
               (args (loop for (key . value) in json-object
                           ;; Convert key to uppercase string
                           for key-name = (cond
                                            ((stringp key) key)
                                            ((symbolp key) (symbol-name key))
                                            (t (format nil "~A" key)))
                           for keyword = (intern (string-upcase key-name) :keyword)
                           append (list keyword value))))
          (apply constructor args))
        (error "Invalid JSON object or struct type"))))  ; If no prototype, return the original object









