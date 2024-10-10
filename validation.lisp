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
              by any additional arguments specific to that predicate."
  (let ((predicate-name (intern (format nil "~A-P" name)))
        (list-predicate-name (intern (format nil "LIST-OF-~A-P" name)))
        (list-type-name (intern (format nil "~A-LIST" name))))
    `(progn
       ;; Define the predicate function for the base type
       (defun ,predicate-name (value)
         (let ((errors
                 (loop for (pred . args) in ',predicates
                       unless (apply pred value args)
                         collect (format nil "Failed predicate: ~A~@[ ~{~A~^ ~}~]" pred args))))
           (if errors
               (values nil (format nil "~{~A~^, ~}" errors))
               (values t nil))))
       
       ;; Define the type using satisfies with the predicate function
       (deftype ,name () '(satisfies ,predicate-name))

       ;; Define the list predicate function for lists of this type
       (defun ,list-predicate-name (value)
         "Return t if VALUE is a non-nil list containing only instances of the struct."
         (and (consp value) (every #',predicate-name value)))
       
       ;; Define the list-of type using a unique name for the list type
       (deftype ,list-type-name ()
         '(satisfies ,list-predicate-name))

       ',name)))

(defmacro define-validated-struct (name &rest slots)
  (let* ((constructor-name (intern (format nil "MAKE-~A" name)))
         (internal-constructor-name (intern (format nil "%MAKE-~A" name)))
         (predicate-name (intern (format nil "~A-P" name)))
         (list-predicate-name (intern (format nil "LIST-OF-~A-P" name)))
         (list-type-name (intern (format nil "LIST-OF-~A" name)))
         (slot-names (mapcar #'car slots))
         (slot-types (mapcar (lambda (slot) (getf (cdr slot) :type)) slots)))
    `(progn
       ;; Define the struct
       (defstruct (,name (:constructor ,internal-constructor-name))
         ,@(mapcar (lambda (slot)
                     (if (getf (cdr slot) :type)
                         `(,(car slot))
                         slot))
            slots))

       ;; Define the constructor with validation
       (defun ,constructor-name (&key ,@slot-names)
         (flet ((validate-type (value type)
                  (if (symbolp type)
                      (multiple-value-bind (valid error)
                          (funcall (intern (format nil "~A-P" type)
                                           (or (find-package :cosmos-db)
                                               *package*))
                                   value)
                        (unless valid
                          (format nil "Invalid ~A: ~A ~& ~A" type error value)))
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

       ;; Define the list predicate function using the existing struct predicate
       (defun ,list-predicate-name (value)
         "Return t if VALUE is a non-nil list containing only instances of the struct."
         (and (consp value) (every #',predicate-name value)))

       ;; Define the list type for this struct using the list predicate function
       (deftype ,list-type-name () '(satisfies ,list-predicate-name))

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

(defun valid-iso8601-string-p (str)
  (and (stringp str)
       (cl-ppcre:scan "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$" str)))

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

;; (defmacro define-validated-struct (name &rest slots)
;;   (let* ((constructor-name (intern (format nil "MAKE-~A" name)))
;;          (internal-constructor-name (intern (format nil "%MAKE-~A" name)))
;;          (slot-names (mapcar #'car slots)))
;;     `(progn
;;        (defstruct (,name (:constructor ,internal-constructor-name))
;;          ,@(mapcar (lambda (slot)
;;                      (if (getf (cdr slot) :type)
;;                          `(,(car slot))
;;                          slot))
;;             slots))
;;        (labels ((validate-type (value type slot-name)
;;                   (cond
;;                     ;; Handle nullability by allowing type to be (or type null)
;;                     ((and (consp type) (eq (car type) 'or) (member 'null (cdr type)))
;;                      (if (null value)
;;                          nil
;;                          (validate-type value (cadr type) slot-name)))
;;                     ;; Handle basic types and predicates
;;                     ((symbolp type)
;;                      (let ((predicate (intern (format nil "~A-P" type) *package*)))
;;                        (if (fboundp predicate)
;;                            (multiple-value-bind (valid error)
;;                                (funcall predicate value)
;;                              (unless valid
;;                                (format nil "Invalid ~A (~A): ~A" slot-name type error)))
;;                            (unless (typep value type)
;;                              (format nil "Invalid type for ~A (~A): expected ~A, got ~A" slot-name type (type-of value) value)))))
;;                     ;; Handle nested struct which might be given as a single alist instead of a list
;;                     ((and (listp type) (every #'consp type))
;;                      ;; Wrap in a list to standardize for validation purposes
;;                      (validate-type (list value) type slot-name))
;;                     ;; Handle other types using TYPEP
;;                     (t
;;                      (unless (typep value type)
;;                        (format nil "Invalid type for ~A (~A): expected ~A, got ~A" slot-name type (type-of value) value))))))
;;          (defun ,constructor-name (&key ,@slot-names)
;;            (let ((errors (remove nil
;;                                  (loop for name in ',slot-names
;;                                        for value in (list ,@slot-names)
;;                                        for type in (mapcar #'(lambda (slot) (getf (cdr slot) :type)) ',slots)
;;                                        when type
;;                                          collect (validate-type value type name)))))
;;              (if (null errors)
;;                  (,internal-constructor-name
;;                   ,@(loop for name in slot-names
;;                           collect (intern (symbol-name name) "KEYWORD")
;;                           collect name))
;;                  (error 'validation-error :errors (remove-if-not #'stringp errors))))))
;;        ',name)))





