(in-package :cosmos-db)

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


(defun get-struct-slots (struct-type)
  "Return a list of slot names for the given STRUCT-TYPE."
  (let ((class (find-class struct-type)))
    (loop for slot in (closer-mop:class-slots class)
          collect (intern (string-upcase (symbol-name (closer-mop:slot-definition-name slot))) :keyword))))

(defun filter-valid-keys-dynamic (args struct-type)
  "Filter ARGS to only include keys that match the slots of STRUCT-TYPE."
  (let ((valid-keys (get-struct-slots struct-type)))
    (loop for (key value) on args by #'cddr
          if (member key valid-keys)
            append (list key value))))

(defun keyword-to-struct-type (keyword)
  "Convert a keyword to its corresponding struct type symbol.
   Only applies custom mappings for specified cases, otherwise returns the keyword as symbol."
  (case keyword
    (otherwise (intern (symbol-name keyword)))))

;; (defun alist-to-struct (alist struct-type &key (type-mapper #'keyword-to-struct-type))
;;   "Decode an alist to a struct of the given STRUCT-TYPE or a list of structs if the input represents a list."
;;   (labels ((process-item (item)
;;              (let* ((constructor-name (string-upcase (format nil "MAKE-~A" (symbol-name struct-type))))
;;                     (constructor (intern constructor-name (symbol-package struct-type)))
;;                     (args (loop for (key . value) in item
;;                                 for key-name = (cond ((stringp key) key) ((symbolp key) (symbol-name key)) (t (format nil "~A" key)))
;;                                 for keyword = (intern (string-upcase key-name) :keyword)
;;                                 for final-value = (let ((nested-type (funcall type-mapper keyword)))
;;                                                     (cond
;;                                                       ;; If it's a list of alists, recursively process each item
;;                                                       ((and (listp value) 
;;                                                             (every #'listp value)
;;                                                             (every #'consp (car value))
;;                                                             nested-type)
;;                                                        (mapcar (lambda (v) 
;;                                                                  (alist-to-struct v nested-type))
;;                                                                value))
;;                                                       ;; If it's a single alist and has a nested type, process it
;;                                                       ((and (listp value) 
;;                                                             (every #'consp value)
;;                                                             nested-type)
;;                                                        (alist-to-struct value nested-type))
;;                                                       ;; Otherwise, keep the value as-is
;;                                                       (t value)))
;;                                 append (list keyword final-value)))
;;                     (filtered-args (filter-valid-keys-dynamic args struct-type)))
;;                (apply constructor filtered-args))))
;;     (cond
;;       ;; Case where the JSON object is a list of alists
;;       ((and (listp alist)
;;             (every #'listp alist)
;;             (every #'consp (car alist)))
;;        (mapcar #'process-item alist))

;;       ;; Case where the JSON object is a single alist
;;       ((and (listp alist) (every #'consp alist))
;;        (process-item alist))

;;       ;; Otherwise, raise an error indicating invalid input
;;       (t
;;        (error "Invalid object or struct type")))))



;; Example usage:
(defun alist-to-struct (alist struct-type &key (type-mapper #'keyword-to-struct-type))
  "Decode an alist to a struct of the given STRUCT-TYPE or a list of structs if the input represents a list."
  (labels ((process-item (item)
             (let* ((constructor-name (string-upcase (format nil "MAKE-~A" (symbol-name struct-type))))
                    (constructor (intern constructor-name (symbol-package struct-type)))
                    (args (loop for (key . value) in item
                                for key-name = (cond ((stringp key) key) ((symbolp key) (symbol-name key)) (t (format nil "~A" key)))
                                for keyword = (intern (string-upcase key-name) :keyword)
                                for final-value = (let ((nested-type (funcall type-mapper keyword)))
                                                    (cond
                                                      ((and (listp value) 
                                                            (every #'listp value)
                                                            (every #'consp (car value))
                                                            nested-type)
                                                       (mapcar (lambda (v) 
                                                                 (alist-to-struct v nested-type :type-mapper type-mapper))
                                                               value))
                                                      ((and (listp value) 
                                                            (every #'consp value)
                                                            nested-type)
                                                       (alist-to-struct value nested-type :type-mapper type-mapper))
                                                      (t value)))
                                append (list keyword final-value)))
                    (filtered-args (filter-valid-keys-dynamic args struct-type)))
               (apply constructor filtered-args))))
    (cond
      ((and (listp alist)
            (every #'listp alist)
            (every #'consp (car alist)))
       (mapcar #'process-item alist))
      ((and (listp alist) (every #'consp alist))
       (process-item alist))
      (t
       (error "Invalid object or struct type")))))