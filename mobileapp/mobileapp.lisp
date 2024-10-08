(in-package :cosmos-db)

(defparameter *clever-dev-context*
  (make-cosmos-context :account-name "clmobappdev"
                       :auth-method :cli))

(defun mobile-app-containers ()
  (let ((operations
          (compose (with-database "MobileApp")
                   (list-collections)
                   #'get-results
                   (lambda (x) (getf (first x) :body))
                   #'cl-json:decode-json-from-string)))
    (funcall operations (begin-cosmos-operations *clever-dev-context*))))


(defun container-ids (data)
  (mapcar #'(lambda (collection)
              (cdr (assoc :ID collection)))
          (cdr (assoc :*DOCUMENT-COLLECTIONS data))))

(defun mobile-app-container-ids ()
  (container-ids (mobile-app-containers)))

(defun get-sample-document (container-name)
  (let ((operations
          (compose (with-database "MobileApp")
                   (with-container container-name)
                   (query-documents (format nil "SELECT FROM c") :partition-key nil :enable-cross-partition t :max-item-count 1)
                   #'get-results
                   (lambda (x) (getf (first x) :body))
                   #'cl-json:decode-json-from-string)))
    (funcall operations 
             (begin-cosmos-operations *clever-dev-context*))))

(defun get-all-samples ()
  (let ((container-ids (mobile-app-container-ids)))
    (mapcar (lambda (id)
              (cons id (get-sample-document id)))
            container-ids)))

(defun symbolize-key (key)
  "Convert a key to a keyword symbol."
  (intern (string-upcase (if (symbolp key) (symbol-name key) key)) :keyword))

(defun convert-alist-keys (alist)
  "Convert all keys in an alist to keyword symbols."
  (mapcar (lambda (pair) (cons (symbolize-key (car pair)) (cdr pair))) alist))

(defun filter-cosmos-fields (alist)
  "Remove Cosmos DB specific fields from an alist."
  (remove-if (lambda (pair)
               (member (car pair) '(:--RID :--SELF :--ETAG :--ATTACHMENTS :--TS)))
             alist))

(defun process-alist (alist)
  "Process an alist: convert keys, filter fields, and convert to plist."
  (let ((operations
          (compose #'convert-alist-keys
                   #'filter-cosmos-fields
                   #'alexandria:alist-plist)))
    (funcall operations alist)))

(defun alist-to-plist (alist)
  "Convert an alist to a plist."
  (apply 'append (mapcar (lambda (pair) (list (car pair) (cdr pair))) alist)))

(defun create-struct-from-alist (constructor &optional field-handlers)
  "Create a struct from an alist. FIELD-HANDLERS is an alist of field names and handler functions."
  (lambda (alist)
    (let* ((plist (alist-to-plist alist))  ;; Convert alist to plist
           (processed-plist
             (mapcan (lambda (pair)
                       (let ((key (car pair))
                             (value (cdr pair)))
                         (if (and field-handlers (assoc key field-handlers))
                             ;; Use handler function for the field if specified
                             (list key (funcall (cdr (assoc key field-handlers)) value))
                             ;; Otherwise just keep the pair as-is
                             (list key value))))
                     plist)))
      ;; Apply the constructor with the processed property list
      (apply constructor processed-plist))))


(defun create-chargepoint-data-record-from-alist (alist)
  "Create a chargepoint-data-record struct from an alist."
  (let ((operations
          (compose #'process-alist
                   (lambda (plist) (apply #'make-chargepoint-data-record plist)))))
    (funcall operations alist)))

;; (defun create-struct-from-alist (constructor)
;;   "Create a struct from an alist."
;;   (lambda (alist)
;;     (let ((operations
;;             (compose #'process-alist
;;                      (lambda (plist) (apply constructor plist)))))
;;       (funcall operations alist))))

(defun get-document-sample (container)
  "Retrieve a sample CDR document from the Cosmos DB."
  (let ((operations
          (compose (with-database "MobileApp")
                   (with-container container)
                   (query-documents "SELECT * FROM c" 
                                    :partition-key nil 
                                    :enable-cross-partition t 
                                    :max-item-count 1)
                   #'get-results
                   (lambda (x) (getf (first x) :body))
                   #'cl-json:decode-json-from-string)))
    (funcall operations (begin-cosmos-operations *clever-dev-context*))))

(defun get-cdr-as-struct ()
  "Retrieve a CDR document and convert it to a chargepoint-data-record struct."
  (let ((operations
          (compose (lambda () (cadr (assoc :*documents (get-document-sample "CDR"))))
                   #'create-chargepoint-data-record-from-alist)))
    (funcall operations)))

(defun get-document-as-struct (container constructor)
  "Retrieve a CDR document and convert it to a chargepoint-data-record struct."
  (let ((operations
          (compose (lambda () (cadr (assoc :*documents (get-document-sample container))))
                   (create-struct-from-alist constructor))))
    (funcall operations)))

;; Helper function to print struct details (useful for REPL testing)
(defun print-chargepoint-data-record (record)
  (format t "Chargepoint Data Record:~%")
  (loop for slot in (sb-mop:class-slots (find-class 'chargepoint-data-record))
        for slot-name = (sb-mop:slot-definition-name slot)
        do (format t "  ~A: ~A~%" slot-name (slot-value record slot-name))))

;; Example usage in REPL:
;; (print-chargepoint-data-record *cdr-struct*)

(defun create-invoice-from-alist (alist)
  "Convert an alist to an invoice struct."
  (let* ((field-handlers '((:invoice-lines . (lambda (lines)
                                               (mapcar (create-struct-from-alist #'make-invoice-line)
                                                       lines)))))
         (create-invoice (create-struct-from-alist #'make-invoice field-handlers)))
    (funcall create-invoice alist)))

(defvar *cdr-doc* (cadr (assoc :*documents (get-document-sample "CDR"))))

(defvar *invoice-doc* (cadr (assoc :*documents (get-document-sample "Invoices"))))

(decode-json-to-struct (list *invoice-doc*) 'invoice)

(decode-json-to-struct (list *cdr-doc*) 'chargepoint-data-record)