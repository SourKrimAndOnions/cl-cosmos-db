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

(defvar *cdr-doc* (cadr (assoc :*documents (get-document-sample "CDR"))))
(defvar *invoices-doc* (cadr (assoc :*documents (get-document-sample "Invoices"))))
(defvar *chargingplan-doc* (cadr (assoc :*documents (get-document-sample "ChargingPlan"))))
(defvar *sessions-doc* (cadr (assoc :*documents (get-document-sample "Sessions"))))
(defvar *marketingconsent-doc* (cadr (assoc :*documents (get-document-sample "MarketingConsent"))))
(defvar *surveys-doc* (cadr (assoc :*documents (get-document-sample "transaction-surveys"))))
(defvar *powerconsumptionyearly-doc* (cadr (assoc :*documents (get-document-sample "PowerConsumptionYearly"))))
(defvar *powerconsumptionmonthly-doc* (cadr (assoc :*documents (get-document-sample "PowerConsumptionMonthly"))))
(defvar *powerconsumptiondaily-doc* (cadr (assoc :*documents (get-document-sample "PowerConsumptionDaily"))))
(defvar *domainevents-doc* (cadr (assoc :*documents (get-document-sample "DomainEvents"))))
(defvar *energysurchargeestimated-doc* (cadr (assoc :*documents (get-document-sample "EnergySurchargeEstimated"))))
(defvar *energysurchargehistoric-doc* (cadr (assoc :*documents (get-document-sample "EnergySurchargeHistoric"))))
(defvar *consumptionsummary-doc* (cadr (assoc :*documents (get-document-sample "ConsumptionSummary"))))
(defvar *pushnotificationsettings-doc* (cadr (assoc :*documents (get-document-sample "PushNotificationSettings"))))
(defvar *powermarket-doc* (cadr (assoc :*documents (get-document-sample "PowerMarket"))))

(decode-json-to-struct *invoices-doc* 'invoice)
(decode-json-to-struct *cdr-doc* 'chargepoint-data-record)
(decode-json-to-struct *chargingplan-doc* 'chargingplan-doc)
(decode-json-to-struct *powermarket-doc* 'power-market-doc)
;; (defun keyword-to-struct-type (keyword)
;;   "Convert a keyword to its corresponding struct type symbol.
;;    Only applies custom mappings for specified cases, otherwise returns the keyword as symbol."
;;   (case keyword
;;     (:invoice-lines 'invoice-line)
;;     (:segments 'segment)
;;     (:data 'data)
;;     (otherwise (intern (symbol-name keyword)))))
;; (defun decode-json-to-struct (json-input struct-type)
;;   "Decode a JSON string or alist to a struct of the given STRUCT-TYPE or a list of structs if the input represents a list."
;;   (let ((json-object (if (stringp json-input)
;;                          (cl-json:decode-json-from-string json-input)
;;                          json-input)))
;;     (labels ((process-item (item)
;;                (let* ((constructor-name (string-upcase (format nil "MAKE-~A" (symbol-name struct-type))))
;;                       (constructor (intern constructor-name (symbol-package struct-type)))
;;                       (args (loop for (key . value) in item
;;                                   for key-name = (cond
;;                                                    ((stringp key) key)
;;                                                    ((symbolp key) (symbol-name key))
;;                                                    (t (format nil "~A" key)))
;;                                   for keyword = (intern (string-upcase key-name) :keyword)
;;                                   for final-value = (let ((nested-type (keyword-to-struct-type keyword)))
;;                                                       (cond
;;                                                         ;; If it's a list of alists, recursively process each item
;;                                                         ((and (listp value) 
;;                                                               (every #'listp value)
;;                                                               (every #'consp (car value))
;;                                                               nested-type)
;;                                                          (mapcar (lambda (v) 
;;                                                                    (decode-json-to-struct v nested-type))
;;                                                                  value))
;;                                                         ;; If it's a single alist and has a nested type, process it
;;                                                         ((and (listp value) 
;;                                                               (every #'consp value)
;;                                                               nested-type)
;;                                                          (decode-json-to-struct value nested-type))
;;                                                         ;; Otherwise, keep the value as-is
;;                                                         (t value)))
;;                                   append (list keyword final-value)))
;;                       (filtered-args (filter-valid-keys-dynamic args struct-type)))
;;                  (apply constructor filtered-args))))
;;       (cond
;;         ;; Case where the JSON object is a list of alists
;;         ((and (listp json-object)
;;               (every #'listp json-object)
;;               (every #'consp (car json-object)))
;;          (mapcar #'process-item json-object))

;;         ;; Case where the JSON object is a single alist
;;         ((and (listp json-object) (every #'consp json-object))
;;          (process-item json-object))

;;         ;; Otherwise, raise an error indicating invalid input
;;         (t
;;          (error "Invalid JSON object or struct type"))))))


;; (decode-json-to-struct *chargingplan-doc* 'chargingplan-doc)


