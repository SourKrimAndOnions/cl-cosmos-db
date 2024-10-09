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
                   )))
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

(decode-json-to-struct *invoice-doc* 'invoice)

(decode-json-to-struct *cdr-doc* 'chargepoint-data-record)

(decode-json-to-struct *chargingplan-doc* 'chargingplan-doc)
(defun decode-json-to-struct (json-input struct-type)
  "Decode a JSON string or alist to a struct of the given STRUCT-TYPE or a list of structs if the input represents a list."
  (let ((json-object (if (stringp json-input)
                         (cl-json:decode-json-from-string json-input)
                         json-input)))
    (labels ((process-item (item)
               (let* ((constructor-name (string-upcase (format nil "MAKE-~A" (symbol-name struct-type))))
                      (constructor (intern constructor-name (symbol-package struct-type)))
                      (args (loop for (key . value) in item
                                  for key-name = (cond
                                                   ((stringp key) key)
                                                   ((symbolp key) (symbol-name key))
                                                   (t (format nil "~A" key)))
                                  for keyword = (intern (string-upcase key-name) :keyword)
                                  for final-value = (let ((nested-type (keyword-to-struct-type keyword)))
                                                      (cond
                                                        ;; If it's a list of alists, recursively process each item
                                                        ((and (listp value) 
                                                              (every #'listp value)
                                                              (every #'consp (car value))
                                                              nested-type)
                                                         (mapcar (lambda (v) 
                                                                   (decode-json-to-struct v nested-type))
                                                                 value))
                                                        ;; If it's a single alist and has a nested type, process it
                                                        ((and (listp value) 
                                                              (every #'consp value)
                                                              nested-type)
                                                         (decode-json-to-struct value nested-type))
                                                        ;; Otherwise, keep the value as-is
                                                        (t value)))
                                  append (list keyword final-value)))
                      (filtered-args (filter-valid-keys-dynamic args struct-type)))
                 (apply constructor filtered-args))))
      (cond
        ;; Case where the JSON object is a list of alists
        ((and (listp json-object)
              (every #'listp json-object)
              (every #'consp (car json-object)))
         (mapcar #'process-item json-object))
        
        ;; Case where the JSON object is a single alist
        ((and (listp json-object) (every #'consp json-object))
         (process-item json-object))

        ;; Otherwise, raise an error indicating invalid input
        (t
         (error "Invalid JSON object or struct type"))))))
(defun keyword-to-struct-type (keyword)
  "Convert a keyword (representing a struct field) to its corresponding struct type symbol, if applicable."
  (case keyword
    (:INVOICE-LINES 'invoice-line)
    (:DATA 'data)
    (:CHARGING-PLAN 'charging-plan)
    (:TIME-SCHEDULE 'time-schedule)
    (:SEGMENTS 'segment)
    (:POWER-CONSUMED 'power-consumed)
    (:REASON 'reason)
    (t nil)))
(json:encode-json (make-chargepoint-data-record     :ID "88833-1-1328"
                                                    :PK "5002534"
                                                    :CHARGE-POINT-ID "88833"
                                                    :CONNECTOR-ID 1
                                                    :TRANSACTION-ID 1328
                                                    :ID-TAG "88833_77CF0DC2-0829-"
                                                    :METER-VALUE-START 117239
                                                    :METER-VALUE-STOP 119451
                                                    :SOURCE "CPMS3"
                                                    :START-ENQUEUED-TIME 1649687992000000
                                                    :STOP-ENQUEUED-TIME 1649688016000000
                                                    :START-MESSAGE-ID "cfb77f8e-625c-49f0-9e8b-965c66de064b"
                                                    :STOP-MESSAGE-ID "904ffb19-809b-467c-8d14-8e52eb45eb0e"
                                                    :MAX-ENQUEUED-YEAR 2022
                                                    :MAX-ENQUEUED-MONTH 4
                                                    :MAX-ENQUEUED-DAY 11
                                                    :CONSUMPTION-TIME-LOCAL 1649695192631301
                                                    :CONSUMPTION-TIME-UTC 1649687992631301
                                                    :START-TIME-LOCAL 1649695192631301
                                                    :START-TIME-UTC 1649687992631301
                                                    :STOP-TIME-LOCAL 1649695216848377
                                                    :STOP-TIME-UTC 1649688016848377
                                                    :MAX-ENQUEUED-TIME-LOCAL 1649695216000000
                                                    :MAX-ENQUEUED-TIME-UTC 1649688016000000
                                                    :K-WH 2.212
                                                    :DATE-DIFF 0
                                                    :TRANSACTION-COMPLETE 1
                                                    :CDR-NOT-RELIABLE 0
                                                    :LAST-METER-VALUE-STOP 117239
                                                    :NEXT-METER-VALUE-START 119451
                                                    :NEGATIVE-METER-VALUE 0
                                                    :METER-VALUE-DROP 0
                                                    :METER-VALUE-DROP-TO-ZERO 0
                                                    :BRACKETED-OTHER-TRANSACTION NIL
                                                    :UNPHYSICAL-CONSUMPTION 0
                                                    :UNPHYSICAL-AVG-POWER 0
                                                    :MODEL "Charge O'Matic 1000"
                                                    :VENDOR "GoldenPower"
                                                    :CHARGE-POINT-CONFIGURATION-ID 0
                                                    :SUBSCRIPTION-ID "BC30000140"
                                                    :EXTERNAL-CUSTOMER-ID 5002534
                                                    :CUSTOMER-ID "5002534"
                                                    :SUBSCRIPTION-LINE-ID "BCL-9000641"
                                                    :SUBSCRIPTION-LINE-ID-NEWEST "BCL-9000641"
                                                    :ID-TOKEN-ID NIL
                                                    :ID-TOKEN-SUBSCRIPTION-ID NIL
                                                    :ID-TOKEN-EXTERNAL-CUSTOMER-ID NIL
                                                    :ID-TOKEN-CUSTOMER-ID NIL
                                                    :OPERATOR-ID 4
                                                    :ID-TOKEN-ERP-PRODUCT-ID NIL
                                                    :ID-TOKEN-GROUP-NAME "Temporary"
                                                    :ID-TOKEN-SUBSCRIPTION-LINE-ID NIL
                                                    :ID-TOKEN-SUBSCRIPTION-LINE-ID-NEWEST NIL
                                                    :PRODUCT-PUBLIC-NAME "Clever One"
                                                    :CHARGE-POINT-NICK-NAME NIL
                                                    :IS-ROAMING T
                                                    :ROAMING-PROVIDER-ID "DK-CLE"
                                                    :SESSION-ID "77cf0dc2-0829-42ad-bfe1-5d23ed1cd499"
                                                    :ROAMING-NETWORK "Hubject"
                                                    :KWH-PRICE-EXCL-VAT NIL
                                                    :TOTAL-PRICE-EXCL-VAT NIL
                                                    :TOTAL-PRICE-INCL-VAT NIL
                                                    :VAT-RATE 0.25
                                                    :CURRENCY-CODE NIL
                                                    :FIXED-PRICE NIL
                                                    :EVCO-ID NIL
                                                    :PLUG-AND-CHARGE-IDENTIFICATION NIL
                                                    :INTELLIGENT-CHARGING-TIME-LOCAL NIL
                                                    :INTELLIGENT-CHARGING-TIME-UTC NIL
                                                    :INTELLIGENT-CHARGING-CODE NIL
                                                    :OPERATOR-NAME "Clever"
                                                    :ADDRESS-CITY NIL
                                                    :ADDRESS-STREET-NAME NIL
                                                    :ADDRESS-POSTAL-CODE NIL
                                                    :ADDRESS-HOUSE-NUM NIL
                                                    :ADDRESS-COUNTRY NIL
                                                    :CLAIMS-OPERATOR "hubject"
                                                    :CLAIMS-AUTHENTICATION "Unknown"
                                                    :PAYMENT-METHOD NIL
                                                    :IMPACTED-BY-FREQUENCY-REGULATION NIL
                                                    :IS-PARTNER-LOCATION NIL
                                                    :TIMESTAMP 1719444696255000))


