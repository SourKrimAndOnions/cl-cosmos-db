(in-package :cosmos-db)

(defun execute-cosmos-op (context method resource-type resource-id &rest args)
  (let* ((content (getf args :content))
         (header-args (alexandria:remove-from-plist args :content))
         (headers (apply #'make-common-headers
                         :auth (make-auth-headers context method resource-type resource-id)
                         header-args)))
    (multiple-value-bind (body status-code response-headers uri stream must-close reason-phrase)
        (dex:request 
         (build-uri context :resource-type resource-type :resource-id resource-id)
         :method method
         :headers headers
         :content (when content (cl-json:encode-json-to-string content)))
      (declare (ignore uri stream must-close reason-phrase))
      (values body status-code response-headers context))))

(defun compose-cosmos-ops (&rest operations)
  (labels ((execute-ops (ops context results)
             (if (null ops)
                 (let* ((final-result (first results))
                        (final-body (first final-result))
                        (other-results (rest final-result)))
                   (values-list (cons final-body (cons context (append other-results (list (reverse results)))))))
                 (multiple-value-bind (body status-code response-headers new-context)
                     (funcall (car ops) context)
                   (if (numberp status-code)
                       (if (>= status-code 400)
                           (error "Cosmos DB operation failed: ~A ~A ~A"
                                  status-code response-headers body)
                           (execute-ops (cdr ops)
                                        new-context
                                        (cons (list body status-code response-headers) results)))
                       (execute-ops (cdr ops) new-context results))))))
    (lambda (initial-context)
      (execute-ops operations initial-context '()))))


(defun create-database (database-id &key offer-throughput)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :post :databases nil
                                       :content `(("id" . ,database-id))
                                       :offer-throughput offer-throughput)
      context)))

(defun create-collection (collection-id partition-key &key offer-throughput)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :post :collections nil
                                       :content `((id . ,collection-id)
                                                  (partition-key . ((paths . #(,partition-key)))))
                                       :offer-throughput offer-throughput)
      context)))

(defun create-document (document &key partition-key-value)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :post :documents nil
                                       :content document
                                       :partition-key partition-key-value)
      context)))

(defun list-databases ()
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :get :databases nil)
      context)))

(defun get-database (database-id)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :get :database database-id)
      context)))

(defun delete-database (database-id)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :delete :database database-id)
      context)))

(defun list-collections ()
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :get :collections nil)
      context)))

(defun get-collection (collection-id)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :get :collection collection-id)
      context)))

(defun delete-collection (collection-id)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :delete :collection collection-id)
      context)))

(defun query-documents (query &key parameters partition-key enable-cross-partition (max-item-count 100))
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :post :documents nil
                                       :content `(("query" . ,query)
                                                  ("parameters" . ,(construct-parameters-header-value parameters)))
                                       :content-type +content-type-query-json+
                                       :enable-cross-partition enable-cross-partition
                                       :max-item-count max-item-count
                                       :partition-key partition-key)
      context)))

(defun get-document (document-id &key partition-key)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :get :document document-id
                                      :partition-key partition-key)
      context)))

(defun delete-document (document-id &key partition-key)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :delete :document document-id
                                         :partition-key partition-key)
      context)))

(defun replace-document (document-id new-document &key partition-key)
  (lambda (context)
    (multiple-value-call #'values
      (execute-cosmos-op context :put :document document-id
                                      :content new-document
                                      :partition-key partition-key)
      context)))

(defun with-account (account-name)
  (lambda (context)
    (values nil nil nil
            (make-cosmos-context
             :account-name account-name
             :database-name (cosmos-context-database-name context)
             :container-name (cosmos-context-container-name context)
             :auth-method (cosmos-context-auth-method context)
             :connection-string (cosmos-context-connection-string context)))))

(defun with-database (database-name)
  (lambda (context)
    (values nil nil nil
            (make-cosmos-context
             :account-name (cosmos-context-account-name context)
             :database-name database-name
             :container-name nil
             :auth-method (cosmos-context-auth-method context)
             :connection-string (cosmos-context-connection-string context)))))

(defun with-container (container-name)
  (lambda (context)
    (values nil nil nil
            (make-cosmos-context
             :account-name (cosmos-context-account-name context)
             :database-name (cosmos-context-database-name context)
             :container-name container-name
             :auth-method (cosmos-context-auth-method context)
             :connection-string (cosmos-context-connection-string context)))))
