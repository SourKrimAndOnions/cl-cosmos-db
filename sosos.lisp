(in-package :cosmos-db)

(defmacro ->> (x &rest forms)
  (labels ((insert-and-execute (x form)
             (cond
               ((symbolp form) `(,form ,x))
               ((and (listp form) (not (null form)))
                (let* ((fn (car form))
                       (args (cdr form))
                       (non-keyword-args (loop for arg in args
                                               until (keywordp arg)
                                               collect arg)) ;; collect all args until we see keyword
                       (keyword-args (nthcdr (length non-keyword-args) args)))
                  `(funcall (,fn ,@non-keyword-args ,@keyword-args) ,x)))
               (t (error "Invalid form in ->> macro: ~S" form)))))
    (if (null forms)
        x
        `(->> ,(insert-and-execute x (car forms)) ,@(cdr forms)))))

;; Helper function to execute Cosmos DB operations
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
      (list :status status-code
            :headers response-headers
            :body body))))

(defun create-database (database-id &key offer-throughput)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :post :databases nil
                                                     :content `(("id" . ,database-id))
                                                     :offer-throughput offer-throughput)))
        (cons (cons result results) context)))))

(defun create-collection (collection-id partition-key &key offer-throughput)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :post :collections nil
                                                     :content `((id . ,collection-id)
                                                                (partition-key . ((paths . #(,partition-key)))))
                                                     :offer-throughput offer-throughput)))
        (cons (cons result results) context)))))

(defun create-document (document &key partition-key-value)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :post :documents nil
                                                     :content document
                                                     :partition-key partition-key-value)))
        (cons (cons result results) context)))))

(defun query-documents (query &key parameters partition-key enable-cross-partition (max-item-count 100))
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :post :documents nil
                                                     :content `(("query" . ,query)
                                                                ("parameters" . ,(construct-parameters-header-value parameters)))
                                                     :content-type +content-type-query-json+
                                                     :enable-cross-partition enable-cross-partition
                                                     :max-item-count max-item-count
                                                     :partition-key partition-key)))
        (cons (cons result results) context)))))

;; New and updated operations

(defun list-databases ()
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :get :databases nil)))
        (cons (cons result results) context)))))

(defun get-database (database-id)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :get :database database-id)))
        (cons (cons result results) context)))))

(defun delete-database (database-id)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :delete :database database-id)))
        (cons (cons result results) context)))))

(defun list-collections ()
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :get :collections nil)))
        (cons (cons result results) context)))))

(defun get-collection (collection-id)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :get :collection collection-id)))
        (cons (cons result results) context)))))

(defun delete-collection (collection-id)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :delete :collection collection-id)))
        (cons (cons result results) context)))))

(defun get-document (document-id &key partition-key)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :get :document document-id
                                                    :content document-id
                                                    :partition-key partition-key)))
        (cons (cons result results) context)))))

(defun delete-document (document-id &key partition-key)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :delete :document document-id
                                                       :partition-key partition-key)))
        (cons (cons result results) context)))))

(defun replace-document (document-id new-document &key partition-key)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (let ((result (execute-cosmos-op context :put :document document-id
                                                    :content new-document
                                                    :partition-key partition-key)))
        (cons (cons result results) context)))))

;; Helper functions (unchanged)
(defun begin-cosmos-operations (initial-context)
  (cons '() initial-context))

(defun get-results (context-pair)
  (reverse (car context-pair)))

;; Context modification functions (unchanged)
(defun with-account (account-name)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (cons results (make-cosmos-context
                     :account-name account-name
                     :database-name (cosmos-context-database-name context)
                     :container-name (cosmos-context-container-name context)
                     :auth-method (cosmos-context-auth-method context)
                     :connection-string (cosmos-context-connection-string context))))))

(defun with-database (database-name)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (cons results (make-cosmos-context
                     :account-name (cosmos-context-account-name context)
                     :database-name database-name
                     :container-name nil
                     :auth-method (cosmos-context-auth-method context)
                     :connection-string (cosmos-context-connection-string context))))))

(defun with-container (container-name)
  (lambda (context-pair)
    (destructuring-bind (results . context) context-pair
      (cons results (make-cosmos-context
                     :account-name (cosmos-context-account-name context)
                     :database-name (cosmos-context-database-name context)
                     :container-name container-name
                     :auth-method (cosmos-context-auth-method context)
                     :connection-string (cosmos-context-connection-string context))))))

;; (->> *local-context*
;;      begin-cosmos-operations
;;      (delete-database "TestDatabase"))

;; (->> *local-context*
;;      begin-cosmos-operations
;;      (create-database "TestDatabase" :offer-throughput 400))

;; (->> *local-context*
;;      begin-cosmos-operations
;;      (create-database "TestDatabase" :offer-throughput 400)
;;      (list-databases)
;;      (delete-database "TestDatabase")
;;      get-results)
;; (->> *local-context*
;;      begin-cosmos-operations
;;      (query-documents "SELECT * FROM c WHERE c.id = @id"
;;                       :parameters '(("@id" . "1"))
;;                       :partition-key "1"))

;; (->> *local-context*
;;      begin-cosmos-operations
;;      (with-database "TestDatabase")
;;      (with-container "TestCollection")
;;      (create-database "TestDatabase" :offer-throughput 400)
;;      (create-collection "TestCollection" "/id" :offer-throughput 400)
;;      (create-document '(("id" . "1") ("name" . "Test Document")) :partition-key-value "1")
;;      (get-document "1" :partition-key "1")
;;      (query-documents "SELECT * FROM c WHERE c.id = @id"
;;                       :parameters '(("@id" . "1"))
;;                       :partition-key "1")
;;      (delete-document "1" :partition-key "1")
;;      (delete-collection "TestCollection")
;;      (delete-database "TestDatabase")
;;      get-results)

;;from alexandria
(defun compose (function &rest more-functions)
  "Returns a function composed of FUNCTION and MORE-FUNCTIONS that applies its
arguments to to each in turn, starting from the rightmost of MORE-FUNCTIONS,
and then calling the next one with the primary value of the last."
  (declare (optimize (speed 3) (safety 1) (debug 1)))
  (reduce (lambda (f g)
            (let ((f (ensure-function f))
                  (g (ensure-function g)))
              (lambda (&rest arguments)
                (declare (dynamic-extent arguments))
                (funcall f (apply g arguments)))))
          more-functions
          :initial-value function))

(defmacro compose (&rest functions)
  `(alexandria:compose ,@(reverse functions)))

;; (let ((composed-function
;;         (compose (with-database "TestDatabase")
;;                  (with-container "TestCollection")
;;                  (create-database "TestDatabase" :offer-throughput 400)
;;                  (create-collection "TestCollection" "/id" :offer-throughput 400)
;;                  (create-document '(("id" . "1") ("name" . "Test Document")) :partition-key-value "1"))))
;;   (funcall composed-function (begin-cosmos-operations *local-context*)))
