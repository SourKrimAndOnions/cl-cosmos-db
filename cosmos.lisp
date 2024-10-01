(in-package :cosmos-db)

(defstruct cosmos-context
  account-name
  database-name
  container-name
  (auth-method :cli)
  tenant-id
  client-id
  client-secret
  connection-string)

(defun build-base-uri (context)
  (let ((connection-string (cosmos-context-connection-string context))
        (account-name (cosmos-context-account-name context)))
    (cond
      (connection-string
       (let ((endpoint (first (split-connection-string connection-string))))
         (or endpoint
             (error "Invalid connection string: missing AccountEndpoint."))))
      ((and account-name (string= (string-downcase account-name) "localhost"))
       "https://localhost:8081")
      (account-name
       (format nil "https://~A.documents.azure.com"
               account-name))
      (t
       (error "Cannot determine base URI: missing connection string or account name.")))))

(defun build-service-uri (context resource-type resource-id)
  (cond
    ((eq resource-type :databases)
     "/dbs")
    ((eq resource-type :database)
     (if resource-id
         (format nil "/dbs/~A"  resource-id)
         (format nil "/dbs/~A"  (cosmos-context-database-name context))))
    ((eq resource-type :collections)
     (format nil "/dbs/~A/colls"  (cosmos-context-database-name context)))
    ((eq resource-type :collection)
     (format nil "/dbs/~A/colls/~A" 
             (cosmos-context-database-name context)
             (or resource-id (cosmos-context-container-name context))))
    ((eq resource-type :documents)
     (format nil "/dbs/~A/colls/~A/docs" 
             (cosmos-context-database-name context)
             (cosmos-context-container-name context)))
    ((eq resource-type :document)
     (format nil "/dbs/~A/colls/~A/docs/~A" 
             (cosmos-context-database-name context)
             (cosmos-context-container-name context)
             resource-id))
    (t (error "Unknown resource type: ~A" resource-type))))

(defun build-uri (context &key resource-type resource-id)
  (let ((base-url (build-base-uri context))
        (resource-url (build-service-uri context resource-type resource-id)))
    (format nil "~A~A" base-url resource-url)))


;; (defun make-common-headers (access-token &key consistency-level continuation max-item-count
;;                                            partition-key enable-cross-partition session-token
;;                                            if-match if-none-match if-modified-since
;;                                            activity-id allow-tentative-writes)
;;   (remove nil
;;           `((,+header-authorization+ . ,(format nil "type=aad&ver=1.0&sig=~A" access-token))
;;             (,+header-version+ . "2018-12-31")
;;             (,+header-date+ . ,(local-time:format-rfc1123-timestring nil (local-time:now)))
;;             ,@(when consistency-level
;;                 `((,+header-consistency-level+ . ,(string-downcase (symbol-name consistency-level)))))
;;             ,@(when continuation
;;                 `((,+header-continuation+ . ,continuation)))
;;             ,@(when max-item-count
;;                 `((,+header-max-item-count+ . ,(write-to-string max-item-count))))
;;             ,@(when partition-key
;;                 `((,+header-partition-key+ . ,(format nil "[~S]" partition-key))))
;;             ,@(when enable-cross-partition
;;                 `((,+header-enable-cross-partition+ . "true")))
;;             ,@(when session-token
;;                 `((,+header-session-token+ . ,session-token)))
;;             ,@(when if-match
;;                 `((,+header-if-match+ . ,if-match)))
;;             ,@(when if-none-match
;;                 `((,+header-if-none-match+ . ,if-none-match)))
;;             ,@(when if-modified-since
;;                 `((,+header-if-modified-since+ . ,if-modified-since)))
;;             ,@(when activity-id
;;                 `((,+header-activity-id+ . ,activity-id)))
;;             ,@(when allow-tentative-writes
;;                 `((,+header-allow-tentative-writes+ . "true"))))))

(defun make-common-headers (access-token &key consistency-level continuation max-item-count
                                           partition-key enable-cross-partition session-token
                                           if-match if-none-match if-modified-since
                                           activity-id allow-tentative-writes)
  (with-headers (+header-version+ . "2018-12-31")
    (+header-date+ . (local-time:format-rfc1123-timestring nil (local-time:now)))
    (+header-consistency-level+ . (when consistency-level 
                                    (string-downcase (symbol-name consistency-level))))
    (+header-continuation+ . continuation)
    (+header-max-item-count+ . (when max-item-count (write-to-string max-item-count)))
    (+header-partition-key+ . (when partition-key (format nil "[~S]" partition-key)))
    (+header-enable-cross-partition+ . (when enable-cross-partition "true"))
    (+header-session-token+ . session-token)
    (+header-if-match+ . if-match)
    (+header-if-none-match+ . if-none-match)
    (+header-if-modified-since+ . if-modified-since)
    (+header-activity-id+ . activity-id)
    (+header-allow-tentative-writes+ . (when allow-tentative-writes "true"))))

(defmacro with-cosmos-context ((context) &body body)
  `(let ((access-token (get-access-token ,context)))
     (flet ((perform-request (url headers &key (method :get) content)
              (multiple-value-bind (body status response-headers)
                  (dex:request url :method method 
                                   :headers headers 
                                   :content (when content (cl-json:encode-json-to-string content)))
                (let ((json-body (cl-json:decode-json-from-string body))
                      (request-charge (gethash +header-request-charge+ response-headers)))
                  (values json-body status response-headers request-charge)))))
       ,@body)))

(defmacro with-headers (&rest header-specs)
  `(remove nil
           (list
            ,@(loop for (header-name . value) in header-specs
                    collect
                    (if (and (listp value) (eq (car value) 'when))
                        `(when ,(cadr value)
                           (cons ,header-name ,(caddr value)))
                        `(when ,value
                           (cons ,header-name ,value)))))))

;;;;;;;;;;;;;;
;; database ;;
;;;;;;;;;;;;;;
(defun create-database (context database-def &key offer-throughput
                                               consistency-level
                                               session-token
                                               activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :databases))
           (common-headers (make-common-headers access-token
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id))
           (with-headers
             ((,+header-content-type+ . "application/json")
              ,@(when offer-throughput
                  `((,+header-offer-throughput+ . ,(write-to-string offer-throughput))))))
           (headers (append common-headers create-specific-headers)))
      (perform-request url headers :method :post :content database-def))))

(defun list-databases (context &key max-item-count
                                 continuation-token
                                 consistency-level
                                 session-token
                                 activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :databases))
           (common-headers (make-common-headers access-token
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id))
           (list-specific-headers
             `(,@(when max-item-count
                   `((,+header-max-item-count+ . ,(write-to-string max-item-count))))
               ,@(when continuation-token
                   `((,+header-continuation+ . ,continuation-token)))))
           (headers (append common-headers list-specific-headers)))
      (perform-request url headers :method :get))))

(defun get-database (context database-id &key consistency-level
                                           session-token
                                           activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :database :resource-id database-id))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (perform-request url headers :method :get))))

(defun delete-database (context database-id &key consistency-level
                                              session-token
                                              activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :database :resource-id database-id))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (perform-request url headers :method :delete))))

(defun query-databases (context query &key parameters
                                        max-item-count
                                        continuation-token
                                        consistency-level
                                        session-token
                                        activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :databases))
           (common-headers (make-common-headers access-token
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id))
           (query-specific-headers
             `((,+header-content-type+ . "application/query+json")
               ,@(when max-item-count
                   `((,+header-max-item-count+ . ,(write-to-string max-item-count))))
               ,@(when continuation-token
                   `((,+header-continuation+ . ,continuation-token)))))
           (headers (append common-headers query-specific-headers))
           (content `(("query" . ,query)
                      ("parameters" . ,(loop for (name . value) in parameters
                                             collect `(("name" . ,name)
                                                       ("value" . ,value)))))))
      (perform-request url headers :method :post :content content))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; collections / containers ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defun create-collection (context collection-def &key offer-throughput offer-autopilot
                                                   consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :collections))
           (common-headers (make-common-headers access-token
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id))
           (create-specific-headers
             `((,+header-content-type+ . "application/json")
               ,@(when offer-throughput
                   `((,+header-offer-throughput+ . ,(write-to-string offer-throughput))))
               ,@(when offer-autopilot
                   `((,+header-offer-autopilot+ . ,offer-autopilot)))))
           (headers (append common-headers create-specific-headers)))
      (perform-request url headers :method :post :content collection-def))))

(defun list-collections (context &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :collections))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (perform-request url headers :method :get))))
(make-common-headers "auth"
                     :consistency-level 'consistency-level
                     :session-token 'session-token
                     :activity-id 'activity-id)
(defun get-collection (context collection-id &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :collection :resource-id collection-id))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (perform-request url headers :method :get))))

(defun delete-collection (context collection-id &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :collection :resource-id collection-id))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (perform-request url headers :method :delete))))

(defun replace-collection (context collection-id collection-def &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :collection :resource-id collection-id))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (perform-request url headers :method :put :content collection-def))))

;; Update document functions

(defun create-document (context document &key partition-key (is-upsert nil) (indexing-directive nil)
                                           consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :documents))
           (common-headers (make-common-headers access-token
                                                :partition-key partition-key
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id))
           (create-specific-headers
             `((,+header-content-type+ . "application/json")
               ,@(when is-upsert
                   `((,+header-is-upsert+ . ,"true")))
               ,@(when indexing-directive
                   `((,+header-indexing-directive+ . ,(string-downcase (symbol-name indexing-directive)))))))
           (headers (append common-headers create-specific-headers)))
      (perform-request url headers :method :post :content document))))

(defun get-document (context document-id &key partition-key consistency-level 
                                           session-token activity-id if-none-match)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :document :resource-id document-id))
           (common-headers (make-common-headers access-token
                                                :partition-key partition-key
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id
                                                :if-none-match if-none-match))
           (get-specific-headers
             `((,+header-content-type+ . "application/json")))
           (headers (append common-headers get-specific-headers)))
      (perform-request url headers))))

(defun query-documents (context query &key parameters partition-key (max-item-count 100)
                                        continuation-token enable-cross-partition
                                        consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :documents))
           (common-headers (make-common-headers access-token
                                                :partition-key partition-key
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id))
           (query-headers (with-headers
                            (+header-content-type+ . "application/query+json")
                            (+header-max-item-count+ . (write-to-string max-item-count))
                            (+header-continuation+ . continuation-token)
                            (+header-enable-cross-partition+ . "true")))
           (headers (append common-headers query-headers))
           (content `(("query" . ,query)
                      ("parameters" . ,(unless (loop for (name . value) in parameters
                                                     collect `(("name" . ,name)
                                                               ("value" . ,value)))
                                         #())))))
      (perform-request url headers :method :post :content content))))

(defun delete-document (context document-id &key partition-key consistency-level 
                                              session-token activity-id if-match)
  (with-cosmos-context (context)
    (let* ((url (build-uri context :resource-type :document :resource-id document-id))
           (common-headers (make-common-headers access-token
                                                :partition-key partition-key
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id
                                                :if-match if-match))           (headers common-headers))
      (perform-request url headers :method :delete))))


(defun example-usage ()
  (let ((client (make-cosmos-context :account-name "clmobappdev"
                                     :database-name "MobileApp"
                                     :container-name "ConsumptionSummary"
                                     :connection-string "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
                                     :auth-method :cli)))
    ;; Query documents
    ;; (query-documents client "SELECT * FROM c"
    ;;                  :parameters nil
    ;;                  :partition-key nil
    ;;                  :enable-cross-partition t)
    
    ;; (query-documents client "SELECT * FROM c WHERE c.wh = @wh"
    ;;                  :parameters '(("@wh" . 19902))
    ;;                  :partition-key "5002534")

    ;; Create a document
    ;; (create-document context '(("id" . "123") ("name" . "John Doe"))
    ;;                  :partition-key "your-partition-key"
    ;;                  :is-upsert t)
    (build-uri client :resource-type :databases)
    ;; ;; Get a document
    ;; (get-document client "BCL-9000641-2022-09-01" :partition-key "5002534")

    ;; ;; Delete a document
    ;; (delete-document context "123" :partition-key "your-partition-key")

    ;; (list-collections client)
    ;; (list-databases client)
    ))

(example-usage)
;; (multiple-value-bind (body &rest ignore)
;;     (example-usage)
;;   (loop for collection in (cdr (assoc :*document-collections body))
;;         for id-pair = (assoc :ID collection)
;;         when id-pair
;;           collect (cdr id-pair)))
(defun build-base-uri (context)
  (let ((connection-string (cosmos-context-connection-string context))
        (account-name (cosmos-context-account-name context)))
    (cond
      (connection-string
       (let ((endpoint (first (split-connection-string connection-string))))
         (or endpoint
             (error "Invalid connection string: missing AccountEndpoint."))))
      ((and account-name (string= (string-downcase account-name) "localhost"))
       "https://localhost:8081")
      (account-name
       (format nil "https://~A.documents.azure.com"
               account-name))
      (t
       (error "Cannot determine base URI: missing connection string or account name.")))))

(defparameter *local-context*
  (make-cosmos-context
   :account-name "localhost"
   :database-name "myDatabase"
   :container-name "myContainer"
   :auth-method :connection-string
   :connection-string "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="))
(defparameter *clever-dev-context*
  (make-cosmos-context :account-name "clmobappdev"
                       :database-name "MobileApp"
                       :container-name "ConsumptionSummary"
                       :auth-method :cli))
(build-uri *local-context* :resource-type :document :resource-id "doc123")
(build-uri *clever-dev-context* :resource-type :document :resource-id "doc123")
