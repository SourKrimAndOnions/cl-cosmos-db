(in-package :cosmos-db)

;; (defun unescape-forward-slashes (json-string)
;;   (cl-ppcre:regex-replace-all "\\\\/" json-string "/"))

(defun perform-request (url headers &key (method :get) content)
  (multiple-value-bind (body status response-headers)
      (dex:request url :method method :headers headers :content (when content (cl-json:encode-json-to-string content)))
    (values body status response-headers (gethash +header-request-charge+ response-headers))))

;;;;;;;;;;;;;;
;; database ;;
;;;;;;;;;;;;;;
(defun create-database (context database-id &key offer-throughput
                                              consistency-level
                                              session-token
                                              activity-id)
  (let ((url (build-uri context :resource-type :databases))
        (headers (make-common-headers  :auth (make-auth-headers context :post :databases nil)
                                       :consistency-level consistency-level
                                       :session-token session-token
                                       :offer-throughput offer-throughput
                                       :activity-id activity-id)))
    (perform-request url headers :method :post :content `(("id" . ,database-id)))))

(defun list-databases (context &key max-item-count
                                 continuation-token
                                 consistency-level
                                 session-token
                                 activity-id)
  (let ((url (build-uri context :resource-type :databases))
        (headers (make-common-headers :auth (make-auth-headers context :get :databases nil)
                                      :consistency-level consistency-level
                                      :continuation continuation-token
                                      :session-token session-token
                                      :max-item-count max-item-count
                                      :activity-id activity-id)))
    (perform-request url headers :method :get)))

(defun get-database (context database-id &key consistency-level
                                           session-token
                                           activity-id)
  (let ((url (build-uri context :resource-type :database :resource-id database-id))
        (headers (make-common-headers :auth (make-auth-headers context :get :database database-id)
                                      :consistency-level consistency-level
                                      :session-token session-token
                                      :activity-id activity-id)))
    (perform-request url headers :method :get)))

(defun delete-database (context database-id &key consistency-level
                                              session-token
                                              activity-id)
  (let ((url (build-uri context :resource-type :database :resource-id database-id))
        (headers (make-common-headers :auth (make-auth-headers context :delete :database database-id)
                                      :consistency-level consistency-level
                                      :session-token session-token
                                      :activity-id activity-id)))
    (perform-request url headers :method :delete)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; collections / containers ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defun create-collection (context collection-id partition-key &key offer-throughput offer-autopilot
                                                                consistency-level session-token activity-id)
  (let ((url (build-uri context :resource-type :collections))
        (headers (make-common-headers :auth (make-auth-headers context :post :collections (cosmos-context-database-name context))
                                      :content-type +content-type-json+
                                      :consistency-level consistency-level
                                      :session-token session-token
                                      :offer-autopilot "{\"maxThroughput\": 4000}"
                                      :offer-throughput offer-throughput
                                      :activity-id activity-id)))
    (perform-request url headers :method :post :content `((id . ,collection-id)
                                                          (partition-key . ((paths . #(,partition-key))))))))

(defun list-collections (context &key consistency-level session-token activity-id)
  (let ((url (build-uri context :resource-type :collections))
        (headers (make-common-headers :auth (make-auth-headers context :get :collections (cosmos-context-database-name context))
                                      :consistency-level consistency-level
                                      :session-token session-token
                                      :activity-id activity-id)))
    (perform-request url headers :method :get)))

(defun get-collection (context collection-id &key consistency-level session-token activity-id)
  (let ((url (build-uri context :resource-type :collection :resource-id collection-id))
        (headers (make-common-headers :auth (make-auth-headers context :get :collection collection-id)
                                      :consistency-level consistency-level
                                      :session-token session-token
                                      :activity-id activity-id)))
    (perform-request url headers :method :get)))

(defun delete-collection (context collection-id &key consistency-level session-token activity-id)
  (let ((url (build-uri context :resource-type :collection :resource-id collection-id))
        (headers (make-common-headers :auth (make-auth-headers context :delete :collection collection-id)
                                      :consistency-level consistency-level
                                      :session-token session-token
                                      :activity-id activity-id)))
    (perform-request url headers :method :delete)))

(defun replace-collection (context collection-id collection-def &key consistency-level session-token activity-id)
  (let ((url (build-uri context :resource-type :collection :resource-id collection-id))
        (headers (make-common-headers :auth (make-auth-headers context :put :collection collection-id)
                                      :consistency-level consistency-level
                                      :session-token session-token
                                      :activity-id activity-id)))
    (perform-request url headers :method :put :content collection-def)))

;; Update document functions

(defun create-document (context document &key partition-key-value (is-upsert nil) 
                                           consistency-level session-token activity-id)
  (let ((url (build-uri context :resource-type :documents))
        (headers (make-common-headers :auth (make-auth-headers context :post :documents (cosmos-context-container-name context))
                                      :partition-key partition-key-value
                                      :consistency-level consistency-level
                                      :is-upsert is-upsert
                                      :session-token session-token
                                      :activity-id activity-id)))
    (perform-request url headers :method :post :content document)))

(defun get-document (context document-id &key partition-key consistency-level 
                                           session-token activity-id if-none-match)
  (let* ((url (build-uri context :resource-type :document :resource-id document-id))
         (headers (make-common-headers :auth (make-auth-headers context :get :document document-id)
                                       :partition-key partition-key
                                       :consistency-level consistency-level
                                       :session-token session-token
                                       :activity-id activity-id
                                       :if-none-match if-none-match)))
    (perform-request url headers)))

(defun query-documents (context query &key parameters partition-key (max-item-count 100)
                                        continuation-token enable-cross-partition
                                        consistency-level session-token activity-id)
  (let* ((url (build-uri context :resource-type :documents))
         (headers (make-common-headers :auth (make-auth-headers context :post :documents (cosmos-context-container-name context))
                                       :partition-key partition-key
                                       :content-type +content-type-query-json+
                                       :consistency-level consistency-level
                                       :continuation continuation-token
                                       :max-item-count max-item-count
                                       :enable-cross-partition enable-cross-partition
                                       :session-token session-token
                                       :activity-id activity-id))
         (content `(("query" . ,query)
                    ("parameters" . ,(construct-parameters-header-value parameters)))))
    (perform-request url headers :method :post :content content)))

(defun delete-document (context document-id &key partition-key consistency-level 
                                              session-token activity-id if-match)
  (let* ((url (build-uri context :resource-type :document :resource-id document-id))
         (headers (make-common-headers :auth (make-auth-headers context :delete :document document-id)
                                       :partition-key partition-key
                                       :consistency-level consistency-level
                                       :session-token session-token
                                       :activity-id activity-id
                                       :if-match if-match)))
    (perform-request url headers :method :delete)))

(defparameter *local-context*
  (make-cosmos-context
   :account-name "localhost"
   :database-name "Database"
   :container-name "Container"
   :auth-method :connection-string
   :connection-string "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="))

(defparameter *clever-dev-context*
  (make-cosmos-context :account-name "clmobappdev"
                       :database-name "MobileApp"
                       :container-name "ConsumptionSummary"
                       :auth-method :cli))
