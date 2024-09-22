(in-package :cosmos-db)
;;common headers
(defconstant +header-authorization+ "Authorization")
(defconstant +header-content-type+ "Content-Type")
(defconstant +header-if-match+ "If-Match")
(defconstant +header-if-none-match+ "If-None-Match")
(defconstant +header-if-modified-since+ "If-Modified-Since")
(defconstant +header-user-agent+ "User-Agent")
(defconstant +header-activity-id+ "x-ms-activity-id")
(defconstant +header-consistency-level+ "x-ms-consistency-level")
(defconstant +header-continuation+ "x-ms-continuation")
(defconstant +header-date+ "x-ms-date")
(defconstant +header-max-item-count+ "x-ms-max-item-count")
(defconstant +header-partition-key+ "x-ms-documentdb-partitionkey")
(defconstant +header-enable-cross-partition+ "x-ms-documentdb-query-enablecrosspartition")
(defconstant +header-session-token+ "x-ms-session-token")
(defconstant +header-version+ "x-ms-version")
(defconstant +header-a-im+ "A-IM")
(defconstant +header-partition-key-range-id+ "x-ms-documentdb-partitionkeyrangeid")
(defconstant +header-allow-tentative-writes+ "x-ms-cosmos-allow-tentative-writes")
;; Define constants for header names
(defconstant +header-is-query+ "x-ms-documentdb-isquery")

;; Define constants for content types
(defconstant +content-type-query-json+ "application/query+json")
(defconstant +content-type-json+ "application/json")
;; create document header
(defconstant +header-is-upsert+ "x-ms-documentdb-is-upsert")
(defconstant +header-indexing-directive+ "x-ms-indexing-directive")
;;delete document header
(defconstant +header-request-charge+ "x-ms-request-charge")
;;collections / containers
(defconstant +header-offer-throughput+ "x-ms-offer-throughput")
(defconstant +header-offer-autopilot+ "x-ms-cosmos-offer-autopilot-settings")
(defstruct cosmos-context
  account-name
  database-name
  container-name
  (auth-method :cli)
  tenant-id
  client-id
  client-secret)
(defun get-access-token-from-cli (account-name)
  (let* ((resource (format nil "https://~A.documents.azure.com" account-name))
         (command (format nil "az account get-access-token --resource ~A --query accessToken --output tsv" resource))
         (token (string-trim '(#\Space #\Newline #\Return) (uiop:run-program command :output :string))))
    (if (string= token "")
        (error "Failed to get token from Azure CLI. Make sure you're logged in with 'az login'.")
        token)))

(defun get-access-token-from-oauth (tenant-id client-id client-secret account-name)
  (let* ((resource (format nil "https://~A.documents.azure.com" account-name))
         (url (format nil "https://login.microsoftonline.com/~A/oauth2/token" tenant-id))
         (params `(("grant_type" . "client_credentials")
                   ("client_id" . ,client-id)
                   ("client_secret" . ,client-secret)
                   ("resource" . ,resource)))
         (response (dex:post url :content params))
         (json-response (cl-json:decode-json-from-string response)))
    (cdr (assoc :access-token json-response))))

(defun get-access-token (context)
  (ecase (cosmos-context-auth-method context)
    (:cli (get-access-token-from-cli (cosmos-context-account-name context)))
    (:oauth (get-access-token-from-oauth
             (cosmos-context-tenant-id context)
             (cosmos-context-client-id context)
             (cosmos-context-client-secret context)
             (cosmos-context-account-name context)))))

(defun build-uri (context &key resource-type resource-id)
  (let ((base-url (format nil "https://~A.documents.azure.com"
                          (cosmos-context-account-name context))))
    (cond
      ((eq resource-type :database)
       (format nil "~A/dbs/~A" base-url (cosmos-context-database-name context)))
      ((eq resource-type :collections)
       (format nil "~A/dbs/~A/colls" base-url (cosmos-context-database-name context)))
      ((eq resource-type :collection)
       (format nil "~A/dbs/~A/colls/~A" 
               base-url 
               (cosmos-context-database-name context)
               (or resource-id (cosmos-context-container-name context))))
      ((eq resource-type :documents)
       (format nil "~A/dbs/~A/colls/~A/docs" 
               base-url 
               (cosmos-context-database-name context)
               (cosmos-context-container-name context)))
      ((eq resource-type :document)
       (format nil "~A/dbs/~A/colls/~A/docs/~A" 
               base-url 
               (cosmos-context-database-name context)
               (cosmos-context-container-name context)
               resource-id))
      (t (error "Unknown resource type: ~A" resource-type)))))

(defun make-common-headers (access-token &key consistency-level continuation max-item-count
                                           partition-key enable-cross-partition session-token
                                           if-match if-none-match if-modified-since
                                           activity-id allow-tentative-writes)
  (remove nil
          `((,+header-authorization+ . ,(format nil "type=aad&ver=1.0&sig=~A" access-token))
            (,+header-version+ . "2018-12-31")
            (,+header-date+ . ,(local-time:format-rfc1123-timestring nil (local-time:now)))
            ,@(when consistency-level
                `((,+header-consistency-level+ . ,(string-downcase (symbol-name consistency-level)))))
            ,@(when continuation
                `((,+header-continuation+ . ,continuation)))
            ,@(when max-item-count
                `((,+header-max-item-count+ . ,(write-to-string max-item-count))))
            ,@(when partition-key
                `((,+header-partition-key+ . ,(format nil "[~S]" partition-key))))
            ,@(when enable-cross-partition
                `((,+header-enable-cross-partition+ . "true")))
            ,@(when session-token
                `((,+header-session-token+ . ,session-token)))
            ,@(when if-match
                `((,+header-if-match+ . ,if-match)))
            ,@(when if-none-match
                `((,+header-if-none-match+ . ,if-none-match)))
            ,@(when if-modified-since
                `((,+header-if-modified-since+ . ,if-modified-since)))
            ,@(when activity-id
                `((,+header-activity-id+ . ,activity-id)))
            ,@(when allow-tentative-writes
                `((,+header-allow-tentative-writes+ . "true"))))))

(defun make-query-headers (&key partition-key max-item-count continuation-token
                             enable-cross-partition consistency-level session-token)
  (remove nil
          `((,+header-is-query+ . "true")
            (,+header-content-type+ . ,+content-type-query-json+)
            ,@(when partition-key
                `((,+header-partition-key+ . ,(format nil "[~S]" partition-key))))
            ,@(when max-item-count
                `((,+header-max-item-count+ . ,(write-to-string max-item-count))))
            ,@(when continuation-token
                `((,+header-continuation+ . ,continuation-token)))
            ,@(when enable-cross-partition
                `((,+header-enable-cross-partition+ . "true")))
            ,@(when consistency-level
                `((,+header-consistency-level+ . ,(string-downcase (symbol-name consistency-level)))))
            ,@(when session-token
                `((,+header-session-token+ . ,session-token))))))

(defmacro with-cosmos-context ((context) &body body)
  `(let ((access-token (get-access-token ,context)))
     (flet ((perform-request (url headers &key (method :get) content)
              (dex:request url
                           :method method
                           :headers headers
                           :content (when content (cl-json:encode-json-to-string content)))))
       ,@body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; collections / containers ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defun create-collection (context collection-def &key offer-throughput offer-autopilot
                                                   consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context "colls"))
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
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :post :content collection-def)
        (let ((json-body (cl-json:decode-json-from-string body))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

(defun list-collections (context &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context "colls"))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :get)
        (let ((json-body (cl-json:decode-json-from-string body))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

(defun get-collection (context collection-id &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context (format nil "colls/~A" collection-id)))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :get)
        (let ((json-body (cl-json:decode-json-from-string body))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

(defun delete-collection (context collection-id &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context (format nil "colls/~A" collection-id)))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :delete)
        (let ((json-body (when body (cl-json:decode-json-from-string body)))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

(defun replace-collection (context collection-id collection-def &key consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context (format nil "colls/~A" collection-id)))
           (headers (make-common-headers access-token
                                         :consistency-level consistency-level
                                         :session-token session-token
                                         :activity-id activity-id)))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :put :content collection-def)
        (let ((json-body (cl-json:decode-json-from-string body))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

;;;;;;;;;;;;;;;
;; documents ;;
;;;;;;;;;;;;;;;

(defun query-documents (context query &key parameters partition-key
                                        (max-item-count 100)
                                        continuation-token
                                        enable-cross-partition
                                        consistency-level
                                        session-token
                                        activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context))
           (common-headers (make-common-headers access-token
                                                :consistency-level consistency-level
                                                :continuation continuation-token
                                                :max-item-count max-item-count
                                                :partition-key partition-key
                                                :enable-cross-partition enable-cross-partition
                                                :session-token session-token
                                                :activity-id activity-id))
           (query-specific-headers `((,+header-content-type+ . "application/query+json")))
           (headers (append common-headers query-specific-headers))
           (content `(("query" . ,query)
                      ("parameters" . ,(unless (loop for (name . value) in parameters
                                                     collect `(("name" . ,name)
                                                               ("value" . ,value)))
                                         #())))))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :post :content content)
        (let* ((json-body (cl-json:decode-json-from-string body))
               (new-continuation-token (gethash "x-ms-continuation" response-headers)))
          (list json-body new-continuation-token status response-headers))))))

(defun create-document (context document &key partition-key (is-upsert nil) (indexing-directive nil)
                                           consistency-level session-token activity-id)
  (with-cosmos-context (context)
    (let* ((url (build-uri context))
           (common-headers (make-common-headers access-token
                                                :partition-key partition-key
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id))
           (create-specific-headers
             `((,+header-content-type+ . "application/json")
               ,@(when is-upsert `((,+header-is-upsert+ . ,"true")))
               ,@(when indexing-directive
                   `((,+header-indexing-directive+ . ,(string-downcase (symbol-name indexing-directive)))))))
           (headers (append common-headers create-specific-headers)))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :post :content document)
        (let ((json-body (cl-json:decode-json-from-string body))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

(defun get-document (context document-id &key partition-key consistency-level 
                                           session-token activity-id if-none-match)
  (with-cosmos-context (context)
    (let* ((url (build-uri context (format nil "docs/~A" document-id)))
           (common-headers (make-common-headers access-token
                                                :partition-key partition-key
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id
                                                :if-none-match if-none-match))
           (get-specific-headers
             `((,+header-content-type+ . "application/json")))
           (headers (append common-headers get-specific-headers)))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers)
        (let ((json-body (cl-json:decode-json-from-string body))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

(defun delete-document (context document-id &key partition-key consistency-level 
                                              session-token activity-id if-match)
  (with-cosmos-context (context)
    (let* ((url (build-uri context (format nil "docs/~A" document-id)))
           (common-headers (make-common-headers access-token
                                                :partition-key partition-key
                                                :consistency-level consistency-level
                                                :session-token session-token
                                                :activity-id activity-id
                                                :if-match if-match))
           (headers common-headers))
      (multiple-value-bind (body status response-headers)
          (perform-request url headers :method :delete)
        (let ((json-body (when body (cl-json:decode-json-from-string body)))
              (request-charge (gethash +header-request-charge+ response-headers)))
          (list json-body status response-headers request-charge))))))

                                        ;Usage examples
(defun example-usage ()
  (let ((client (make-cosmos-context :account-name "clmobappdev"
                                     :database-name "MobileApp"
                                     :container-name "ConsumptionSummary"
                                     :auth-method :cli)))
    ;; Query documents
    ;; (query-documents client "SELECT * FROM c"
    ;;                  :parameters nil
    ;;                  :partition-key "5002534")
    ;; (query-documents client "SELECT * FROM c WHERE c.wh = @wh"
    ;;                  :parameters '(("@wh" . 19902))
    ;;                  :partition-key "5002534")

    ;; Create a document
    ;; (create-document context '(("id" . "123") ("name" . "John Doe"))
    ;;                  :partition-key "your-partition-key"
    ;;                  :is-upsert t)

    ;; ;; Get a document
    ;; (get-document client "BCL-9000641-2022-09-01" :partition-key "5002534")

    ;; ;; Delete a document
    ;; (delete-document context "123" :partition-key "your-partition-key")

    (list-collections client)
    ))

