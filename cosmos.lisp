(in-package :cosmos-db)

;; Define constants for header names
(defconstant +header-is-query+ "x-ms-documentdb-isquery")
(defconstant +header-content-type+ "Content-Type")
(defconstant +header-max-item-count+ "x-ms-max-item-count")
(defconstant +header-continuation+ "x-ms-continuation")
(defconstant +header-enable-cross-partition+ "x-ms-documentdb-query-enablecrosspartition")
(defconstant +header-consistency-level+ "x-ms-consistency-level")
(defconstant +header-session-token+ "x-ms-session-token")
(defconstant +header-partition-key+ "x-ms-documentdb-partitionkey")
(defconstant +header-authorization+ "Authorization")
(defconstant +header-version+ "x-ms-version")

;; Define constants for content types
(defconstant +content-type-query-json+ "application/query+json")
(defconstant +content-type-json+ "application/json")
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
    (cdr (assoc :access--token json-response))))

(defun get-access-token (context)
  (ecase (cosmos-context-auth-method context)
    (:cli (get-access-token-from-cli (cosmos-context-account-name context)))
    (:oauth (get-access-token-from-oauth
             (cosmos-context-tenant-id context)
             (cosmos-context-client-id context)
             (cosmos-context-client-secret context)
             (cosmos-context-account-name context)))))

(defun build-uri (context &optional (resource "docs"))
  (format nil "https://~A.documents.azure.com/dbs/~A/colls/~A/~A"
          (cosmos-context-account-name context)
          (cosmos-context-database-name context)
          (cosmos-context-container-name context)
          resource))

(defun build-headers (context access-token &key partition-key additional-headers)
  (remove nil
          (append
           `(("Authorization" . ,(format nil "type=aad&ver=1.0&sig=~A" access-token))
             ("x-ms-version" . "2018-12-31"))
           (when partition-key
             `(("x-ms-documentdb-partitionkey" . ,(format nil "[~S]" partition-key))))
           additional-headers)))

(defmacro with-cosmos-context ((context) &body body)
  `(let ((access-token (get-access-token ,context)))
     (flet ((perform-request (url headers &key (method :get) content)
              (dex:request url
                           :method method
                           :headers headers
                           :content (when content (cl-json:encode-json-to-string content)))))
       ,@body)))

(defun query-documents (context query &key parameters partition-key)
  (with-cosmos-context (context)
    (let ((url (build-uri context))
          (headers (build-headers context access-token
                                  :partition-key partition-key
                                  :additional-headers '(("x-ms-documentdb-isquery" . "true")
                                                        ("Content-Type" . "application/query+json"))))
          (content `(("query" . ,query)
                     ("parameters" . ,(unless (loop for (name . value) in parameters
                                                    collect `(("name" . ,name)
                                                              ("value" . ,value)))
                                        #())))))
      (perform-request url headers :method :post :content content))))

(defun create-document (context document &key partition-key (is-upsert nil) (indexing-directive nil))
  (with-cosmos-context (context)
    (let* ((url (build-uri context))
           (additional-headers
             (remove nil
                     (list
                      (when is-upsert
                        (cons "x-ms-documentdb-is-upsert" (if is-upsert "true" "false")))
                      (when indexing-directive
                        (cons "x-ms-indexing-directive" (string-downcase (symbol-name indexing-directive)))))))
           (headers (build-headers context access-token
                                   :partition-key partition-key
                                   :additional-headers additional-headers)))
      (perform-request url headers :method :post :content document))))

(defun get-document (context document-id &key partition-key)
  (with-cosmos-context (context)
    (let* ((url (build-uri context (format nil "docs/~A" document-id)))
           (headers (build-headers context access-token
                                   :partition-key partition-key
                                   :additional-headers '(("Content-Type" . "application/query+json")))))
      (cl-json:decode-json-from-string (perform-request url headers)))))

(defun delete-document (context document-id &key partition-key)
  (with-cosmos-context (context)
    (let* ((url (build-uri context (format nil "docs/~A" document-id)))
           (headers (build-headers context access-token :partition-key partition-key)))
      (perform-request url headers :method :delete))))

Usage examples
(defun example-usage ()
  (let ((client (make-cosmos-context :account-name "clmobappdev"
                                     :database-name "MobileApp"
                                     :container-name "ConsumptionSummary"
                                     :auth-method :cli)))
    ;; Query documents
    (query-documents client "SELECT * FROM c WHERE c.id = @id"
                     :parameters '(("@id" . "BCL-9000641-2022-09-01"))
                     :partition-key "5002534")
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
    ))

