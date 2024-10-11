(in-package :cosmos-db)

;; Basic utility functions

(defun trim-whitespace (string)
  "Remove leading and trailing whitespace from a string."
  (string-trim '(#\Space #\Newline #\Return) string))
(defun run-command (command)
  "Run a shell command and return its output as a trimmed string."
  (trim-whitespace (uiop:run-program command :output :string)))

(defun make-url (base-url &rest parts)
  "Construct a URL from a base URL and additional parts."
  (format nil "~A~{/~A~}" base-url parts))

;; Resource-specific functions

(defun cosmos-resource-url (account-name)
  "Construct the Azure Cosmos DB resource URL."
  (format nil "https://~A.documents.azure.com" account-name))

(defun azure-oauth-url (tenant-id)
  "Construct the Azure OAuth token URL."
  (make-url "https://login.microsoftonline.com" tenant-id "oauth2/token"))

;; CLI token retrieval

(defun get-cli-access-token (resource)
  "Get an access token using Azure CLI."
  (run-command 
   (format nil "az account get-access-token --resource ~A --query accessToken --output tsv" 
           resource)))

(define-condition cli-not-logged-in (error)
  ()
  (:report (lambda (condition stream)
             (declare (ignore condition))
             (format stream "Not logged into Azure CLI. Please run 'az login'."))))

;;todo make restart to show off condition restart login with az cli
;; download az cl :O!
(defun validate-cli-token (token)
  "Validate the token obtained from CLI."
  (cond
    ((string= token "")
     (error 'cli-not-logged-in))
    (t
     token)))

(defun get-access-token-from-cli (account-name)
  "Get an access token for Cosmos DB using Azure CLI."
  (restart-case
      (validate-cli-token
       (get-cli-access-token (cosmos-resource-url account-name)))
    (az-login ()
      :report "Run 'az login' and retry obtaining the access token."
      (run-command "az login")
      (get-access-token-from-cli account-name))))

;; OAuth token retrieval

(defun make-oauth-params (client-id client-secret resource)
  "Create the parameter list for OAuth token request."
  `(("grant_type" . "client_credentials")
    ("client_id" . ,client-id)
    ("client_secret" . ,client-secret)
    ("resource" . ,resource)))

(defun parse-oauth-response (response)
  "Parse the JSON response from OAuth token request."
  (let ((json-response (cl-json:decode-json-from-string response)))
    (cdr (assoc :access-token json-response))))

(defun get-access-token-from-oauth (tenant-id client-id client-secret account-name)
  "Get an access token for Cosmos DB using OAuth."
  (let* ((resource (cosmos-resource-url account-name))
         (url (azure-oauth-url tenant-id))
         (params (make-oauth-params client-id client-secret resource)))
    (-> (dex:post url :content params)
        parse-oauth-response)))

;; Connection string parsing

(defun split-connection-string (connection-string)
  "Split a connection string into endpoint and key, preserving the full key."
  (let* ((parts (uiop:split-string connection-string :separator ";"))
         (endpoint-part (first parts))
         (key-part (second parts)))
    (list (string-left-trim "AccountEndpoint=" endpoint-part)
          (string-left-trim "AccountKey=" key-part))))

(defun parse-connection-string (connection-string)
  "Parse a connection string into an alist of key-value pairs."
  (let ((parts (uiop:split-string connection-string :separator ";")))
    (list (cons "AccountEndpoint" (string-trim "AccountEndpoint=" (first parts)))
          (cons "AccountKey" (string-trim "AccountKey=" (second parts))))))

(defun get-access-token-from-connection-string (connection-string)
  "Extract the access token (AccountKey) from a connection string."
  (second (split-connection-string connection-string)))

(defun ad-auth-header (token)
  (format nil "type=aad&ver=1.0&sig=~A" token))

(defun resource-auth-header (token)
  (format nil "type=resource&ver=1.0&sig=~A" token))

(defun masterkey-auth-header (token)
  (format nil "type=master&ver=1.0&sig=~A" token))

(defun create-payload (verb resource-type resource-link date)
  "Create the payload string for hashing."
  (format nil "~a~c~a~c~a~c~a~c~c"
          (string-downcase verb) #\Newline
          (string-downcase resource-type) #\Newline
          resource-link #\Newline
          (string-downcase date) #\Newline
          #\Newline))

(defun hmac-sha256 (key message)
  "Compute HMAC-SHA256 of the message using the given key."
  (let ((hmac (ironclad:make-hmac (base64:base64-string-to-usb8-array key) :sha256)))
    (ironclad:update-hmac hmac (babel:string-to-octets message))
    (ironclad:hmac-digest hmac)))

(defun base64-encode (bytes)
  "Encode byte array to Base64 string."
  (base64:usb8-array-to-base64-string bytes))

(defun url-encode (string)
  "URL encode a string."
  (quri:url-encode string))

(defun generate-auth-signature (verb resource-type resource-link date key)
  "Generate the authorization signature for connection string method."
  (let* ((payload (create-payload verb resource-type resource-link date))
         (hmac (hmac-sha256 key payload)))
    (base64-encode hmac)))

(defun create-auth-token (key-type token-version signature)
  "Create the authorization token string."
  (format nil "type=~a&ver=~a&sig=~a" key-type token-version signature))

(defun get-access-token (context verb resource-type resource-link)
  "Get an access token based on the authentication method in the context."
  (ecase (cosmos-context-auth-method context)
    (:cli
     (let* ((bearer-token (get-access-token-from-cli (cosmos-context-account-name context)))
            (auth-header-value (create-auth-token "aad" "1.0" bearer-token)))
       auth-header-value))
    (:connection-string 
     (let* ((connection-string (cosmos-context-connection-string context))
            (key (second (split-connection-string connection-string)))
            (date (local-time:to-rfc1123-timestring (local-time:now)))
            (signature (generate-auth-signature verb resource-type resource-link date key))
            (auth-token (create-auth-token "master" "1.0" signature)))
       (url-encode auth-token)))
    (:oauth 
     (get-access-token-from-oauth
      (cosmos-context-tenant-id context)
      (cosmos-context-client-id context)
      (cosmos-context-client-secret context)
      (cosmos-context-account-name context)))))

(defun get-resource-type-string (resource-type)
  "Convert resource type keyword to string."
  (case resource-type
    (:databases "dbs")
    (:database "dbs")
    (:collections "colls")
    (:collection "colls")
    (:documents "docs")
    (:document "docs")
    (:stored-procedures "sprocs")
    (:user-defined-functions "udfs")
    (:triggers "triggers")
    (:users "users")
    (:permissions "permissions")
    (otherwise (error "Unknown resource type: ~A" resource-type))))

(defun get-resource-link (context resource-type resource-id)
  "Get the resource link based on the resource type and ID."
  (case resource-type
    (:databases "")
    (:database (format nil "dbs/~A" (or resource-id (cosmos-context-database-name context))))
    (:collections (format nil "dbs/~A" (cosmos-context-database-name context)))
    (:collection (format nil "dbs/~A/colls/~A" 
                         (cosmos-context-database-name context)
                         (or resource-id (cosmos-context-container-name context))))
    (:documents (format nil "dbs/~A/colls/~A" 
                        (cosmos-context-database-name context)
                        (cosmos-context-container-name context)))
    (:document (format nil "dbs/~A/colls/~A/docs/~A" 
                       (cosmos-context-database-name context)
                       (cosmos-context-container-name context)
                       resource-id))
    (otherwise (error "Unknown resource type: ~A" resource-type))))

(defun make-auth-headers (context verb resource-type resource-id)
  "Create the authorization headers for a Cosmos DB request."
  (let* ((resource-type-string (get-resource-type-string resource-type))
         (resource-link (get-resource-link context resource-type resource-id))
         (auth-header (get-access-token context 
                                        (string-downcase (string verb))
                                        resource-type-string
                                        resource-link)))
    auth-header))

