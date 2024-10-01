(in-package :cosmos-db)

(defun url-encode (str)
  "Encodes STR using URL encoding."
  (with-output-to-string (out)
    (map nil
         (lambda (c)
           (cond
             ;; Unreserved characters: ALPHA / DIGIT / "-" / "." / "_" / "~"
             ((or (and (char>= c #\a) (char<= c #\z))
                  (and (char>= c #\A) (char<= c #\Z))
                  (and (char>= c #\0) (char<= c #\9))
                  (member c '(#\- #\_ #\. #\~)))
              (write-char c out))
             (t
              (format out "%%~2,'0X" (char-code c)))))
         str)))

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

(defun generate-master-key-authorization-signature (verb resource-type resource-link date key)
  (let* ((key-type "master")
         (token-version "1.0")
         ;; Construct the payload
         (payload (format nil "~(~a~)~c~(~a~)~c~a~c~(~a~)~c~c"
                          verb #\Newline
                          resource-type #\Newline
                          resource-link #\Newline
                          date #\Newline
                          #\Newline))
         ;; Decode the key from Base64
         (decoded-key (ironclad:base64-string-to-byte-array key))
         ;; Compute HMAC-SHA256 hash of the payload
         (hmac (ironclad:hmac-hash 'ironclad:sha256 decoded-key
                                   (babel:string-to-octets payload :encoding :utf-8)))
         ;; Encode the signature in Base64
         (signature (ironclad:byte-array-to-base64-string hmac))
         ;; Construct the authorization header value
         (auth-string (format nil "type=~a&ver=~a&sig=~a" key-type token-version signature))
         ;; URL encode the authorization string
         (url-encoded-auth-string (url-encode auth-string :encode-slash nil)))
    url-encoded-auth-string))


(defun get-access-token (context)
  (ecase (cosmos-context-auth-method context)
    (:cli (get-access-token-from-cli (cosmos-context-account-name context)))
    (:connection-string (second (split-connection-string (cosmos-context-connection-string context))))
    (:oauth (get-access-token-from-oauth
             (cosmos-context-tenant-id context)
             (cosmos-context-client-id context)
             (cosmos-context-client-secret context)
             (cosmos-context-account-name context)))))
;;AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
(defun split-connection-string (connection-string)
  (destructuring-bind (endpoint-string key)
      (uiop:split-string connection-string :separator ";")
    (list (string-left-trim "AccountEndpoint=" endpoint-string)
          (string-left-trim "AccountKey=" key))))