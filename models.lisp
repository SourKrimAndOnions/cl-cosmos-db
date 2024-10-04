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

(defclass partition-key ()
  ((paths :initarg :paths :reader paths :type array :documentation "path of the partitionKey")))

;; Context builders
(defun with-account (context account-name)
  (make-cosmos-context
   :account-name account-name
   :database-name (cosmos-context-database-name context)
   :container-name (cosmos-context-container-name context)
   :auth-method (cosmos-context-auth-method context)
   :tenant-id (cosmos-context-tenant-id context)
   :client-id (cosmos-context-client-id context)
   :client-secret (cosmos-context-client-secret context)
   :connection-string (cosmos-context-connection-string context)))

(defun with-database (context database-name)
  (make-cosmos-context
   :account-name (cosmos-context-account-name context)
   :database-name database-name
   :container-name nil  ; Reset container when changing database
   :auth-method (cosmos-context-auth-method context)
   :tenant-id (cosmos-context-tenant-id context)
   :client-id (cosmos-context-client-id context)
   :client-secret (cosmos-context-client-secret context)
   :connection-string (cosmos-context-connection-string context)))

(defun with-container (context container-name)
  (make-cosmos-context
   :account-name (cosmos-context-account-name context)
   :database-name (cosmos-context-database-name context)
   :container-name container-name
   :auth-method (cosmos-context-auth-method context)
   :tenant-id (cosmos-context-tenant-id context)
   :client-id (cosmos-context-client-id context)
   :client-secret (cosmos-context-client-secret context)
   :connection-string (cosmos-context-connection-string context)))

;; Auth methods
(defun with-cli-auth (context)
  (make-cosmos-context
   :account-name (cosmos-context-account-name context)
   :database-name (cosmos-context-database-name context)
   :container-name (cosmos-context-container-name context)
   :auth-method :cli))

(defun with-connection-string-auth (context connection-string)
  (make-cosmos-context
   :account-name (cosmos-context-account-name context)
   :database-name (cosmos-context-database-name context)
   :container-name (cosmos-context-container-name context)
   :auth-method :connection-string
   :connection-string connection-string))

(defun with-service-principal-auth (context tenant-id client-id client-secret)
  (make-cosmos-context
   :account-name (cosmos-context-account-name context)
   :database-name (cosmos-context-database-name context)
   :container-name (cosmos-context-container-name context)
   :auth-method :service-principal
   :tenant-id tenant-id
   :client-id client-id
   :client-secret client-secret))
