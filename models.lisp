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

