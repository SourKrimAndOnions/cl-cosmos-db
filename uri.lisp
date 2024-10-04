(in-package :cosmos-db)

(defun get-endpoint-from-connection-string (connection-string)
  (or (first (split-connection-string connection-string))
      (error "Invalid connection string: missing AccountEndpoint.")))

(defun get-localhost-uri ()
  "https://localhost:8081")

(defun get-azure-uri (account-name)
  (format nil "https://~A.documents.azure.com" account-name))

(defun build-base-uri (context)
  (let ((connection-string (cosmos-context-connection-string context))
        (account-name (cosmos-context-account-name context)))
    (cond
      (connection-string (get-endpoint-from-connection-string connection-string))
      ((and account-name (string= (string-downcase account-name) "localhost"))
       (get-localhost-uri))
      (account-name (get-azure-uri account-name))
      (t (error "Cannot determine base URI: missing connection string or account name.")))))

(defun build-resource-path (resource-type resource-id context)
  (flet ((format-path (fmt &rest args)
           (apply #'format nil fmt args)))
    (case resource-type
      (:databases "/dbs")
      (:database (format-path "/dbs/~A" (or resource-id (cosmos-context-database-name context))))
      (:collections (format-path "/dbs/~A/colls" (cosmos-context-database-name context)))
      (:collection (format-path "/dbs/~A/colls/~A" 
                                (cosmos-context-database-name context)
                                (or resource-id (cosmos-context-container-name context))))
      (:documents (format-path "/dbs/~A/colls/~A/docs" 
                               (cosmos-context-database-name context)
                               (cosmos-context-container-name context)))
      (:document (format-path "/dbs/~A/colls/~A/docs/~A" 
                              (cosmos-context-database-name context)
                              (cosmos-context-container-name context)
                              resource-id))
      (otherwise (error "Unknown resource type: ~A" resource-type)))))

(defun build-uri (context &key resource-type resource-id)
  (format nil "~A~A" 
          (build-base-uri context)
          (build-resource-path resource-type resource-id context)))