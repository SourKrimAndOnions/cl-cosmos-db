(in-package :cosmos-db)

(defun example-composed-operation ()
  (let ((ops (compose-cosmos-ops
              (with-account "clmobappdev")
              (with-database "MobileApp")
              (with-container "Invoices")
              (query-documents "SELECT * FROM c" :enable-cross-partition t :max-item-count 1)
              (with-container "CDR")
              (query-documents "SELECT * FROM c" :enable-cross-partition t :max-item-count 1))))
    (funcall ops (make-cosmos-context))))
;; ;; Run the example
(multiple-value-bind (body status-code response-headers final-context results) (example-composed-operation)
  (format t "Final context: ~A~%" final-context)
  (format t "Status code: ~A~%" status-code)
  (format t "Response headers: ~A~%" response-headers)
  (format t "Body: ~A~%" (json:decode-json-from-string body)))


(defparameter *local-context*
  (make-cosmos-context
   :account-name "localhost"
   :database-name "Database"
   :container-name "Container"
   :auth-method :connection-string
   :connection-string "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="))