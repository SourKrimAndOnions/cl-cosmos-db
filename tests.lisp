(defpackage :cosmos-db/tests
  (:use :cl :fiveam)
  (:import-from :cosmos-db
                #:make-cosmos-context
                #:list-databases
                #:create-database
                #:delete-database
                #:create-collection
                #:list-collections
                #:create-document
                #:query-documents
                #:delete-document)
  (:export #:run-tests))

(in-package :cosmos-db/tests)

(def-suite :cosmos-db-test-suite  
  :description "Test suite for Cosmos DB functions including internal ones")

(in-suite :cosmos-db-test-suite)

;; Define your test context
(defparameter *local-context*
  (make-cosmos-context
   :account-name "localhost"
   :database-name "TestDatabase"
   :container-name "TestContainer"
   :auth-method :connection-string
   :connection-string "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="))

(test |escapes slashes correctly|
      (is (string= (cosmos-db::unescape-forward-slashes "{\"path\":\"\\/id\"}")
                   "{\"path\":\"/id\"}")))

(test |generates correct resource path|
  (is (string= (cosmos-db::get-resource-link *local-context* :databases nil)
               "")))

(test |constructs correct uri|
      (let ((uri (cosmos-db::build-uri *local-context* :resource-type :databases)))
        (is (string= uri "https://localhost:8081/dbs"))))

(test test-list-databases
  (multiple-value-bind (body status headers request-charge)
      (list-databases *local-context*)
    (is (stringp body))
    (is (= status 200))
    (is (hash-table-p headers))
    (is (stringp request-charge))
    (is (search "Database" body))))



(test test-create-database
  (multiple-value-bind (body status headers request-charge)
      (create-database *local-context* "TestDatabase")
    (is (stringp body))
    (is (= status 201))
    (is (hash-table-p headers))
    (is (stringp request-charge))
    (is (search "TestDatabase" body))))

(test test-delete-database
  (multiple-value-bind (body status headers request-charge)
      (delete-database *local-context* "TestDatabase")
    (is (null body))
    (is (= status 204))
    (is (hash-table-p headers))
    (is (stringp request-charge))))

(test test-create-collection
  (multiple-value-bind (body status headers request-charge)
      (create-collection *local-context* "TestCollection" "/id")
    (is (stringp body))
    (is (= status 201))
    (is (hash-table-p headers))
    (is (stringp request-charge))
    (is (search "TestCollection" body))))

(test test-list-collections
  (multiple-value-bind (body status headers request-charge)
      (list-collections *local-context*)
    (is (stringp body))
    (is (= status 200))
    (is (hash-table-p headers))
    (is (stringp request-charge))
    (is (search "Collection" body))))

(test test-create-document
  (let ((doc '(("id" . "1") ("name" . "Test Document"))))
    (multiple-value-bind (body status headers request-charge)
        (create-document *local-context* doc)
      (is (stringp body))
      (is (= status 201))
      (is (hash-table-p headers))
      (is (stringp request-charge))
      (is (search "Test Document" body)))))

(test test-query-documents
  (multiple-value-bind (body status headers request-charge)
      (query-documents *local-context* "SELECT * FROM c WHERE c.id = '1'")
    (is (stringp body))
    (is (= status 200))
    (is (hash-table-p headers))
    (is (stringp request-charge))
    (is (search "Test Document" body))))

(test test-delete-document
  (multiple-value-bind (body status headers request-charge)
      (delete-document *local-context* "1" "1")
    (is (null body))
    (is (= status 204))
    (is (hash-table-p headers))
    (is (stringp request-charge))))

;; Builder method tests

(test test-with-account
  (let* ((original-context *local-context*)
         (new-context (with-account original-context "new-account")))
    (is (string= (cosmos-db:cosmos-context-account-name new-context) "new-account"))
    (is (string= (cosmos-db:cosmos-context-database-name new-context) 
                 (cosmos-db:cosmos-context-database-name original-context)))
    (is (string= (cosmos-db:cosmos-context-container-name new-context) 
                 (cosmos-db:cosmos-context-container-name original-context)))
    (is (eq (cosmos-db:cosmos-context-auth-method new-context) 
            (cosmos-db:cosmos-context-auth-method original-context)))))

(test test-with-database
  (let* ((original-context *local-context*)
         (new-context (with-database original-context "new-database")))
    (is (string= (cosmos-db:cosmos-context-account-name new-context) 
                 (cosmos-db:cosmos-context-account-name original-context)))
    (is (string= (cosmos-db:cosmos-context-database-name new-context) "new-database"))
    (is (null (cosmos-db:cosmos-context-container-name new-context)))
    (is (eq (cosmos-db:cosmos-context-auth-method new-context) 
            (cosmos-db:cosmos-context-auth-method original-context)))))

(test test-with-container
  (let* ((original-context *local-context*)
         (new-context (with-container original-context "new-container")))
    (is (string= (cosmos-db:cosmos-context-account-name new-context) 
                 (cosmos-db:cosmos-context-account-name original-context)))
    (is (string= (cosmos-db:cosmos-context-database-name new-context) 
                 (cosmos-db:cosmos-context-database-name original-context)))
    (is (string= (cosmos-db:cosmos-context-container-name new-context) "new-container"))
    (is (eq (cosmos-db:cosmos-context-auth-method new-context) 
            (cosmos-db:cosmos-context-auth-method original-context)))))

(test test-with-connection-string-auth
  (let* ((original-context *local-context*)
         (new-connection-string "new-connection-string")
         (new-context (with-connection-string-auth original-context new-connection-string)))
    (is (string= (cosmos-db:cosmos-context-account-name new-context) 
                 (cosmos-db:cosmos-context-account-name original-context)))
    (is (string= (cosmos-db:cosmos-context-database-name new-context) 
                 (cosmos-db:cosmos-context-database-name original-context)))
    (is (string= (cosmos-db:cosmos-context-container-name new-context) 
                 (cosmos-db:cosmos-context-container-name original-context)))
    (is (eq (cosmos-db:cosmos-context-auth-method new-context) :connection-string))
    (is (string= (cosmos-db:cosmos-context-connection-string new-context) new-connection-string))))
