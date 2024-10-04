(defpackage :cosmos-db/tests
  (:use :cl :fiveam)
  (:import-from :cosmos-db
                #:make-cosmos-context
                #:list-databases
                #:create-database
                #:delete-database
                #:create-collection
                #:list-collections
                #:delete-collection
                #:create-document
                #:query-documents
                #:get-document
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
   :container-name "TestCollection"
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

;; Builder method tests

(test test-with-account
  (let* ((original-context *local-context*)
         (new-context (cosmos-db::with-account original-context "new-account")))
    (is (string= (cosmos-db::cosmos-context-account-name new-context) "new-account"))
    (is (string= (cosmos-db::cosmos-context-database-name new-context) 
                 (cosmos-db::cosmos-context-database-name original-context)))
    (is (string= (cosmos-db::cosmos-context-container-name new-context) 
                 (cosmos-db::cosmos-context-container-name original-context)))
    (is (eq (cosmos-db::cosmos-context-auth-method new-context) 
            (cosmos-db::cosmos-context-auth-method original-context)))))

(test test-with-database
  (let* ((original-context *local-context*)
         (new-context (cosmos-db::with-database original-context "new-database")))
    (is (string= (cosmos-db::cosmos-context-account-name new-context) 
                 (cosmos-db::cosmos-context-account-name original-context)))
    (is (string= (cosmos-db::cosmos-context-database-name new-context) "new-database"))
    (is (null (cosmos-db::cosmos-context-container-name new-context)))
    (is (eq (cosmos-db::cosmos-context-auth-method new-context) 
            (cosmos-db::cosmos-context-auth-method original-context)))))

(test test-with-container
  (let* ((original-context *local-context*)
         (new-context (cosmos-db::with-container original-context "new-container")))
    (is (string= (cosmos-db::cosmos-context-account-name new-context) 
                 (cosmos-db::cosmos-context-account-name original-context)))
    (is (string= (cosmos-db::cosmos-context-database-name new-context) 
                 (cosmos-db::cosmos-context-database-name original-context)))
    (is (string= (cosmos-db::cosmos-context-container-name new-context) "new-container"))
    (is (eq (cosmos-db::cosmos-context-auth-method new-context) 
            (cosmos-db::cosmos-context-auth-method original-context)))))

(test test-with-connection-string-auth
  (let* ((original-context *local-context*)
         (new-connection-string "new-connection-string")
         (new-context (cosmos-db::with-connection-string-auth original-context new-connection-string)))
    (is (string= (cosmos-db::cosmos-context-account-name new-context) 
                 (cosmos-db::cosmos-context-account-name original-context)))
    (is (string= (cosmos-db::cosmos-context-database-name new-context) 
                 (cosmos-db::cosmos-context-database-name original-context)))
    (is (string= (cosmos-db::cosmos-context-container-name new-context) 
                 (cosmos-db::cosmos-context-container-name original-context)))
    (is (eq (cosmos-db::cosmos-context-auth-method new-context) :connection-string))
    (is (string= (cosmos-db::cosmos-context-connection-string new-context) new-connection-string))))

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
        (create-document *local-context* doc :partition-key-value "1")
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

(test test-query-documents
  (multiple-value-bind (body status headers request-charge)
      (query-documents *local-context* "SELECT * FROM c WHERE c.id = @id" :parameters '(("@id" . "1")))
    (is (stringp body))
    (is (= status 200))
    (is (hash-table-p headers))
    (is (stringp request-charge))
    (is (search "Test Document" body))))

(test test-delete-document
  (multiple-value-bind (body status headers request-charge)
      (delete-document *local-context* "1" :partition-key "1")
    (is (string= "" body))
    (is (= status 204))
    (is (hash-table-p headers))
    (is (stringp request-charge))))

(test test-delete-database
  (multiple-value-bind (body status headers request-charge)
      (delete-database *local-context* "TestDatabase")
    (is (string= "" body))
    (is (= status 204))
    (is (hash-table-p headers))
    (is (stringp request-charge))))

;;combined approach
(test test-create-database
  (let* ((original-context *local-context*)
         (database-id "TestDatabase"))
    ;; Create the database using the account context and database ID
    (multiple-value-bind (body status headers request-charge)
        (create-database original-context database-id)
      (is (stringp body))
      (is (= status 201))
      (is (hash-table-p headers))
      (is (stringp request-charge))
      (is (search database-id body)))
    ;; Verify the database was created
    (multiple-value-bind (body status headers request-charge)
        (list-databases original-context)
      (is (search database-id body)))
    ;; Clean up by deleting the database
    (multiple-value-bind (body status headers request-charge)
        (delete-database original-context database-id)
      (is (= status 204)))))

(test test-with-account-and-list-databases
  (let* ((original-context *local-context*)
         (account-context (cosmos-db::with-account original-context "test-account")))
    (multiple-value-bind (body status headers request-charge)
        (list-databases account-context)
      (is (string= "{\"_rid\":\"\",\"Databases\":[],\"_count\":0}" body))
      (is (= status 200))
      (is (hash-table-p headers))
      (is (stringp request-charge)))))


(test test-create-document-with-struct
  (let* ((original-context *local-context*)
         (database-id "TestDatabase")
         (collection-id "TestCollection")
         (document-id "1")
         (partition-key "1")
         ;; Create a user struct instance
         (user-doc (cosmos-db::make-user :id document-id :username "JohnDoe" :age 30)))
    ;; Create the database
    (multiple-value-bind (_body status _headers _request-charge)
        (create-database original-context database-id)
      (is (= status 201)))
    ;; Create a database context
    (let ((database-context (cosmos-db::with-database original-context database-id)))
      ;; Create the collection with the partition key path "/id"
      (multiple-value-bind (_body status _headers _request_charge)
          (create-collection database-context collection-id "/id")
        (is (= status 201)))
      ;; Create a collection context
      (let ((collection-context (cosmos-db::with-container database-context collection-id)))
        ;; Create the document using the struct
        (multiple-value-bind (body status _headers _request_charge)
            (create-document collection-context user-doc :partition-key-value partition-key)
          (is (= status 201))
          (is (stringp body))
          ;; Optionally check if the response body contains expected data
          (is (search "JohnDoe" body)))
        ;; Retrieve the document
        (multiple-value-bind (body status _headers _request_charge)
            (get-document collection-context document-id :partition-key partition-key)
          (is (= status 200))
          (is (stringp body))
          ;; Decode the JSON body back into a struct
          (let ((retrieved-doc (decode-json-to-struct body 'cosmos-db::user)))
            ;; Compare the original and retrieved structs
            (is (equalp user-doc retrieved-doc) "The retrieved document should match the original")))
        ;; Clean up by deleting the document
        (multiple-value-bind (_body status _headers _request_charge)
            (delete-document collection-context document-id :partition-key partition-key)
          (is (= status 204))))
      ;; Clean up by deleting the collection
      (multiple-value-bind (_body status _headers _request_charge)
          (delete-collection database-context collection-id)
        (is (= status 204))))
    ;; Clean up by deleting the database
    (multiple-value-bind (_body status _headers _request_charge)
        (delete-database original-context database-id)
      (is (= status 204)))))
;; validation

(test create-valid-user
  (let ((user (cosmos-db::make-user :username "validname" :age 5)))
    (is (not (null user)))
    (is (string= "validname" (cosmos-db::user-username user)))
    (is (= 5 (cosmos-db::user-age user)))))

(test invalid-user-signals-error
  "Test that creating an invalid user signals a validation-error with correct error messages"
  (signals cosmos-db::validation-error
    (cosmos-db::make-user :username "123@" :age 151)))


(test struct-encoding-decoding
  (let* ((user (cosmos-db::make-user :username "validname" :age 5))
         ;; Encode the user to JSON
         (json-string (cl-json:encode-json-to-string user))
         ;; Decode the JSON back to a user struct
         (decoded-user (decode-json-to-struct json-string 'cosmos-db::user)))
    ;; Ensure that the user object is not null
    (is (not (null user)) "User should not be null")
    ;; Check that the username is correct
    (is (string= "validname" (cosmos-db::user-username user))
        "Username should be 'validname'")
    ;; Check that the age is correct
    (is (= 5 (cosmos-db::user-age user)) "Age should be 5")
    ;; Verify that the JSON string is as expected
    (is (string= json-string "{\"username\":\"validname\",\"age\":5}")
        "JSON string should match expected output")
    ;; Verify that the decoded user matches the original
    (is (equalp user decoded-user) "Decoded user should equal the original user")))

