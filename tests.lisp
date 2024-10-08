(defpackage :cosmos-db/tests
  (:use :cl :fiveam)
  (:import-from :cosmos-db
                #:make-cosmos-context
                #:->>
                #:begin-cosmos-operations
                #:with-account
                #:with-database
                #:with-container
                #:create-database
                #:list-databases
                #:delete-database
                #:create-collection
                #:list-collections
                #:delete-collection
                #:create-document
                #:query-documents
                #:get-document
                #:delete-document
                #:get-results
                #:decode-json-to-struct)
  (:export #:run-tests))

(in-package :cosmos-db/tests)

(def-suite :cosmos-db-test-suite
  :description "Test suite for Cosmos DB functions using the new composable syntax")

(in-suite :cosmos-db-test-suite)

;; Define your test context
(defparameter *local-context*
  (make-cosmos-context
   :account-name "localhost"
   :database-name "TestDatabase"
   :container-name "TestCollection"
   :auth-method :connection-string
   :connection-string "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="))

(test test-database-operations
  (let ((results (->> *local-context*
                      begin-cosmos-operations
                      (create-database "TestDatabase" :offer-throughput 400)
                      (list-databases)
                      (delete-database "TestDatabase")
                      get-results)))
    (is (= (length results) 3) "Should have three results: create, list, and delete")
    (is (= (getf (first results) :status) 201) "Create database should return status 201")
    (is (= (getf (second results) :status) 200) "List databases should return status 200")
    (is (= (getf (third results) :status) 204) "Delete database should return status 204")))

(test test-collection-operations
  (let ((results (->> *local-context*
                      begin-cosmos-operations
                      (create-database "TestDatabase" :offer-throughput 400)
                      (create-collection "TestCollection" "/id" :offer-throughput 400)
                      (list-collections)
                      (delete-collection "TestCollection")
                      (delete-database "TestDatabase")
                      get-results)))
    (is (= (length results) 5) "Should have five results: create db, create collection, list collections, delete collection, delete db")
    (is (= (getf (first results) :status) 201) "Create database should return status 201")
    (is (= (getf (second results) :status) 201) "Create collection should return status 201")
    (is (= (getf (third results) :status) 200) "List collections should return status 200")
    (is (= (getf (fourth results) :status) 204) "Delete collection should return status 204")
    (is (= (getf (fifth results) :status) 204) "Delete database should return status 204")))

(test test-document-operations
  (let* ((doc '(("id" . "1") ("name" . "Test Document")))
         (results (->> *local-context*
                       begin-cosmos-operations
                       (create-database "TestDatabase" :offer-throughput 400)
                       (create-collection "TestCollection" "/id" :offer-throughput 400)
                       (create-document doc :partition-key-value "1")
                       (get-document "1" :partition-key "1")
                       (query-documents "SELECT * FROM c WHERE c.id = @id"
                                        :parameters '(("@id" . "1"))
                                        :partition-key "1")
                       (delete-document "1" :partition-key "1")
                       (delete-collection "TestCollection")
                       (delete-database "TestDatabase")
                       get-results)))
    (is (= (length results) 8) "Should have eight results")
    (is (= (getf (first results) :status) 201) "Create database should return status 201")
    (is (= (getf (second results) :status) 201) "Create collection should return status 201")
    (is (= (getf (third results) :status) 201) "Create document should return status 201")
    (is (= (getf (fourth results) :status) 200) "Get document should return status 200")
    (is (= (getf (fifth results) :status) 200) "Query documents should return status 200")
    (is (= (getf (sixth results) :status) 204) "Delete document should return status 204")
    (is (= (getf (seventh results) :status) 204) "Delete collection should return status 204")
    (is (= (getf (eighth results) :status) 204) "Delete database should return status 204")
    (let* ((created-doc (cl-json:decode-json-from-string (getf (third results) :body)))
           (query-result (cl-json:decode-json-from-string (getf (fifth results) :body)))
           (query-documents (cdr (assoc :*documents query-result))))
      (is (string= "1" (cdr (assoc :id created-doc))) "Created document should have correct id")
      (is (= (length query-documents) 1) "Query should return one document")
      (is (string= (cdr (assoc :id (first query-documents))) "1") "Queried document should have correct id"))))

(test test-context-modifications
  (let* ((initial-context *local-context*)
         (results (->> initial-context
                       begin-cosmos-operations
                       (with-account "new-account")
                       (with-database "new-database")
                       (with-container "new-container"))))
    (is (= (length (car results)) 0) "Context modifications should not produce results")
    (let ((modified-context (cdr results)))
      (is (string= (cosmos-db::cosmos-context-account-name initial-context) "localhost"))
      (is (string= (cosmos-db::cosmos-context-database-name initial-context) "TestDatabase"))
      (is (string= (cosmos-db::cosmos-context-container-name initial-context) "TestCollection"))
      (is (string= (cosmos-db::cosmos-context-account-name modified-context) "new-account"))
      (is (string= (cosmos-db::cosmos-context-database-name modified-context) "new-database"))
      (is (string= (cosmos-db::cosmos-context-container-name modified-context) "new-container")))))

;; validation

(test create-valid-user
  (let ((user (cosmos-db::make-user :id "1" :username "validname" :age 5)))
    (is (not (null user)))
    (is (string= "validname" (cosmos-db::user-username user)))
    (is (= 5 (cosmos-db::user-age user)))))

(test invalid-user-signals-error
  "Test that creating an invalid user signals a validation-error with correct error messages"
  (signals cosmos-db::validation-error
    (cosmos-db::make-user :id "1" :username "123@" :age 151)))


(test struct-encoding-decoding
  (let* ((user (cosmos-db::make-user :id "1" :username "validname" :age 5))
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
    (is (string= json-string "{\"id\":\"1\",\"username\":\"validname\",\"age\":5}")
        "JSON string should match expected output")
    ;; Verify that the decoded user matches the original
    (is (equalp user decoded-user) "Decoded user should equal the original user")))

(defun run-tests ()
  (run! :cosmos-db-test-suite))
()