(asdf:defsystem #:cosmos-db
  :description "A wrapper for Azure Cosmos DB REST api"
  :author "Karim Rasmus Vedelkvist"
  :version "1.0.0"
  :licence "BSD"
  :serial t
  :depends-on (#:dexador #:cl-json #:uiop #:ironclad #:closer-mop)
  :components ((:file "package")
               (:file "constants")
               (:file "models")
               (:file "json")
               (:file "validation")
               (:file "uri")
               (:file "headers")
               (:file "auth")
               (:file "cosmos"))
  :in-order-to ((asdf:test-op (asdf:test-op "cosmos-db/tests"))))

(asdf:defsystem #:cosmos-db/tests
                :depends-on (#:cosmos-db
                             #:fiveam)
                :components ((:file "tests"))
                :perform  (asdf:test-op (o c) (symbol-call :fiveam '#:run! :cosmos-db-test-suite)))