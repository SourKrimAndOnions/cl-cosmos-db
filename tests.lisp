(defpackage #:cosmos-db/tests
  (:use #:cl #:fiveam #:cosmos-db))

(in-package #:cosmos-db/tests)

(def-suite :cosmos-db-test-suite)
(in-suite :cosmos-db-test-suite)

;; (test hello-world-test
;;       (is (string= "Hello, World!" (hello-world))))