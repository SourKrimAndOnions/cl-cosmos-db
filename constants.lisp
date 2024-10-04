(in-package :cosmos-db)
(alexandria:define-constant +header-authorization+ "Authorization" :test #'string=)
(alexandria:define-constant +header-content-type+ "Content-Type" :test #'string=)
(alexandria:define-constant +header-if-match+ "If-Match" :test #'string=)
(alexandria:define-constant +header-if-none-match+ "If-None-Match" :test #'string=)
(alexandria:define-constant +header-if-modified-since+ "If-Modified-Since" :test #'string=)
(alexandria:define-constant +header-user-agent+ "User-Agent" :test #'string=)
(alexandria:define-constant +header-activity-id+ "x-ms-activity-id" :test #'string=)
(alexandria:define-constant +header-consistency-level+ "x-ms-consistency-level" :test #'string=)
(alexandria:define-constant +header-continuation+ "x-ms-continuation" :test #'string=)
(alexandria:define-constant +header-date+ "x-ms-date" :test #'string=)
(alexandria:define-constant +header-max-item-count+ "x-ms-max-item-count" :test #'string=)
(alexandria:define-constant +header-partition-key+ "x-ms-documentdb-partitionkey" :test #'string=)
(alexandria:define-constant +header-enable-cross-partition+ "x-ms-documentdb-query-enablecrosspartition" :test #'string=)
(alexandria:define-constant +header-session-token+ "x-ms-session-token" :test #'string=)
(alexandria:define-constant +header-version+ "x-ms-version" :test #'string=)
(alexandria:define-constant +header-a-im+ "A-IM" :test #'string=)
(alexandria:define-constant +header-partition-key-range-id+ "x-ms-documentdb-partitionkeyrangeid" :test #'string=)
(alexandria:define-constant +header-allow-tentative-writes+ "x-ms-cosmos-allow-tentative-writes" :test #'string=)
;; Define constants for header names
(alexandria:define-constant +header-is-query+ "x-ms-documentdb-isquery" :test #'string=)
;; Define constants for content types
(alexandria:define-constant +content-type-query-json+ "application/query+json" :test #'string=)
(alexandria:define-constant +content-type-json+ "application/json" :test #'string=)
;; create document header
(alexandria:define-constant +header-is-upsert+ "x-ms-documentdb-is-upsert" :test #'string=)
(alexandria:define-constant +header-indexing-directive+ "x-ms-indexing-directive" :test #'string=)
;;delete document header
(alexandria:define-constant +header-request-charge+ "x-ms-request-charge" :test #'string=)
;;collections / containers
(alexandria:define-constant +header-offer-throughput+ "x-ms-offer-throughput" :test #'string=)
(alexandria:define-constant +header-offer-autopilot+ "x-ms-cosmos-offer-autopilot-settings" :test #'string=)


;; (defun format-consistency-level (level)
;;   (when level (string-downcase (symbol-name level))))

;; (defun format-partition-key (key)
;;   (when key (format nil "[~S]" key)))

;; (defun bool-to-string (value)
;;   (when value "true"))

;; (defun number-to-string (value)
;;   (when value (write-to-string value)))

;; (defun make-common-headers (context &key consistency-level continuation max-item-count
;;                                       partition-key enable-cross-partition session-token
;;                                       if-match if-none-match if-modified-since
;;                                       activity-id allow-tentative-writes)
;;   (with-headers 
;;       (+header-version+ . "2018-12-31")
;;     (+header-date+ . (local-time:format-rfc1123-timestring nil (local-time:now)))
;;     (+header-consistency-level+ . (format-consistency-level consistency-level))
;;     (+header-continuation+ . continuation)
;;     (+header-max-item-count+ . (number-to-string max-item-count))
;;     (+header-partition-key+ . (format-partition-key partition-key))
;;     (+header-enable-cross-partition+ . (bool-to-string enable-cross-partition))
;;     (+header-session-token+ . session-token)
;;     (+header-if-match+ . if-match)
;;     (+header-if-none-match+ . if-none-match)
;;     (+header-if-modified-since+ . if-modified-since)
;;     (+header-activity-id+ . activity-id)
;;     (+header-allow-tentative-writes+ . (bool-to-string allow-tentative-writes))))