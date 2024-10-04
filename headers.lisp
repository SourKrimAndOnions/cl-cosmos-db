(in-package :cosmos-db)

(defmacro with-headers (&rest header-specs)
  `(remove nil
           (list
            ,@(loop for (header-name . value) in header-specs
                    collect
                    (if (and (listp value) (eq (car value) 'when))
                        `(when ,(cadr value)
                           (cons ,header-name ,(caddr value)))
                        `(when ,value
                           (cons ,header-name ,value)))))))

(defun format-consistency-level (level)
  (when level (string-downcase (symbol-name level))))

(defun format-partition-key (key)
  (when key (format nil "[~S]" key)))

(defun bool-to-string (value)
  (when value "true"))

(defun number-to-string (value)
  (when value (write-to-string value)))

(defun make-common-headers (&key auth content-type consistency-level continuation max-item-count
                              partition-key enable-cross-partition session-token
                              if-match if-none-match if-modified-since is-upsert
                              activity-id allow-tentative-writes offer-throughput
                              offer-autopilot)
  (with-headers (+header-authorization+ . auth)
    (+header-content-type+ . content-type)
    (+header-version+ . "2018-12-31")
    (+header-date+ . (local-time:format-rfc1123-timestring nil (local-time:now)))
    (+header-is-upsert+ . (bool-to-string is-upsert))
    (+header-consistency-level+ . (format-consistency-level consistency-level))
    (+header-continuation+ . continuation)
    (+header-max-item-count+ . (number-to-string max-item-count))
    (+header-partition-key+ . (format-partition-key partition-key))
    (+header-enable-cross-partition+ . (bool-to-string enable-cross-partition))
    (+header-session-token+ . session-token)
    (+header-if-match+ . if-match)
    (+header-offer-throughput+ . (number-to-string offer-throughput))
    (+header-offer-autopilot+ . offer-autopilot)
    (+header-if-none-match+ . if-none-match)
    (+header-if-modified-since+ . if-modified-since)
    (+header-activity-id+ . activity-id)
    (+header-allow-tentative-writes+ . (bool-to-string allow-tentative-writes))))

(defun construct-parameters-header-value (parameters)
  (or (loop for (name . value) in parameters
            collect `(("name" . ,name)
                      ("value" . ,value)))
      #()))
