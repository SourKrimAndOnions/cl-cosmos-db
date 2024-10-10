(in-package :cosmos-db)
;; Type definitions using only predicates

(define-type cdr-id
  (stringp)
  (string-min-length 1)
  (string-matches-pattern "^\\d+-\\d+-\\d+$"))

(define-type regular-string
  (stringp))

(define-type powermarket-id
  (stringp)
  (string-matches-pattern "^[A-Z]+-[A-Z]+\\d-\\d+$"))

(define-type powermarket-pk
  (stringp)
  (string-matches-pattern "^[A-Z]+-[A-Z]+\\d$"))

(define-type zone
  (stringp)
  (string-matches-pattern "^[A-Z]+-[A-Z]+\\d$"))

(define-type start-time
  (valid-iso8601-string-p))

(define-type end-time
  (valid-iso8601-string-p))

(define-type price
  (numberp)
  (number-min-value 0))

(define-type carbon-intensity
  (numberp)
  (number-min-value 0))

(define-type ttl
  (integerp)
  (number-min-value 0))

(define-type charge-point-id
  (stringp)
  (string-min-length 1)
  (string-max-length 50))

(define-type connector-id
  (integerp)
  (number-min-value 1)
  (number-max-value 10))  ; Assuming max 10 connectors per charge point

(define-type transaction-id
  (integerp)
  (number-min-value 0))

(define-type id-tag
  (stringp)
  (string-min-length 10)
  (string-max-length 50))

(define-type meter-value
  (integerp)
  (number-min-value 0))

(define-type timestamp
  (integerp)
  (number-min-value 0))

(define-type year
  (integerp)
  (number-min-value 2000)
  (number-max-value 2100))  ; Adjustable range

(define-type month
  (integerp)
  (number-min-value 1)
  (number-max-value 12))

(define-type day
  (integerp)
  (number-min-value 1)
  (number-max-value 31))

(define-type kilowatt-hours
  (floatp)
  (number-min-value 0))

(define-type number-as-bool
  (number-min-value 0)
  (number-max-value 1))

(define-type subscription-id
  (stringp)
  (string-matches-pattern "^BC\\d+$"))

(define-type subscription-line-id
  (stringp)
  (string-matches-pattern "^BCL-\\d+$"))

(define-type operator-id
  (integerp)
  (number-min-value 0))

(define-type vat-rate
  (floatp)
  (number-min-value 0)
  (number-max-value 1))

;; Type definitions using only predicates
(define-type invoice-id
  (stringp)
  (string-min-length 1)
  (string-matches-pattern "^E-Mobility-\\d+$"))

(define-type pk
  (stringp)
  (string-min-length 1))

(define-type document-type
  (stringp)
  (string= "Invoice"))

(define-type document-number
  (stringp)
  (string-min-length 1))

(define-type customer-number
  (stringp)
  (string-min-length 1))

(define-type date
  (integerp)
  (number-min-value 0))

(define-type amount
  (numberp))

(define-type currency-code
  (stringp))

(define-type url
  (stringp)
  (string-matches-pattern "^https?://.*\\.pdf$"))

(define-type line-number
  (integerp)
  (number-min-value 0))

(define-type item-number
  (stringp)
  (string-min-length 1))

(define-type quantity
  (numberp))

(define-type unit-of-measure
  (stringp)
  (string-max-length 3))

(define-type invoice-line-type
  (stringp))



(define-validated-struct chargepoint-data-record
    (id :type cdr-id)
  (pk :type (or string null))
  (charge-point-id :type (or charge-point-id null))
  (connector-id :type (or connector-id null))
  (transaction-id :type (or transaction-id null))
  (id-tag :type (or id-tag null))
  (meter-value-start :type (or meter-value null))
  (meter-value-stop :type (or meter-value null))
  (source :type (or string null))
  (start-enqueued-time :type (or timestamp null))
  (stop-enqueued-time :type (or timestamp null))
  (start-message-id :type (or string null))
  (stop-message-id :type (or string null))
  (max-enqueued-year :type (or year null))
  (max-enqueued-month :type (or month null))
  (max-enqueued-day :type (or day null))
  (consumption-time-local :type (or timestamp null))
  (consumption-time-utc :type (or timestamp null))
  (start-time-local :type (or timestamp null))
  (start-time-utc :type (or timestamp null))
  (stop-time-local :type (or timestamp null))
  (stop-time-utc :type (or timestamp null))
  (max-enqueued-time-local :type (or timestamp null))
  (max-enqueued-time-utc :type (or timestamp null))
  (k-wh :type (or kilowatt-hours null))
  (date-diff :type (or integer null))
  (transaction-complete :type (or number-as-bool null))
  (cdr-not-reliable :type (or number-as-bool null))
  (last-meter-value-stop :type (or meter-value null))
  (next-meter-value-start :type (or meter-value null))
  (negative-meter-value :type (or number-as-bool null))
  (meter-value-drop :type (or number-as-bool null))
  (meter-value-drop-to-zero :type (or number-as-bool null))
  (bracketed-other-transaction )
  (unphysical-consumption :type (or number-as-bool null))
  (unphysical-avg-power :type (or number-as-bool null))
  (model :type (or string null))
  (vendor :type (or string null))
  (charge-point-configuration-id :type (or transaction-id null))
  (subscription-id :type (or subscription-id null))
  (external-customer-id :type (or transaction-id null))
  (customer-id :type (or string null))
  (subscription-line-id :type (or subscription-line-id null))
  (subscription-line-id-newest :type (or subscription-line-id null))
  (id-token-id )
  (id-token-subscription-id )
  (id-token-external-customer-id )
  (id-token-customer-id )
  (operator-id :type (or operator-id null))
  (id-token-erp-product-id )
  (id-token-group-name :type (or string null))
  (id-token-subscription-line-id )
  (id-token-subscription-line-id-newest )
  (product-public-name :type (or string null))
  (charge-point-nick-name )
  (is-roaming :type (or boolean null))
  (roaming-provider-id :type (or string null))
  (session-id :type (or string null))
  (roaming-network :type (or string null))
  (kwh-price-excl-vat :type (or kilowatt-hours null))
  (total-price-excl-vat :type (or kilowatt-hours null))
  (total-price-incl-vat :type (or kilowatt-hours null))
  (vat-rate :type (or vat-rate null))
  (currency-code :type (or string null))
  (fixed-price )
  (evco-id )
  (plug-and-charge-identification )
  (intelligent-charging-time-local :type (or timestamp null))
  (intelligent-charging-time-utc :type (or timestamp null))
  (intelligent-charging-code )
  (operator-name :type (or string null))
  (address-city :type (or string null))
  (address-street-name :type (or string null))
  (address-postal-code :type (or string null))
  (address-house-num :type (or string null))
  (address-country :type (or string null))
  (claims-operator :type (or string null))
  (claims-authentication :type (or string null))
  (payment-method )
  (impacted-by-frequency-regulation :type (or boolean null))
  (is-partner-location :type (or boolean null))
  (timestamp :type (or timestamp null)))

;; (make-chargepoint-data-record :id 1)

;; (make-chargepoint-data-record :id "invalid-id" :charge-point-id 123)

;;invoices
(define-validated-struct invoice-line
    (line-number :type line-number)
  (item-number :type item-number)
  (description :type regular-string)
  (quantity :type quantity)
  (unit-price-excl-+vat+ :type amount)
  (total-amount-excl-+vat+ :type amount)
  (unitof-measure :type unit-of-measure)
  (discount-amount-excl-+vat+ :type amount)
  (start-period :type (or date null))
  (end-period :type (or date null))
  (type :type invoice-line-type)
  (meter-number :type regular-string)
  (engagement-line-number :type regular-string))

;; Type definitions using only predicates
(define-validated-struct invoice
    (id :type invoice-id)
  (pk :type pk)
  (document-type :type document-type)
  (document-number :type document-number)
  (type :type regular-string)
  (customer-number :type customer-number)
  (document-date :type date)
  (total-amount-excl-+vat+ :type amount)
  (total-amount-incl-+vat+ :type amount)
  (currency-code :type currency-code)
  (external-ref :type regular-string)
  (date-created :type timestamp)
  (date-modified :type timestamp)
  (url :type url)
  (invoice-lines :type list-of-invoice-line)
  (timestamp :type timestamp))

;;Charplan

(define-validated-struct reason
    (strategy :type regular-string)
  (sub-strategy :type (or regular-string null))
  (information :type (or regular-string null)))

(define-validated-struct power-consumed
    (amount :type (or single-float null))
  (unit :type regular-string))

(define-validated-struct segment
    (start :type timestamp)
  (end :type (or timestamp null))
  (effect :type (or single-float null))
  (power-consumed :type (or power-consumed null))
  (reason :type (or reason null)))

(define-validated-struct time-schedule
    (planned-start :type timestamp)
  (planned-end :type timestamp)
  (departure-time :type (or timestamp null))
  (earliest-finished-at :type (or timestamp null)))

(define-validated-struct charging-plan
    (time-schedule :type time-schedule)
  (power-required :type (or single-float null))
  (segments :type (list-of-segment)))

(define-validated-struct data
    (charge-point-id :type regular-string)
  (connector-id :type connector-id)
  (transaction-id :type transaction-id)
  (applied-base-strategy :type (or regular-string null))
  (enqueued-time-stamp-utc :type timestamp)
  (modified-on-utc :type timestamp)
  (charging-plan :type (or charging-plan null)))

(define-validated-struct chargingplan-doc
    (id :type regular-string)
  (pk :type regular-string)
  (data :type data)
  (timestamp :type timestamp))

(define-validated-struct power-market-doc
    (id :type powermarket-id)
  (pk :type powermarket-pk)
  (zone :type zone)
  (start-time :type start-time)
  (end-time :type end-time)
  (price :type price)
  (carbon-intensity :type carbon-intensity)
  (ttl :type ttl))
