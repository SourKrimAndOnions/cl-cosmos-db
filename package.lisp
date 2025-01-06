(defpackage #:cosmos-db
  (:use :cl)
  (:export
   :alist-to-struct
   :make-cosmos-context
   :compose-cosmos-ops 
   :create-database    
   :create-collection  
   :create-document    
   :list-databases     
   :get-database       
   :delete-database    
   :list-collections   
   :get-collection     
   :delete-collection  
   :query-documents    
   :get-document       
   :delete-document    
   :replace-document   
   :with-account       
   :with-database
   :with-container))

