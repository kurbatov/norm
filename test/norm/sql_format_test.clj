(ns norm.sql-format-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [norm.sql-format :as f]
            [norm.sql :as sql]))

(deftest prefix-test
  (is (= :user/id (f/prefix :user :id)))
  (is (= :supervisor.person/id (f/prefix :supervisor :person/id)))
  (is (= :employee.supervisor/id (f/prefix :employee/supervisor :id)))
  (is (= :employee.supervisor.person/id (f/prefix :employee/supervisor :person/id))))

(deftest prefixed-test
  (is (f/prefixed? :user :user/name))
  (is (f/prefixed? :user :user.person/name))
  (is (f/prefixed? :user :user.person.contact/value))
  (is (f/prefixed? :user/person :user.person/name))
  (is (f/prefixed? :user/person :user.person.contact/value))
  (is (not (f/prefixed? :user :name)))
  (is (not (f/prefixed? :user :person/name)))
  (is (not (f/prefixed? :user/person :person/name))))

(deftest ensure-prefixed-test
  (is (= {:user/id 1 :user/login "user"} (f/ensure-prefixed :user {:id 1 :login "user"})))
  (is (= {:user/id 1 :user.person/name "user"} (f/ensure-prefixed :user {:id 1 :person/name "user"})))
  (is (= {:user/id 1 :user/login "user"} (f/ensure-prefixed :user {:user/id 1 :user/login "user"}))))

(deftest format-value-test
  (is (= "NULL" (f/format-value nil)))
  (is (= "id" (f/format-value :id)))
  (is (= "\"user\".id" (f/format-value :user/id)))
  (is (= "?" (f/format-value 0)))
  (is (= "?" (f/format-value "string"))))

(deftest format-alias-test
  (is (= "id" (f/format-alias :id)) "Alias unquoted.")
  (is (= "person/id" (f/format-alias :person/id)) "Namespace separated with a slash.")
  (is (= "user.person/id" (f/format-alias :user.person/id)) "Complex namespace separated with a slash.")
  (is (= "id" (f/format-alias "id")) "Accept string alliases.")
  (is (= "person.id" (f/format-alias "person.id")) "String aliases stay as is."))

(deftest format-field-test
  (is (= "id" (f/format-field :id)) "Plain field doesn't get quoted.")
  (is (= "\"user\".id" (f/format-field :user/id)) "Namespace is a quoted prefix.")
  (is (= "name AS \"full-name\"" (f/format-field :name :full-name)) "Alias gets quoted.")
  (is (= "name AS \"full-name\"" (f/format-field [:name :full-name])) "Aliased field may be supplied as a vector.")
  (is (= "\"employee\".name AS \"full-name\"" (f/format-field :employee/name :full-name)))
  (is (= "\"employee\".name AS \"user/full-name\"" (f/format-field :employee/name :user/full-name)))
  (is (= "COUNT(id)" (f/format-field '(count :id))) "Aggregation should be applied.")
  (is (= "COUNT(\"user\".id)" (f/format-field '(count :user/id))) "Namespace gets quoted inside an aggregation.")
  (is (= "COUNT(id) AS \"count\"" (f/format-field ['(count :id) :count])) "Aggregation is aliased")
  (is (= "COUNT(\"user\".id) AS \"user/count\"" (f/format-field ['(count :user/id) :user/count]))))

(deftest format-clause-test
  (is (= "(id = ?)" (f/format-clause {:id 1})))
  (is (= "(id <> ?)" (f/format-clause {:id [:not= 1]})))
  (is (= "(id IS NULL)" (f/format-clause {:id nil})))
  (is (= "(id IS NOT NULL)" (f/format-clause {:id [:not= nil]})))
  (is (= "(active IS true)" (f/format-clause {:active true})))
  (is (= "(active IS false)" (f/format-clause {:active false})))
  (is (= "(\"user\".id = \"employee\".id)" (f/format-clause {:user/id :employee/id})))
  (is (= "(id = ? AND name = ?)" (f/format-clause {:id 1 :name "John"})))
  (is (= "(name IN (?, ?, ?))" (f/format-clause {:name ["John" "Sam" "Jack"]})))
  (is (= "(name NOT IN (?, ?, ?))" (f/format-clause {:name [:not-in "John" "Sam" "Jack"]})))
  (is (= "(name ILIKE ?)" (f/format-clause {:name [:ilike "John%"]})))
  (is (= "((birthday BETWEEN ? AND ?))" (f/format-clause {:birthday [:between "1988-01-01" "1988-02-01"]})))
  (is (= "((id = ? OR name = ?) AND role = ?)" (f/format-clause {:or {:id 1 :name "John"} :role "admin"})))
  (is (= "(((id = ? OR name = ?)) AND (role = ?))" (f/format-clause '(and {:or {:id 1 :name "John"}} {:role "admin"})))))

(deftest format-source-test
  (testing "Simple case"
    (is (= "users AS \"users\"" (f/format-source :users)))
    (is (= "schema_name.users_secrets AS \"schema_name.users_secrets\"" (f/format-source :schema-name/users-secrets))))
  (testing "Aliasing"
    (is (= "users AS \"user\""
           (f/format-source :users :user)))
    (is (= "users AS \"user\""
           (f/format-source [:users :user]))))
  (testing "Joins"
    (is (= "(users AS \"users\" LEFT JOIN people AS \"people\" ON (\"users\".id = \"people\".id))"
           (f/format-source :users :left-join :people {:users/id :people/id})))
    (is (= "(users AS \"users\" LEFT JOIN people AS \"person\" ON (\"users\".id = \"person\".id))"
           (f/format-source :users :left-join [:people :person] {:users/id :person/id})))
    (is (= "(users AS \"user\" LEFT JOIN people AS \"person\" ON (\"user\".id = \"person\".id))"
           (f/format-source [:users :user] :left-join [:people :person] {:user/id :person/id})))
    (is (= "(users AS \"user\" INNER JOIN people AS \"person\" ON (\"user\".id = \"person\".id))"
           (f/format-source [:users :user] :inner-join [:people :person] {:user/id :person/id}))))
  (testing "Nested joins"
    (is (= "(users AS \"user\" LEFT JOIN (people AS \"person\" LEFT JOIN contacts AS \"contact\" ON (\"person\".id = \"contact\".person_id)) ON (\"user\".id = \"person\".id))"
           (f/format-source [:users :user] :left-join [[:people :person] :left-join [:contacts :contact] {:person/id :contact/person-id}] {:user/id :person/id})))
    (is (= "((users AS \"user\" LEFT JOIN secrets AS \"secret\" ON (\"user\".id = \"secret\".user_id)) LEFT JOIN people AS \"person\" ON (\"user\".id = \"person\".id))"
           (f/format-source [[:users :user] :left-join [:secrets :secret] {:user/id :secret/user-id}] :left-join [:people :person] {:user/id :person/id})))
    (is (= "((users AS \"user\" LEFT JOIN secrets AS \"secret\" ON (\"user\".id = \"secret\".user_id)) LEFT JOIN (people AS \"person\" LEFT JOIN contacts AS \"contact\" ON (\"person\".id = \"contact\".person_id)) ON (\"user\".id = \"person\".id))"
           (f/format-source [[:users :user] :left-join [:secrets :secret] {:user/id :secret/user-id}] :left-join [[:people :person] :left-join [:contacts :contact] {:person/id :contact/person-id}] {:user/id :person/id}))))
  (testing "Subselect"
    (is (= "(SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (role = ?)) AS \"admin\"" (f/format-source [(sql/select nil :users [:id :name] {:role "admin"}) :admin])))))

(deftest format-order-test
  (is (= "id" (f/format-order :id)))
  (is (= "\"user\".id" (f/format-order :user/id)))
  (is (= "\"user\".id, \"user\".name" (f/format-order :user/id :user/name)))
  (is (= "id" (f/format-order [:id])))
  (is (= "id, name" (f/format-order [:id :name])))
  (is (= "\"user\".id, \"user\".name" (f/format-order [:user/id :user/name])))
  (is (= "id asc" (f/format-order {:id :asc})))
  (is (= "id asc, name desc" (f/format-order {:id :asc :name :desc})))
  (is (= "\"user\".id asc, \"person\".name desc" (f/format-order {:user/id :asc :person/name :desc}))))

(deftest extract-values-test
  (testing "Extract values"
    (testing "from clause"
      (is (= [] (f/extract-values {})))
      (is (= [1] (f/extract-values {:id 1})))
      (is (= [] (f/extract-values {:id nil})))
      (is (= [] (f/extract-values {:active true})))
      (is (= [] (f/extract-values {:user/id :employee/id})))
      (is (= [1 "John"] (f/extract-values {:id 1 :name "John"})))
      (is (= [1 "John%"] (f/extract-values {:id 1 :name [:like "John%"]})))
      (is (= ["John%" "1980-01-01" "1990-01-01"] (f/extract-values {:name [:like "John%"] :birthday [:between "1980-01-01" "1990-01-01"]})))
      (is (= ["John" "Jane" "Boris" "1980-01-01" "1990-01-01"] (f/extract-values {:name ["John" "Jane" "Boris"] :birthday [:between "1980-01-01" "1990-01-01"]})))
      (is (= ["John" "Jane" "Boris" "1980-01-01" "1990-01-01" "admin"] (f/extract-values {:or {:name ["John" "Jane" "Boris"] :birthday [:between "1980-01-01" "1990-01-01"]} :active true :role "admin"})))
      (is (= ["John" "Jane" "Boris" "1980-01-01" "1990-01-01" "admin"] (f/extract-values '(and {:or {:name ["John" "Jane" "Boris"] :birthday [:between "1980-01-01" "1990-01-01"]}} {:active true :role "admin"})))))
    (testing "from target"
      (is (= [] (f/extract-values :users)))
      (is (= [] (f/extract-values [:users :user])))
      (is (= [] (f/extract-values [[:users :user] :left-join [:people :person] {:user/id :person/id}])))
      (is (= ["John"] (f/extract-values [[:users :user] :left-join [:users :sub] {:user/id :sub/supervisor-id :user/name "John"}]))))))
