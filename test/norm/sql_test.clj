(ns norm.sql-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [next.jdbc :as jdbc]
            [norm.core :as norm :refer [join where order skip limit fetch fetch-count]]
            [norm.sql :as sql])
  (:import [norm.sql RelationalEntity]))

(deftest sql-query-test
  (testing "Query building"
    (is (= "SELECT * FROM users AS \"users\"" (str (sql/select nil :users))))
    (is (= "SELECT * FROM users AS \"users\"" (str (sql/select nil :users [:*]))))
    (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\"" (str (sql/select nil :users [:id :name]))))
    (is (= "SELECT \"user\".id AS \"user/id\", \"user\".name AS \"user/name\" FROM users AS \"user\""
           (str (sql/select nil [:users :user] [:user/id :user/name]))))
    (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (id = ?)"
           (str (sql/select nil :users [:id :name] {:id 1}))))
    (is (= "SELECT column_name AS \"column-name\" FROM information_schema.columns AS \"information_schema.columns\" WHERE (table_schema ILIKE ? AND table_name ILIKE ?)"
           (-> (sql/select nil
                           :information-schema/columns
                           [:column-name]
                           {:table-schema [:ilike "public"]
                            :table-name   [:ilike "table_name"]}) str)))
    (is (= "SELECT \"user\".id AS \"user/id\", \"user\".name AS \"user/name\" FROM users AS \"user\" WHERE (\"user\".id = ?)"
           (str (sql/select nil [:users :user] [:user/id :user/name] {:user/id 1}))))
    (is (= "SELECT \"user\".id AS \"user/id\", \"person\".name AS \"person/name\" FROM (users AS \"user\" LEFT JOIN people AS \"person\" ON (\"user\".id = \"person\".id))"
           (str (sql/select nil [[:users :user] :left-join [:people :person] {:user/id :person/id}] [:user/id :person/name])))))
  (testing "Query parts amendment"
    (let [query (sql/select nil :users [:id :name])]
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (id = ?)" (-> query (where {:id 1}) str)))
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM (users AS \"users\" LEFT JOIN people AS \"people\" ON (\"users\".person_id = \"people\".id)) WHERE (\"people\".name = ?)"
             (-> query (join :left-join :people {:users/person-id :people/id}) (where {:people/name "John Doe"}) str)))
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" OFFSET ?" (-> query (skip 2) str)))
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" LIMIT ?" (-> query (limit 10) str)))
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" LIMIT ? OFFSET ?" (-> query (skip 2) (limit 10) str)))
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (id = ?) LIMIT ? OFFSET ?"
             (-> query (where {:id 1}) (skip 2) (limit 10) str)))))
  (testing "Data fetching"
    (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
      (let [select (partial sql/select conn)
            query (select :people [:id :name])]
        (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL, name VARCHAR(100), gender VARCHAR(10), birthday DATE)"])
        (jdbc/execute! conn ["CREATE TABLE users (id BIGINT, login VARCHAR(100), role VARCHAR(50))"])
        (is (zero? (fetch-count query)))
        (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('John Doe', 'male')"])
        (jdbc/execute! conn ["INSERT INTO users (id, login) VALUES (1, 'john.doe')"])
        (is (= 1 (fetch-count query)))
        (is (= [{:id 1 :name "John Doe"}] (fetch query)))
        (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('Jane Doe', 'female')"])
        (is (= 2 (fetch-count query)))
        (is (= 1 (-> query (where {:id 1}) fetch-count)))
        (is (= [{:id 1 :name "John Doe"} {:id 2 :name "Jane Doe"}] (-> query (where {:gender ["female", "male"]}) fetch)))
        (is (= [{:id 2 :name "Jane Doe" :gender "female"}] (-> query (where {:id 2}) (fetch [:id :name :gender]))))
        (is (= [{:person/id 1 :person/name "John Doe" :person/gender "male" :user/login "john.doe"}
                {:person/id 2 :person/name "Jane Doe" :person/gender "female" :user/login nil}]
               (->
                (select [[:people :person] :left-join [:users :user] {:person/id :user/id}] [:person/id :person/name :person/gender :user/login])
                fetch)))
        (is (= [{:id 1 :name "John Doe"} {:id 2 :name "Jane Doe"}] (-> query (where {:or {:id 1 :gender "female"}}) fetch)))
        (is (= [{:id 1 :name "John Doe"} {:id 2 :name "Jane Doe"}] (-> query (order [:id]) fetch)))
        (is (= [{:id 2 :name "Jane Doe"} {:id 1 :name "John Doe"}] (-> query (order {:id :desc}) fetch)))))))

(deftest sql-command-test
  (testing "Insert command building"
    (is (= "INSERT INTO users (login, role) VALUES (?, ?)" (str (sql/insert nil :users {:login "admin" :role "admin"}))))
    (is
     (= "INSERT INTO users (login, role) VALUES (?, ?), (?, ?), (?, ?)"
        (str (sql/insert nil :users [[:login :role]
                                     ["admin" "admin"]
                                     ["foo" "user"]
                                     ["bar" "user"]])))))
  (testing "Update command building"
    (is (= "UPDATE users SET login = ?, role = ? WHERE (id = ?)" (str (sql/update nil :users {:login "admin", :role "admin"} {:id 1})))))
  (testing "Delete command building"
    (is (= "DELETE FROM users WHERE (id = ?)" (str (sql/delete nil :users {:id 1})))))
  (testing "Execution"
    (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
      (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL, name VARCHAR(100), gender VARCHAR(10), birthday DATE)"])
      (testing "of insert"
        (is (= {:id 1} (-> (sql/insert conn :people {:name "John Doe"}) norm/execute)))
        (is (= {:id 2} (-> (sql/insert conn :people [[:name :gender] ["Jane Doe" "female"] ["Zoe Doe" "female"]]) norm/execute)))
        (is (= 3 (-> (sql/select conn :people) fetch-count)) "Database must contain all the inserted records."))
      (testing "of update"
        (is (= {:next.jdbc/update-count 1} (-> (sql/update conn :people {:gender "male"} {:id 1}) norm/execute)))
        (is (= [{:id 1 :gender "male"}] (-> (sql/select conn :people [:id :gender] {:id 1}) fetch)) "Row must change after update."))
      (testing "of delete"
        (is (= {:next.jdbc/update-count 1} (-> (sql/delete conn :people {:id 1}) norm/execute)))
        (is (= [{:id 2} {:id 3}] (-> (sql/select conn :people [:id]) fetch)) "Deleted row must be missing from the table.")))))

(deftest relational-entity-test
  (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
    (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL, name VARCHAR(100), gender VARCHAR(10), birthday DATE)"])
    (jdbc/execute! conn ["CREATE TABLE users (id BIGINT, login VARCHAR(100), role VARCHAR(50))"])
    (let [person (sql/->RelationalEntity conn :people :person :id [:id :name :gender :birthday] {})
          user (sql/->RelationalEntity conn :users :user :id [:id :login :role] {})]
      (testing "creation"
        (is (= {:id 1} (-> (norm/create person {:name "John Doe" :gender "male"}) norm/execute)))
        (is (= {:id 2} (-> (norm/create person {:name "Jane Doe" :gender "female"}) norm/execute)))
        (is (= nil (-> (norm/create user {:id 1 :login "john" :role "user"}) norm/execute))))
      (testing "fetching"
        (is (= {:id 1 :name "John Doe" :gender "male"} (norm/fetch-by-id person 1)))
        (is (= (merge norm/instance-meta {:entity person}) (-> (norm/fetch-by-id person 1) meta))
            "Metadata of an instance must contain an implementation of the `Instance `protocol and a reference to the entity.")
        (is (= [{:id 1 :name "John Doe" :gender "male"}
                {:id 2 :name "Jane Doe" :gender "female"}]
               (-> (norm/find person) fetch)))
        (is (= [{:id 1 :name "John Doe" :gender "male"}] (-> (norm/find person {:id 1}) fetch))))
      (testing "order"
        (is (= [{:id 2 :name "Jane Doe" :gender "female"}
                {:id 1 :name "John Doe" :gender "male"}]
               (-> (norm/find person {:birthday nil}) (order {:id :desc}) fetch))))
      (testing "offset and limit"
        (is (= [{:id 2 :name "Jane Doe" :gender "female"}]
               (-> (norm/find person {:birthday nil}) (order {:id :desc}) (skip  0) (limit 1) fetch)))
        (is (= [{:id 1 :name "John Doe" :gender "male"}]
               (-> (norm/find person {:birthday nil}) (order {:id :desc}) (skip 1) (limit 1) fetch))))
      (testing "update"
        (is (= {:next.jdbc/update-count 1} (-> (norm/update person {:id 2} {:name "Jane Love"}) norm/execute)))
        (is (= {:id 2 :name "Jane Love" :gender "female"} (norm/fetch-by-id person 2)) "Entity must change after update."))
      (testing "delete"
        (is (= {:next.jdbc/update-count 1} (-> (norm/delete person {:id 2}) norm/execute)))
        (is (nil? (norm/fetch-by-id person 2)) "Entity must be missing after delete.")))))

(def entities
  {:person {:table :people
            :pk :id
            :relations {:contacts {:entity :contact
                                   :type :has-many
                                   :fk :person-id}}}
   :contact {:table :contacts
             :pk :id
             :relations {:owner {:entity :person
                                 :type :belongs-to
                                 :fk :person-id
                                 :eager true}}}
   :user {:table :users
          :pk :id
          :relations {:person {:entity :person
                               :type :belongs-to
                               :fk :id
                               :eager true}}}
   :user-secret {:table :secrets
                 :pk :id
                 :relations {:owner {:entity :user
                                     :type :belongs-to
                                     :fk :id}}}
   :employee {:table :employees
              :pk :id
              :relations {:person {:entity :person
                                   :type :belongs-to
                                   :fk :id
                                   :eager true}
                          :supervisor {:entity :employee
                                       :type :has-one
                                       :fk :supervisor-id}
                          :responsibilities {:entity :responsibility
                                             :type :has-many
                                             :fk :employee-id
                                             :rfk :responsibility-id
                                             :join-table :employees-responsibilities}
                          :nonresponsibilities {:entity :responsibility
                                                :type :has-many
                                                :fk :employee-id
                                                :rfk :responsibility-id
                                                :join-table :employees-responsibilities-negative}}}
   :responsibility {:table :responsibilities
                    :pk :id
                    :relations {:employees {:entity :employee
                                            :type :has-many
                                            :fk :responsibility-id
                                            :rfk :employee-id
                                            :join-table :employees-responsibilities}
                                :nonemployees {:entity :employee
                                               :type :has-many
                                               :fk :responsibility-id
                                               :rfk :employee-id
                                               :join-table :employees-responsibilities-negative}}}})

(deftest create-repository-test
  (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
    (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL, name VARCHAR(100), gender VARCHAR(10), birthday DATE)"])
    (jdbc/execute! conn ["CREATE TABLE contacts (id BIGSERIAL, person_id BIGINT, type VARCHAR(32), value VARCHAR(128))"])
    (jdbc/execute! conn ["CREATE TABLE users (id BIGINT, login VARCHAR(100), role VARCHAR(50))"])
    (jdbc/execute! conn ["CREATE TABLE secrets (id BIGINT, secret VARCHAR(256))"])
    (jdbc/execute! conn ["CREATE TABLE employees (id BIGSERIAL, supervisor_id BIGINT, salary NUMERIC(19, 4))"])
    (jdbc/execute! conn ["CREATE TABLE responsibilities (id BIGSERIAL, title VARCHAR(128), description TEXT)"])
    (jdbc/execute! conn ["CREATE TABLE employees_responsibilities (employee_id BIGINT, responsibility_id BIGINT)"])
    (jdbc/execute! conn ["CREATE VIEW employees_responsibilities_negative AS
(SELECT e.id AS employee_id, r.id AS responsibility_id
FROM (employees AS e CROSS JOIN responsibilities AS r)
LEFT JOIN employees_responsibilities AS er
ON er.responsibility_id = r.id AND er.employee_id = e.id
WHERE er.employee_id IS NULL)"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('John Doe', 'male')"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('Jane Doe', 'female')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, value) VALUES (1, 'email', 'john.doe@mailinator.com')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, value) VALUES (1, 'phone', '+1(xxx)xxx-xx-xx')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, value) VALUES (2, 'email', 'jane.doe@mailinator.com')"])
    (jdbc/execute! conn ["INSERT INTO users (id, login) VALUES (1, 'john.doe')"])
    (jdbc/execute! conn ["INSERT INTO secrets (id, secret) VALUES (1, 'sha256(xxxxxxx)')"])
    (jdbc/execute! conn ["INSERT INTO employees (id, salary) VALUES (1, 1500)"])
    (jdbc/execute! conn ["INSERT INTO employees (id, supervisor_id, salary) VALUES (2, 1, 3000)"])
    (jdbc/execute! conn ["INSERT INTO responsibilities (title) VALUES ('Cleaning')"])
    (jdbc/execute! conn ["INSERT INTO responsibilities (title) VALUES ('Watering plants')"])
    (jdbc/execute! conn ["INSERT INTO responsibilities (title) VALUES ('Gardening')"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (1, 1)"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (1, 3)"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (2, 2)"])
    (let [repository (norm/create-repository :sql entities {:db conn})]
      (is (instance? RelationalEntity (:person repository)))
      (is (= repository @(:repository (meta (:person repository)))) "Entity must have repository in metadata.")
      (testing "Fields populated."
        (is (= [:id :name :gender :birthday] (get-in repository [:person :fields])))
        (is (= [:id :person-id :type :value] (get-in repository [:contact :fields])))
        (is (= [:id :login :role] (get-in repository [:user :fields]))))
      (testing "SQL generation"
        (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\", \"user\".role AS \"user/role\", \"user.person\".id AS \"user.person/id\", \"user.person\".name AS \"user.person/name\", \"user.person\".gender AS \"user.person/gender\", \"user.person\".birthday AS \"user.person/birthday\" FROM (users AS \"user\" LEFT JOIN people AS \"user.person\" ON (\"user\".id = \"user.person\".id))"
               (str (norm/find (:user repository)))))
        (is (= "SELECT \"employee\".id AS \"employee/id\", \"employee\".supervisor_id AS \"employee/supervisor-id\", \"employee\".salary AS \"employee/salary\", \"employee.person\".id AS \"employee.person/id\", \"employee.person\".name AS \"employee.person/name\", \"employee.person\".gender AS \"employee.person/gender\", \"employee.person\".birthday AS \"employee.person/birthday\" FROM (employees AS \"employee\" LEFT JOIN people AS \"employee.person\" ON (\"employee\".id = \"employee.person\".id)) WHERE (\"employee.person\".name = ?)"
               (-> (norm/find (:employee repository) {:person/name "Jane Doe"}) str)
               (-> (norm/find (:employee repository) {:employee.person/name "Jane Doe"}) str)
               (-> (norm/find (:employee repository)) (where {:employee.person/name "Jane Doe"}) str))))
      (testing "eager fetching"
        (let [instance (norm/fetch-by-id (:user repository) 1)]
          (is (= {:id 1
                  :login "john.doe"
                  :person {:id 1
                           :name "John Doe"
                           :gender "male"}}
                 instance))
          (is (= (merge norm/instance-meta {:entity (:user repository)})
                 (meta instance)))
          (is (= (merge norm/instance-meta {:entity (:person repository)})
                 (meta (:person instance)))
              "Related entity must have correct metadata."))
        (is (= [{:id 2
                 :supervisor-id 1
                 :salary 3000.0000M
                 :person {:id 2
                          :name "Jane Doe"
                          :gender "female"}}]
               (-> (norm/find (:employee repository))
                   (where {:employee.person/name "Jane Doe"})
                   fetch))
            "Clause by related entity's fields must work."))
      (testing "fetching related entities"
        (is (= [{:id 1 :name "John Doe" :gender "male"}]
               (-> (norm/find-related (:user repository) :person {:id 1}) fetch)
               (-> (norm/find-related (:user repository) :person {:user/id 1}) fetch)
               (-> (norm/find-related (:user repository) :person nil) (where {:user/id 1}) fetch)))
        (is (= [{:id 3 :person-id 2 :type "email" :value "jane.doe@mailinator.com" :owner {:id 2 :name "Jane Doe" :gender "female"}}]
               (-> (norm/find-related (:person repository) :contacts {:person/name "Jane Doe"}) fetch)))
        (is (= [{:id 1, :title "Cleaning"} {:id 3, :title "Gardening"}]
               (-> (norm/find-related (:employee repository) :responsibilities {:id 1}) fetch)
               (-> (norm/find-related (:employee repository) :nonresponsibilities {:id 2}) fetch)))
        (is (= [{:id 1 :salary 1500.0000M :person {:id 1 :name "John Doe" :gender "male"}}]
               (-> (norm/find-related (:responsibility repository) :employees {:id 1}) fetch)
               (-> (norm/find-related (:responsibility repository) :employees {:id 3}) fetch)
               (-> (norm/find-related (:responsibility repository) :nonemployees {:id 2}) fetch)))))))
