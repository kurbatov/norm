(ns norm.sql-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [norm.core :as norm :refer [where order skip limit fetch! fetch-count!]]
            [norm.sql :as sql]
            [norm.sql.specs :as sql.specs]))

(deftest sql-query-test
  (testing "Query building"
    (is (= "SELECT * FROM users AS \"users\"" (str (sql/select nil :users))))
    (is (= "SELECT * FROM users AS \"users\"" (str (sql/select nil :users [:*]))))
    (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\"" (str (sql/select nil :users [:id :name]))))
    (is (= "SELECT \"user\".id AS \"user/id\", \"user\".name AS \"user/name\" FROM users AS \"user\""
           (str (sql/select nil [:users :user] [:user/id :user/name]))))
    (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\""
           (str (sql/select nil :users [:id :name] {})))
        "Empty where clause should not apear in the query.")
    (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (id = ?)"
           (str (sql/select nil :users [:id :name] {:id 1}))))
    (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (((id = ?) AND (name = ?)) AND (role = ?))"
           (-> (sql/select nil :users [:id :name] {:id 1})
               (where {:name "John Doe"})
               (where {:role "admin"})
               str)))
    (is (= "SELECT column_name AS \"column-name\" FROM information_schema.columns AS \"information_schema.columns\" WHERE (table_schema ILIKE ? AND table_name ILIKE ?)"
           (-> (sql/select nil
                           :information-schema/columns
                           [:column-name]
                           {:table-schema [:ilike "public"]
                            :table-name   [:ilike "table_name"]}) str)))
    (is (= "SELECT \"user\".id AS \"user/id\", \"user\".name AS \"user/name\" FROM users AS \"user\" WHERE (\"user\".id = ?)"
           (str (sql/select nil [:users :user] [:user/id :user/name] {:user/id 1}))))
    (is (= "SELECT \"user\".id AS \"user/id\", \"person\".name AS \"person/name\" FROM (users AS \"user\" LEFT JOIN people AS \"person\" ON (\"user\".id = \"person\".id))"
           (str (sql/select nil [[:users :user] :left-join [:people :person] {:user/id :person/id}] [:user/id :person/name]))))
    (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\" FROM users AS \"user\" WHERE (\"user\".id IN ((SELECT user_id AS \"user-id\" FROM employees AS \"employees\" WHERE (active IS true))))"
           (str (sql/select nil [:users :user] [:user/id :user/login] {:user/id [:in (sql/select nil :employees [:user-id] {:active true})]}))))
    (is (= "SELECT CURRENT_SCHEMA() AS \"current-schema\"" (str (sql/select nil nil [['(current-schema) :current-schema]])))))
  (testing "Query parts amendment"
    (let [query (sql/select nil :users [:id :name])]
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (id = ?)" (-> query (where {:id 1}) str)))
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM (users AS \"users\" LEFT JOIN people AS \"people\" ON (\"users\".person_id = \"people\".id)) WHERE (\"people\".name = ?)"
             (-> query (norm/join :left-join :people {:users/person-id :people/id}) (where {:people/name "John Doe"}) str)))
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
        (is (zero? (fetch-count! query)))
        (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('John Doe', 'male')"])
        (jdbc/execute! conn ["INSERT INTO users (id, login) VALUES (1, 'john.doe')"])
        (is (= 1 (fetch-count! query)))
        (is (= [{:id 1 :name "John Doe"}] (fetch! query)))
        (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('Jane Doe', 'female')"])
        (is (= 2 (fetch-count! query)))
        (is (= 1 (-> query (where {:id 1}) fetch-count!)))
        (is (= [{:id 1 :name "John Doe"} {:id 2 :name "Jane Doe"}] (-> query (where {:gender ["female", "male"]}) fetch!)))
        (is (= [{:id 2 :name "Jane Doe" :gender "female"}] (-> query (where {:id 2}) (fetch! [:id :name :gender]))))
        (is (= [{:person/id 1 :person/name "John Doe" :person/gender "male" :user/login "john.doe"}
                {:person/id 2 :person/name "Jane Doe" :person/gender "female" :user/login nil}]
               (->
                (select [[:people :person] :left-join [:users :user] {:person/id :user/id}] [:person/id :person/name :person/gender :user/login])
                fetch!)))
        (is (= [{:id 1 :name "John Doe"} {:id 2 :name "Jane Doe"}] (-> query (where {:or {:id 1 :gender "female"}}) fetch!)))
        (is (= [{:id 1 :name "John Doe"} {:id 2 :name "Jane Doe"}] (-> query (order [:id]) fetch!)))
        (is (= [{:id 2 :name "Jane Doe"} {:id 1 :name "John Doe"}] (-> query (order {:id :desc}) fetch!)))
        (testing "with sub-query"
          (is (= [{:person/id 1, :person/name "John Doe"}]
                 (-> (select [:people :person] [:person/id :person/name] {:person/id [:in (select :users [:users/id] {:users/login "john.doe"})]})
                     fetch!))))))))

(deftest sql-command-test
  (testing "Insert command building"
    (is (= "INSERT INTO users (login, role) VALUES (?, ?)" (sql/generate-sql-command (sql/insert nil :users {:login "admin" :role "admin"}))))
    (is
     (= "INSERT INTO users (login, role) VALUES (?, ?), (?, ?), (?, ?)"
        (-> (sql/insert nil :users [[:login :role]
                                    ["admin" "admin"]
                                    ["foo" "user"]
                                    ["bar" "user"]])
            sql/generate-sql-command))))
  (testing "Update command building"
    (is (= "UPDATE users SET login = ?, role = ? WHERE (id = ?)" (sql/generate-sql-command (sql/update nil :users {:login "admin", :role "admin"} {:id 1})))))
  (testing "Delete command building"
    (is (= "DELETE FROM users WHERE (id = ?)" (sql/generate-sql-command (sql/delete nil :users {:id 1})))))
  (testing "Execution"
    (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
      (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL, name VARCHAR(100) NOT NULL, gender VARCHAR(10), birthday DATE)"])
      (testing "of insert"
        (is (= {:id 1} (-> (sql/insert conn :people {:name "John Doe"}) norm/execute!)))
        (is (= {:id 2} (-> (sql/insert conn :people [[:name :gender] ["Jane Doe" "female"] ["Zoe Doe" "female"]]) norm/execute!)))
        (is (= 3 (-> (sql/select conn :people) fetch-count!)) "Database must contain all the inserted records."))
      (testing "of update"
        (is (= 1 (-> (sql/update conn :people {:gender "male"} {:id 1}) norm/execute!)))
        (is (= [{:id 1 :gender "male"}] (-> (sql/select conn :people [:id :gender] {:id 1}) fetch!)) "Row must change after update.")
        (is (= 1 (-> (sql/update conn :people {:gender nil} {:id 1}) norm/execute!)))
        (is (= [{:id 1 :gender nil}] (-> (sql/select conn :people [:id :gender] {:id 1}) fetch!)) "Property must change after update with nil."))
      (testing "of delete"
        (is (= 1 (-> (sql/delete conn :people {:id 1}) norm/execute!)))
        (is (= [{:id 2} {:id 3}] (-> (sql/select conn :people [:id]) fetch!)) "Deleted row must be missing from the table."))
      (testing "of transaction"
        (is (= [{:id 4} {:id 5} {:id 6} {:id 7} [{:id 4 :name "Buzz Lightyear"}]]
               (-> (sql/transaction conn
                                    (sql/insert nil :people {:name "Buzz Lightyear"})
                                    (sql/insert nil :people {:name "Woody"})
                                    (sql/insert nil :people {:name "Jessie"})
                                    (sql/insert nil :people {:name "Sid"})
                                    (sql/select nil :people [:id :name] {:name "Buzz Lightyear"}))
                   norm/execute!)))
        (is (= [{:id 4 :name "Buzz Lightyear"}
                {:id 5 :name "Woody"}
                {:id 6 :name "Jessie"}
                {:id 7 :name "Sid"}]
               (-> (sql/select conn :people [:id :name] {:id [:> 3]}) norm/execute!)))
        (is (= (repeat 4 1)
               (-> (sql/delete conn :people {:id 4})
                   (norm/then (sql/delete conn :people {:id 5}))
                   (norm/then (sql/delete conn :people {:id 6}))
                   (norm/then (sql/delete conn :people {:id 7}))
                   norm/execute!)))
        (is (thrown? org.h2.jdbc.JdbcSQLSyntaxErrorException
                     (-> (sql/transaction conn
                                          (sql/delete nil :people {:name "Jane Doe"})
                                          (sql/insert nil :people {:first-name "John" :last-name "Doe"})
                                          (sql/select nil :people [:id :name] {:name "John Doe"}))
                         norm/execute!)))
        (is (= [{:id 2 :name "Jane Doe"}]
               (-> (sql/select conn :people [:id :name] {:name "Jane Doe"}) norm/execute!))
            "Record must be present after failed transaction")))))

(deftest relational-entity-test
  (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
    (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL, name VARCHAR(100), gender VARCHAR(10), birthday DATE)"])
    (jdbc/execute! conn ["CREATE TABLE users (id BIGINT, login VARCHAR(100), role VARCHAR(50), active BOOLEAN)"])
    (let [person (sql/create-entity {:db conn} {:name :person, :table :people, :fields [:id :name :gender :birthday]})
          user (sql/create-entity {:db conn} {:name :user, :table :users, :fields [:id :login :role :active]})]
      (testing "creation"
        (is (= {:id 1} (-> (norm/create person {:name "John Doe" :gender "male"}) norm/execute!)))
        (is (= {:id 2} (-> (norm/create person {:name "Jane Doe" :gender "female"}) norm/execute!)))
        (is (= {:id 1} (-> (norm/create user {:id 1 :login "john" :role "user"}) norm/execute!))))
      (testing "fetching"
        (is (= {:id 1 :name "John Doe" :gender "male"} (norm/fetch-by-id! person 1)))
        (is (= (merge norm/instance-meta {:entity person}) (-> (norm/fetch-by-id! person 1) meta))
            "Metadata of an instance must contain an implementation of the `Instance `protocol and a reference to the entity.")
        (is (= [{:id 1 :name "John Doe" :gender "male"}
                {:id 2 :name "Jane Doe" :gender "female"}]
               (-> (norm/find person) fetch!)))
        (is (= [{:id 1 :name "John Doe" :gender "male"}] (-> (norm/find person {:id 1}) fetch!))))
      (testing "order"
        (is (= [{:id 2 :name "Jane Doe" :gender "female"}
                {:id 1 :name "John Doe" :gender "male"}]
               (-> (norm/find person {:birthday nil}) (order {:id :desc}) fetch!))))
      (testing "offset and limit"
        (is (= [{:id 2 :name "Jane Doe" :gender "female"}]
               (-> (norm/find person {:birthday nil}) (order {:id :desc}) (skip  0) (limit 1) fetch!)))
        (is (= [{:id 1 :name "John Doe" :gender "male"}]
               (-> (norm/find person {:birthday nil}) (order {:id :desc}) (skip 1) (limit 1) fetch!))))
      (testing "update"
        (is (= 1 (-> (norm/update person {:name "Jane Love"} {:id 2}) norm/execute!)))
        (is (= {:id 2 :name "Jane Love" :gender "female"} (norm/fetch-by-id! person 2)) "Entity must change after update."))
      (testing "delete"
        (is (= 1 (-> (norm/delete person {:id 2}) norm/execute!)))
        (is (nil? (norm/fetch-by-id! person 2)) "Entity must be missing after delete."))
      (testing "with filter"
        (let [active-user (norm/with-filter user {:active true})
              active-admin (norm/with-filter active-user {:role "admin"})]
          (is (= (merge user {:filter {:active true}})
                 active-user))
          (is (= (merge user {:filter '(and {:active true} {:role "admin"})})
                 active-admin))
          (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\", \"user\".role AS \"user/role\", \"user\".active AS \"user/active\" FROM users AS \"user\" WHERE (\"user\".active IS true)"
                 (-> active-user norm/find str)))
          (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\", \"user\".role AS \"user/role\", \"user\".active AS \"user/active\" FROM users AS \"user\" WHERE ((\"user\".active IS true) AND (\"user\".id = ?))"
                 (-> active-user (norm/find {:id 1}) str)))
          (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\", \"user\".role AS \"user/role\", \"user\".active AS \"user/active\" FROM users AS \"user\" WHERE ((\"user\".active IS true) AND (\"user\".role = ?))"
                 (-> active-admin norm/find str)))
          (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\", \"user\".role AS \"user/role\", \"user\".active AS \"user/active\" FROM users AS \"user\" WHERE (((\"user\".active IS true) AND (\"user\".role = ?)) AND (\"user\".id = ?))"
                 (-> active-admin (norm/find {:id 1}) str)))))
      (testing "mutating an entity"
        (is (= (merge user {:relations {:secret {:entity :secret
                                                 :type :has-one
                                                 :fk :user-id}}})
               (norm/with-relations user {:secret {:entity :secret
                                                   :type :has-one
                                                   :fk :user-id}})))
        (is (= true
               (-> user
                   (norm/with-relations {:secret {:entity :secret
                                                  :type :has-one
                                                  :fk :user-id}})
                   (norm/with-eager [:secret])
                   (get-in [:relations :secret :eager]))))))))

(def entities
  {:person {:table :people
            :relations {:contacts {:entity :contact
                                   :type :has-many
                                   :fk :person-id}}}
   :contact {:table :contacts
             :relations {:owner {:entity :person
                                 :type :belongs-to
                                 :fk :person-id
                                 :eager true}}}
   :user {:table :users
          :relations {:person {:entity :person
                               :type :belongs-to
                               :fk :id
                               :eager true}}}
   :user-secret {:table :secrets
                 :relations {:user {:entity :user
                                     :type :belongs-to
                                     :fk :id}}}
   :employee {:table :employees
              :relations {:person {:entity :person
                                   :type :belongs-to
                                   :fk :id
                                   :eager true}
                          :supervisor {:entity :employee
                                       :type :belongs-to
                                       :fk :supervisor-id}
                          :subordinates {:entity :employee
                                         :type :has-many
                                         :fk :supervisor-id
                                         :filter {:active true}}
                          :responsibilities {:entity :responsibility
                                             :type :has-many
                                             :fk :employee-id
                                             :rfk :responsibility-id
                                             :join-table :employees-responsibilities
                                             :filter {:active true}}
                          :nonresponsibilities {:entity :responsibility
                                                :type :has-many
                                                :fk :employee-id
                                                :rfk :responsibility-id
                                                :join-table :employees-responsibilities-negative
                                                :filter {:active true}}}}
   :responsibility {:table :responsibilities
                    :relations {:employees {:entity :employee
                                            :type :has-many
                                            :fk :responsibility-id
                                            :rfk :employee-id
                                            :join-table :employees-responsibilities
                                            :filter {:active true}}
                                :nonemployees {:entity :employee
                                               :type :has-many
                                               :fk :responsibility-id
                                               :rfk :employee-id
                                               :join-table :employees-responsibilities-negative
                                               :filter {:active true}}}}})

(deftest create-repository-test
  (sql.specs/instrument)
  (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
    (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL, name VARCHAR(100), gender VARCHAR(10), birthday DATE)"])
    (jdbc/execute! conn ["CREATE TABLE contacts (id BIGSERIAL, person_id BIGINT, type VARCHAR(32), value VARCHAR(128))"])
    (jdbc/execute! conn ["CREATE TABLE users (id BIGINT, login VARCHAR(100), role VARCHAR(50), active BOOLEAN DEFAULT true)"])
    (jdbc/execute! conn ["CREATE TABLE secrets (id BIGINT, secret VARCHAR(256))"])
    (jdbc/execute! conn ["CREATE TABLE employees (id BIGSERIAL, supervisor_id BIGINT, salary NUMERIC(19, 4), active BOOLEAN DEFAULT true)"])
    (jdbc/execute! conn ["CREATE TABLE responsibilities (id BIGSERIAL, title VARCHAR(128), description TEXT, active BOOLEAN DEFAULT true)"])
    (jdbc/execute! conn ["CREATE TABLE employees_responsibilities (employee_id BIGINT, responsibility_id BIGINT)"])
    (jdbc/execute! conn ["CREATE VIEW employees_responsibilities_negative AS
(SELECT e.id AS employee_id, r.id AS responsibility_id
FROM (employees AS e CROSS JOIN responsibilities AS r)
LEFT JOIN employees_responsibilities AS er
ON er.responsibility_id = r.id AND er.employee_id = e.id
WHERE er.employee_id IS NULL)"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('John Doe', 'male')"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('Jane Doe', 'female')"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('Zoe Doe', 'female')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, value) VALUES (1, 'email', 'john.doe@mailinator.com')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, value) VALUES (1, 'phone', '+1(xxx)xxx-xx-xx')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, value) VALUES (2, 'email', 'jane.doe@mailinator.com')"])
    (jdbc/execute! conn ["INSERT INTO users (id, login) VALUES (1, 'john.doe')"])
    (jdbc/execute! conn ["INSERT INTO users (id, login, active) VALUES (2, 'jane.doe', false)"])
    (jdbc/execute! conn ["INSERT INTO users (id, login, active) VALUES (3, 'zoe.doe', true)"])
    (jdbc/execute! conn ["INSERT INTO secrets (id, secret) VALUES (1, 'sha256(xxxxxxx)')"])
    (jdbc/execute! conn ["INSERT INTO secrets (id, secret) VALUES (2, 'sha256(yyyyyyy)')"])
    (jdbc/execute! conn ["INSERT INTO secrets (id, secret) VALUES (3, 'sha256(zzzzzzz)')"])
    (jdbc/execute! conn ["INSERT INTO employees (id, salary) VALUES (1, 1500)"])
    (jdbc/execute! conn ["INSERT INTO employees (id, supervisor_id, salary) VALUES (2, 1, 3000)"])
    (jdbc/execute! conn ["INSERT INTO employees (id, supervisor_id, salary, active) VALUES (3, 1, 3100, false)"])
    (jdbc/execute! conn ["INSERT INTO responsibilities (title) VALUES ('Cleaning')"])
    (jdbc/execute! conn ["INSERT INTO responsibilities (title) VALUES ('Watering plants')"])
    (jdbc/execute! conn ["INSERT INTO responsibilities (title) VALUES ('Gardening')"])
    (jdbc/execute! conn ["INSERT INTO responsibilities (title, active) VALUES ('Deprecated activity', false)"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (1, 1)"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (1, 3)"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (2, 2)"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (3, 2)"])
    (jdbc/execute! conn ["INSERT INTO employees_responsibilities (employee_id, responsibility_id) VALUES (3, 4)"])
    (let [repository (norm/create-repository :sql entities {:db conn})]
      #_(is (instance? norm.sql.RelationalEntity (:person repository)))
      (is (= repository @(:repository (meta (:person repository)))) "Entity must have repository in metadata.")
      (testing "Fields populated."
        (is (= [:id :name :gender :birthday] (get-in repository [:person :fields])))
        (is (= [:id :person-id :type :value] (get-in repository [:contact :fields])))
        (is (= [:id :login :role :active] (get-in repository [:user :fields]))))
      (testing "SQL generation"
        (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\", \"user\".role AS \"user/role\", \"user\".active AS \"user/active\", \"user.person\".id AS \"user.person/id\", \"user.person\".name AS \"user.person/name\", \"user.person\".gender AS \"user.person/gender\", \"user.person\".birthday AS \"user.person/birthday\" FROM (users AS \"user\" LEFT JOIN people AS \"user.person\" ON (\"user\".id = \"user.person\".id))"
               (str (norm/find (:user repository)))))
        (is (= "SELECT \"employee\".id AS \"employee/id\", \"employee\".supervisor_id AS \"employee/supervisor-id\", \"employee\".salary AS \"employee/salary\", \"employee\".active AS \"employee/active\", \"employee.person\".id AS \"employee.person/id\", \"employee.person\".name AS \"employee.person/name\", \"employee.person\".gender AS \"employee.person/gender\", \"employee.person\".birthday AS \"employee.person/birthday\" FROM (employees AS \"employee\" LEFT JOIN people AS \"employee.person\" ON (\"employee\".id = \"employee.person\".id)) WHERE (\"employee.person\".name = ?)"
               (-> (norm/find (:employee repository) {:person/name "Jane Doe"}) str)
               (-> (norm/find (:employee repository) {:employee.person/name "Jane Doe"}) str)
               (-> (norm/find (:employee repository)) (where {:employee.person/name "Jane Doe"}) str)))
        (is (= "SELECT \"user\".login AS \"user/login\", \"user.person\".name AS \"user.person/name\" FROM (users AS \"user\" LEFT JOIN people AS \"user.person\" ON (\"user\".id = \"user.person\".id)) WHERE (\"user\".id = ?)"
               (str (norm/find (:user repository) [:user/login :person/name] {:id 1}))
               (str (norm/find (:user repository) [:login :person/name] {:user/id 1})))))
      (testing "creation"
        (testing "with embedded entities"
          (is (= {:id 4}
                 (-> (norm/create
                      (:user-secret repository)
                      {:secret "sha256xxxx"
                       :user {:login "buzz.lightyear"
                              :active false
                              :person {:name "Buzz Lightyear"}}})
                     norm/execute!)))
          (is (= {:id 4 :secret "sha256xxxx"} (norm/fetch-by-id! (:user-secret repository) 4)))
          (is (= {:id 4 :login "buzz.lightyear" :active false :person {:id 4 :name "Buzz Lightyear"}}
                 (norm/fetch-by-id! (:user repository) 4))))
        (testing "with prepare function"
          (let [person (assoc (:person repository) :prepare #(update % :name str/upper-case))]
            (is (= {:id 5} (norm/create! person {:name "Sid"})))
            (is (= {:id 5, :name "SID"} (norm/fetch-by-id! person 5))
                "Field must be prepeocessed with `prepare` fn."))))
      (testing "eager fetching"
        (let [instance (norm/fetch-by-id! (:user repository) 1)]
          (is (= {:id 1
                  :login "john.doe"
                  :active true
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
                 :active true
                 :person {:id 2
                          :name "Jane Doe"
                          :gender "female"}}]
               (-> (norm/find (:employee repository))
                   (where {:employee.person/name "Jane Doe"})
                   fetch!))
            "Clause by related entity's fields must work."))
      (testing "fetching fields of related entities"
        (is (= [{:login "john.doe", :person {:name "John Doe"}}] (norm/find! (:user repository) [:user/login :person/name] {:id 1}))))
      (testing "fetching related entities"
        (is (= [{:id 1 :name "John Doe" :gender "male"}]
               (-> (norm/find-related (:user repository) :person {:id 1}) fetch!)
               (-> (norm/find-related (:user repository) :person {:user/id 1}) fetch!)
               (-> (norm/find-related (:user repository) :person nil) (where {:user/id 1}) fetch!)
               (-> (norm/find-related (:user repository) :person {:person/name "John Doe"}) fetch!)
               (-> (norm/find-related (:user repository) :person {:user.person/name "John Doe"}) fetch!)))
        (is (= [{:id 3 :person-id 2 :type "email" :value "jane.doe@mailinator.com" :owner {:id 2 :name "Jane Doe" :gender "female"}}]
               (-> (norm/find-related (:person repository) :contacts {:person/name "Jane Doe"}) fetch!)))
        (is (= 0 (-> (norm/find-related (:person repository) :contacts {:id 3}) fetch-count!)))
        (is (= 2 (-> (norm/find-related (:employee repository) :responsibilities {:id 1}) fetch-count!)))
        (is (= [{:id 1, :title "Cleaning", :active true} {:id 3, :title "Gardening", :active true}]
               (-> (norm/find-related (:employee repository) :responsibilities {:id 1}) fetch!)
               (-> (norm/find-related (:employee repository) :nonresponsibilities {:id 2}) fetch!)))
        (is (= [{:id 1 :salary 1500.0000M :active true :person {:id 1 :name "John Doe" :gender "male"}}]
               (-> (norm/find-related (:responsibility repository) :employees {:id 1}) fetch!)
               (-> (norm/find-related (:responsibility repository) :employees {:id 3}) fetch!)
               (-> (norm/find-related (:responsibility repository) :nonemployees {:id 2}) fetch!)))
        (is (= [{:id 2 :supervisor-id 1 :salary 3000.0000M :active true :person {:id 2 :name "Jane Doe" :gender "female"}}]
               (-> (norm/find-related (:employee repository) :subordinates {:id 1}) fetch!)))
        (is (= [{:id 1 :salary 1500.0000M :active true :person {:id 1 :name "John Doe" :gender "male"}}]
               (-> (norm/find-related (:employee repository) :supervisor {:id 2}) fetch!)))
        (is (= [{:id 1 :name "John Doe" :gender "male"}]
               (-> (norm/find-related (:contact repository) :owner {:value "john.doe@mailinator.com"}) fetch!))
            "Filtering by a field of the main entity should work.")
        (is (= [{:id 1 :login "john.doe" :active true :person {:id 1 :name "John Doe" :gender "male"}}]
               (-> (norm/find-related (:user-secret repository) :user {:user.person/name "John Doe"}) fetch!))
            "Clause by a related entity's relation should join the source to the query."))
      (testing "fetch with filter"
        (is (= [{:id 1 :login "john.doe" :active true :person {:id 1, :name "John Doe", :gender "male"}}
                {:id 3 :login "zoe.doe" :active true :person {:id 3, :name "Zoe Doe", :gender "female"}}]
               (-> (:user repository) (norm/with-filter {:active true}) norm/find fetch!)))
        (is (= [{:id 3 :login "zoe.doe" :active true :person {:id 3, :name "Zoe Doe", :gender "female"}}]
               (-> (:user repository) (norm/with-filter {:active true}) (norm/find {:person/gender "female"}) fetch!))))
      (testing "fetch with transform"
        (let [person (assoc (:person repository) :transform #(update % :name str/lower-case))]
          (is (= {:id 5, :name "sid"} (norm/fetch-by-id! person 5)))))
      (testing "filter by related entities without eager fetching"
        (is (= [{:id 1, :secret "sha256(xxxxxxx)"}] (-> (norm/find (:user-secret repository) {:user/login "john.doe"}) fetch!))))
      (testing "update"
        (testing "with embedded entities"
          (is (= 1 (norm/update! (:user repository) {:person {:name "Buzz"}} {:id 4})))
          (is (= {:id 4 :name "Buzz"} (norm/fetch-by-id! (:person repository) 4))
              "Updating of an embedded entity must change the entity.")
          (is (= 2 (norm/update! (:user repository) {:role "user" :person {:name "Buzz Lightyear"}} {:id 4})))
          (is (= {:id 4 :login "buzz.lightyear" :role "user" :active false :person {:id 4, :name "Buzz Lightyear"}}
                 (norm/fetch-by-id! (:user repository) 4))
              "Updating with an embedded entity must change both the main and embedded entity."))
        (testing "filtered by relationship"
          (is (= 1 (norm/update! (:user repository) {:login "buzz"} {:person/name "Buzz Lightyear"})))
          (is (= {:id 4 :login "buzz" :role "user" :active false :person {:id 4, :name "Buzz Lightyear"}}
                 (norm/fetch-by-id! (:user repository) 4))
              "Update with filtering by related entity must change the main entity."))
        (testing "with prepare function"
          (let [person (assoc (:person repository) :prepare #(update % :name str/lower-case))]
            (is (= 1 (norm/update! person {:name "Sid"} {:id 5})))
            (is (= {:id 5, :name "sid"} (norm/fetch-by-id! person 5))
                "Field must be preprocessed with prepare fn."))))
      (testing "changing relations"
        (is (= {:employee-id 1, :responsibility-id 2}
               (norm/create-relation! (:employee repository) 1 :responsibilities 2)))
        (is (= [{:id 1, :title "Cleaning", :active true} {:id 3, :title "Gardening", :active true} {:id 2, :title "Watering plants", :active true}]
               (norm/find-related! (:employee repository) :responsibilities {:id 1}))
            "Created relation should be found.")
        (is (= 1 (norm/delete-relation! (:employee repository) 1 :responsibilities 2)))
        (is (= [{:id 1, :title "Cleaning", :active true} {:id 3, :title "Gardening", :active true}]
               (norm/find-related! (:employee repository) :responsibilities {:id 1}))
            "Deleted relation should not be found."))
      (testing "delete by related entity"
        (is (= 1 (norm/delete! (:user repository) {:person/name "Buzz Lightyear"})))
        (is (nil? (norm/fetch-by-id! (:user repository) 4)) "Deleted entity must not be found."))))
  (sql.specs/unstrument))
