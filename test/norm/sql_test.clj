(ns norm.sql-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [clojure.string :as str]
            [clojure.set :refer [intersection]]
            [next.jdbc :as jdbc]
            [norm.core :as norm :refer [where order skip limit fetch! fetch-count!]]
            [norm.sql :as sql]
            [norm.sql.jdbc :refer [instance-meta]]
            [norm.sql.specs :as sql.specs]))

(defn contains-all?
  "Determines whether the map m contains all the keys ks."
  [m & ks]
  (= (intersection (set ks) (set (keys m))) (set ks)))

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
    (is (= "SELECT \"users\".*, \"people\".* FROM (users AS \"users\" LEFT JOIN people AS \"people\" ON (\"users\".id = \"people\".id))"
           (str (sql/select nil [:users :left-join :people {:users/id :people/id}] [:users/* :people/*])))
        "Wildcard should go without alias.")
    (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\" FROM users AS \"user\" WHERE (\"user\".id IN ((SELECT user_id AS \"user-id\" FROM employees AS \"employees\" WHERE (active IS true))))"
           (str (sql/select nil [:users :user] [:user/id :user/login] {:user/id [:in (sql/select nil :employees [:user-id] {:active true})]}))))
    (is (= "SELECT CURRENT_SCHEMA() AS \"current-schema\"" (str (sql/select nil nil [['(current-schema) :current-schema]]))))
    (is (= "SELECT id AS \"id\" FROM tasks AS \"tasks\" WHERE (scheduled >= NOW())" (str (sql/select nil :tasks [:id] {:scheduled [:>= '(now)]})))))
  (testing "Query parts amendment"
    (let [query (sql/select nil :users [:id :name])]
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM users AS \"users\" WHERE (id = ?)" (-> query (where {:id 1}) str)))
      (is (= "SELECT id AS \"id\", name AS \"name\" FROM (users AS \"users\" LEFT JOIN people AS \"people\" ON (\"users\".person_id = \"people\".id)) WHERE (\"people\".name = ?)"
             (-> query (sql/join :left-join :people {:users/person-id :people/id}) (where {:people/name "John Doe"}) str)))
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
  (testing "Select command building"
    (is (= "SELECT id AS \"id\", login AS \"login\" FROM users AS \"users\" WHERE (id = ?)"
           (sql/generate-sql-command (sql/select nil :users [:id :login] {:id 1})))))
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
        (is (= {:id 1 :name "John Doe" :gender "male"} (norm/fetch-by-id! person 1))
            (str (norm/find person {:id 1})))
        (is (= (merge instance-meta {:entity person}) (-> (norm/fetch-by-id! person 1) meta))
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
        (is (= (merge user {:rels {:secret {:entity :secret
                                            :type :has-one
                                            :fk :user-id}}})
               (norm/with-rels user {:secret {:entity :secret
                                              :type :has-one
                                              :fk :user-id}})))
        (is (= true
               (-> user
                   (norm/with-rels {:secret {:entity :secret
                                             :type :has-one
                                             :fk :user-id}})
                   (norm/with-eager [:secret])
                   (get-in [:rels :secret :eager]))))))))

(def entities
  {:person {:table :people
            :rels {:contacts {:entity :contact
                              :type :has-many
                              :fk :person-id}}}
   :contact {:table :contacts
             :rels {:owner {:entity :person
                            :type :belongs-to
                            :fk :person-id
                            :eager true}}}
   :user {:table :users
          :rels {:person {:entity :person
                          :type :belongs-to
                          :fk :id
                          :eager true}}}
   :user-secret {:table :secrets
                 :rels {:user {:entity :user
                               :type :belongs-to
                               :fk :id}}}
   :employee {:table :employees
              :rels {:person {:entity :person
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
                    :rels {:employees {:entity :employee
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
                                          :filter {:active true}}}}
   :document {:table :documents
              :rels {:items {:entity :doc-item
                             :type :has-many
                             :fk :doc-id}}}
   :doc-item {:table :doc-items
              :rels {:document {:entity :document
                                :type :belongs-to
                                :fk :doc-id}}}})

(deftest create-repository-test
  (sql.specs/instrument)
  (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
    (jdbc/execute! conn ["CREATE TYPE GENDER AS ENUM ('male', 'female')"])
    (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL PRIMARY KEY, name VARCHAR(100), gender GENDER, birthday DATE)"])
    (jdbc/execute! conn ["CREATE TABLE contacts (id BIGSERIAL PRIMARY KEY, person_id BIGINT REFERENCES people (id), type VARCHAR(32), \"value\" VARCHAR(128))"])
    (jdbc/execute! conn ["CREATE TABLE users (id BIGINT PRIMARY KEY REFERENCES people (id), login VARCHAR(100), role VARCHAR(50), active BOOLEAN DEFAULT true)"])
    (jdbc/execute! conn ["CREATE TABLE secrets (id BIGINT PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE, secret VARCHAR(256))"])
    (jdbc/execute! conn ["CREATE TABLE employees (id BIGSERIAL PRIMARY KEY REFERENCES people (id), supervisor_id BIGINT REFERENCES employees (id), salary NUMERIC(19, 4), active BOOLEAN DEFAULT true)"])
    (jdbc/execute! conn ["CREATE TABLE responsibilities (id BIGSERIAL PRIMARY KEY, title VARCHAR(128), description TEXT, active BOOLEAN DEFAULT true)"])
    (jdbc/execute! conn ["CREATE TABLE employees_responsibilities (employee_id BIGINT REFERENCES employees (id), responsibility_id BIGINT REFERENCES responsibilities (id), PRIMARY KEY (employee_id,responsibility_id))"])
    (jdbc/execute! conn ["CREATE VIEW employees_responsibilities_negative AS
(SELECT e.id AS employee_id, r.id AS responsibility_id
FROM (employees AS e CROSS JOIN responsibilities AS r)
LEFT JOIN employees_responsibilities AS er
ON er.responsibility_id = r.id AND er.employee_id = e.id
WHERE er.employee_id IS NULL)"])
    (jdbc/execute! conn ["CREATE TABLE documents (id BIGSERIAL PRIMARY KEY, type VARCHAR(100), status VARCHAR (100), sum NUMBER(19,4), created_at DATE)"])
    (jdbc/execute! conn ["CREATE TABLE doc_items (id BIGSERIAL PRIMARY KEY, doc_id BIGINT REFERENCES documents (id), line_number INT, ordered INT, shipped INT)"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('John Doe', 'male')"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('Jane Doe', 'female')"])
    (jdbc/execute! conn ["INSERT INTO people (name, gender) VALUES ('Zoe Doe', 'female')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, \"value\") VALUES (1, 'email', 'john.doe@mailinator.com')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, \"value\") VALUES (1, 'phone', '+1(xxx)xxx-xx-xx')"])
    (jdbc/execute! conn ["INSERT INTO contacts (person_id, type, \"value\") VALUES (2, 'email', 'jane.doe@mailinator.com')"])
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
      (is (= (keys entities) (keys repository)) "All the entities should be presented in the repository.")
      (is (= [:person :contact] (keys (norm/only repository [:person :contact]))) "Only specified entities must be present.")
      (is (= [:person :contact :user :document :doc-item] (keys (norm/except repository [:user-secret :employee :responsibility])))
          "Specified entities must be absent.")
      (testing "Fields population."
        (is (= [:id :name :gender :birthday] (get-in repository [:person :fields])))
        (is (= [:id :person-id :type :value] (get-in repository [:contact :fields])))
        (is (= [:id :login :role :active] (get-in repository [:user :fields]))))
      (testing "Transaction creation"
        (is (= [] (norm/transaction repository))
            "New transaction should be empty.")
        #_(is (satisfies? norm/Command (norm/transaction repository))
              "Transaction should be a command."); https://ask.clojure.org/index.php/4622/satisfies-doesnt-work-instance-based-protocol-polymorphism
        (is (apply contains-all? (meta (norm/transaction repository)) (keys sql/sql-command-meta))
            "Transaction should be a command."))
      (testing "SQL generation"
        (is (= "SELECT \"user\".id AS \"user/id\", \"user\".login AS \"user/login\", \"user\".role AS \"user/role\", \"user\".active AS \"user/active\", \"user.person\".id AS \"user.person/id\", \"user.person\".name AS \"user.person/name\", \"user.person\".gender AS \"user.person/gender\", \"user.person\".birthday AS \"user.person/birthday\" FROM (users AS \"user\" LEFT JOIN people AS \"user.person\" ON (\"user\".id = \"user.person\".id))"
               (str (norm/find (:user repository)))))
        (is (= "SELECT \"employee\".id AS \"employee/id\", \"employee\".supervisor_id AS \"employee/supervisor-id\", \"employee\".salary AS \"employee/salary\", \"employee\".active AS \"employee/active\", \"employee.person\".id AS \"employee.person/id\", \"employee.person\".name AS \"employee.person/name\", \"employee.person\".gender AS \"employee.person/gender\", \"employee.person\".birthday AS \"employee.person/birthday\" FROM (employees AS \"employee\" LEFT JOIN people AS \"employee.person\" ON (\"employee\".id = \"employee.person\".id)) WHERE (\"employee.person\".name = ?)"
               (-> (norm/find (:employee repository) {:person/name "Jane Doe"}) str)
               (-> (norm/find (:employee repository) {:employee.person/name "Jane Doe"}) str)
               (-> (norm/find (:employee repository)) (where {:employee.person/name "Jane Doe"}) str)))
        (is (= "SELECT \"user\".login AS \"user/login\", \"user.person\".name AS \"user.person/name\" FROM (users AS \"user\" LEFT JOIN people AS \"user.person\" ON (\"user\".id = \"user.person\".id)) WHERE (\"user\".id = ?)"
               (str (norm/find (:user repository) [:user/login :person/name] {:id 1}))
               (str (norm/find (:user repository) [:login :person/name] {:user/id 1}))))
        (is (= "SELECT \"user_secret\".id AS \"user-secret/id\", \"user_secret\".secret AS \"user-secret/secret\" FROM (secrets AS \"user_secret\" LEFT JOIN (users AS \"user_secret.user\" LEFT JOIN people AS \"user_secret.user.person\" ON (\"user_secret.user\".id = \"user_secret.user.person\".id)) ON (\"user_secret\".id = \"user_secret.user\".id)) WHERE (\"user_secret.user.person\".name = ?)"
               (-> (:user-secret repository)
                   (norm/find {:user.person/name "John Doe"})
                   str))
            "Usage of a relation's relation in WHERE clause must join it in.")
        (is (= "SELECT \"user.person\".id AS \"user.person/id\", \"user.person\".name AS \"user.person/name\", \"user.person\".gender AS \"user.person/gender\", \"user.person\".birthday AS \"user.person/birthday\" FROM (people AS \"user.person\" LEFT JOIN users AS \"user\" ON (\"user\".id = \"user.person\".id)) WHERE (\"user.person\".id = ?)"
               (-> (:user repository)
                   (norm/find-related :person nil)
                   (norm/where {:id 1})
                   str))
            "Field in WHERE clause must be prefixed with a selecting entity name.")
        (is (= "SELECT \"user.person\".id AS \"user.person/id\", \"user.person\".name AS \"user.person/name\", \"user.person\".gender AS \"user.person/gender\", \"user.person\".birthday AS \"user.person/birthday\" FROM (people AS \"user.person\" LEFT JOIN users AS \"user\" ON (\"user\".id = \"user.person\".id)) WHERE (\"user\".id = ?)"
               (-> (:user repository)
                   (norm/find-related :person nil)
                   (norm/where ^:exact {:user/id 1})
                   str))
            "Field in WHERE clause must not be modified for exact clause.")
        (is (= "SELECT \"person\".id AS \"person/id\", \"person\".name AS \"person/name\", \"person\".gender AS \"person/gender\", \"person\".birthday AS \"person/birthday\" FROM people AS \"person\" ORDER BY \"person\".id, \"person\".name"
               (-> (norm/find (:person repository))
                   (norm/order [:id :name])
                   str))
            "Fields in ORDER clause must be prefixed with an entity name.")
        (is (= "SELECT \"person\".id AS \"person/id\", \"person\".name AS \"person/name\", \"person\".gender AS \"person/gender\", \"person\".birthday AS \"person/birthday\" FROM people AS \"person\" ORDER BY \"person\".name DESC, \"person\".id ASC"
               (-> (norm/find (:person repository))
                   (norm/order {:name :desc :id :asc})
                   str))
            "Fields in ORDER clause must be prefixed with an entity name.")
        (is (= "SELECT \"person\".id AS \"person/id\", \"person\".name AS \"person/name\", \"person\".gender AS \"person/gender\", \"person\".birthday AS \"person/birthday\" FROM people AS \"person\" ORDER BY id, name"
               (-> (norm/find (:person repository))
                   (norm/order ^:exact [:id :name])
                   str))
            "Fields in ORDER clause must not be modified for exact values.")
        (is (= "SELECT \"person\".id AS \"person/id\", \"person\".name AS \"person/name\", \"person\".gender AS \"person/gender\", \"person\".birthday AS \"person/birthday\" FROM people AS \"person\" ORDER BY name DESC, id ASC"
               (-> (norm/find (:person repository))
                   (norm/order ^:exact {:name :desc :id :asc})
                   str))
            "Fields in ORDER clause must not be modified for exact values."))
      (testing "creation"
        (testing "with embedded entities"
          (testing "of belongs-to relationship"
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
          (testing "of has-many relationship"
            (is (= {:id 1}
                   (norm/create! (:document repository)
                                 {:type "test"
                                  :status "new"
                                  :sum 1000
                                  :items [{:line-number 1
                                           :ordered 10}
                                          {:line-number 2
                                           :ordered 20}
                                          {:line-number 3
                                           :ordered 30}]})))
            (is (= {:id 1 :type "test" :status "new" :sum 1000.0000M} (norm/fetch-by-id! (:document repository) 1)))
            (is (= 3
                   (-> (:doc-item repository)
                       (norm/find! {:doc-id 1})
                       count))))
          (testing "of many to many relationship"
            (is (= {:id 4}
                   (norm/create! (:employee repository)
                                 {:id 4
                                  :salary 1000
                                  :responsibilities [{:id 2} {:title "Saving the world"}]})))
            (is (= {:id 4 :salary 1000.0000M :person {:id 4 :name "Buzz Lightyear"} :active true}
                   (norm/fetch-by-id! (:employee repository) 4))
                "Aggregation root must be saved")
            (is (= {:id 5 :title "Saving the world" :active true}
                   (norm/fetch-by-id! (:responsibility repository) 5))
                "Related entity must be created.")
            (is (= [{:id 2 :title "Watering plants" :active true}
                    {:id 5 :title "Saving the world" :active true}]
                   (norm/find-related! (:employee repository) :responsibilities {:id 4}))
                "Pre-existed and created entities must be referenced.")
            ))
        (testing "with prepare function"
          (let [person (assoc (:person repository) :prepare #(update % :name str/upper-case))]
            (is (= {:id 5} (norm/create! person {:name "Sid" :gender "male"})))
            (is (= {:id 5, :name "SID" :gender "male"} (norm/fetch-by-id! person 5))
                "Field must be preprocessed with `prepare` fn."))))
      (testing "eager fetching"
        (let [instance (norm/fetch-by-id! (:user repository) 1)]
          (is (= {:id 1
                  :login "john.doe"
                  :active true
                  :person {:id 1
                           :name "John Doe"
                           :gender "male"}}
                 instance))
          (is (= (merge instance-meta {:entity (:user repository)})
                 (meta instance)))
          (is (= (merge instance-meta {:entity (:person repository)})
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
               (-> (norm/find-related (:user repository) :person nil) (where ^:exact {:user/id 1}) fetch!)
               (-> (norm/find-related (:user repository) :person nil) (where {:id 1}) fetch!)
               (-> (norm/find-related (:user repository) :person {:person/name "John Doe"}) fetch!)
               (-> (norm/find-related (:user repository) :person {:user.person/name "John Doe"}) fetch!)))
        (is (some? (-> (norm/find-related! (:user repository) :person {:id 1})
                       first
                       meta
                       :entity))
            "Instance of related entity should have an entity in meta.")
        (is (some? (-> (norm/find-related! (:user repository) :person {:id 1})
                       first
                       meta
                       ((comp (partial partial =) :entity))
                       (filter (vals repository))
                       first))
            "Instance of related entity should have a correct entity in metadata (found in repository).")
        (is (= [{:id 3 :person-id 2 :type "email" :value "jane.doe@mailinator.com" :owner {:id 2 :name "Jane Doe" :gender "female"}}]
               (-> (norm/find-related (:person repository) :contacts {:person/name "Jane Doe"}) fetch!)))
        (is (= 0 (-> (norm/find-related (:person repository) :contacts {:id 3}) fetch-count!)))
        (is (= 2 (-> (norm/find-related (:employee repository) :responsibilities {:id 1}) fetch-count!)))
        (is (= [{:id 1, :title "Cleaning", :active true} {:id 3, :title "Gardening", :active true}]
               (-> (norm/find-related (:employee repository) :responsibilities {:id 1}) fetch!)
               (-> (norm/find-related (:employee repository) :nonresponsibilities {:id 4}) fetch!)))
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
          (is (= {:id 5, :name "sid", :gender "male"} (norm/fetch-by-id! person 5)))))
      (testing "filter by related entities without eager fetching"
        (is (= [{:id 1, :secret "sha256(xxxxxxx)"}] (-> (norm/find (:user-secret repository) {:user/login "john.doe"}) fetch!))))
      (testing "update"
        (testing "with embedded entities"
          (testing "of belongs-to relationship"
            (is (= 1 (norm/update! (:user repository) {:person {:name "Buzz"}} {:id 4})))
            (is (= {:id 4 :name "Buzz"} (norm/fetch-by-id! (:person repository) 4))
                "Updating of an embedded entity must change the entity.")
            (is (= 2 (norm/update! (:user repository) {:role "user" :person {:name "Buzz Lightyear"}} {:id 4})))
            (is (= {:id 4 :login "buzz.lightyear" :role "user" :active false :person {:id 4, :name "Buzz Lightyear"}}
                   (norm/fetch-by-id! (:user repository) 4))
                "Updating with an embedded entity must change both the main and embedded entity."))
          (testing "of has-many relationship"
            (is (= 4 (norm/update! (:document repository)
                                   {:status "shipped"
                                    :items [{:id 1 :shipped 0}
                                            {:id 2 :shipped 20}
                                            {:id 3 :shipped 10}]}
                                   {:id 1})))
            (is (= "shipped" (-> (:document repository) (norm/fetch-by-id! 1) :status)) "Aggregation root must be updated.")
            (is (= 0 (-> (:doc-item repository) (norm/fetch-by-id! 1) :shipped)) "Component of aggregation must be updated.")
            (is (= 20 (-> (:doc-item repository) (norm/fetch-by-id! 2) :shipped)) "Component of aggregation must be updated.")))
        (testing "filtered by relationship"
          (is (= 1 (norm/update! (:user repository) {:login "buzz"} {:person/name "Buzz Lightyear"})))
          (is (= {:id 4 :login "buzz" :role "user" :active false :person {:id 4, :name "Buzz Lightyear"}}
                 (norm/fetch-by-id! (:user repository) 4))
              "Update with filtering by related entity must change the main entity."))
        (testing "with prepare function"
          (let [person (assoc (:person repository) :prepare #(update % :name str/lower-case))]
            (is (= 1 (norm/update! person {:name "Sid"} {:id 5})))
            (is (= {:id 5, :name "sid", :gender "male"} (norm/fetch-by-id! person 5))
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
