(ns norm.sql.helper-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [next.jdbc :as jdbc]
            [norm.sql.helper :as helper]))

(deftest find-entities-test
  (testing "Finding entities in the existing database"
    (with-open [conn (jdbc/get-connection {:dbtype "h2:mem"})]
      (jdbc/execute! conn ["CREATE TABLE people (id BIGSERIAL PRIMARY KEY, name VARCHAR(100), gender VARCHAR(10), birthday DATE)"])
      (jdbc/execute! conn ["CREATE TABLE contacts (id BIGSERIAL PRIMARY KEY, person_id BIGINT REFERENCES people (id), type VARCHAR(32), value VARCHAR(128))"])
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
      (let [entities (helper/find-entities {:db conn :schema "public"})]
        (is (= {:contacts {:table :contacts
                           :pk :id
                           :rels {:person {:type :belongs-to, :entity :people, :fk :person-id}}}
                :employees {:table :employees
                            :pk :id
                            :rels {:peopl {:type :belongs-to, :entity :people, :fk :id}
                                   :supervisor {:type :belongs-to, :entity :employees, :fk :supervisor-id}
                                   :responsibilities {:type :has-many
                                                      :entity :responsibilities
                                                      :fk :employee-id
                                                      :join-table :employees-responsibilities
                                                      :rfk :responsibility-id}
                                   :employees {:type :has-many, :entity :employees, :fk :supervisor-id}}}
                :responsibilities {:table :responsibilities
                                   :pk :id
                                   :rels {:employees {:type :has-many
                                                      :entity :employees
                                                      :fk :responsibility-id
                                                      :join-table :employees-responsibilities
                                                      :rfk :employee-id}}}
                :secrets {:table :secrets
                          :pk :id
                          :rels {:user {:type :belongs-to, :entity :users, :fk :id}}}
                :people {:table :people
                         :pk :id
                         :rels {:user {:type :has-one, :entity :users, :fk :id}
                                :contacts {:type :has-many, :entity :contacts, :fk :person-id}
                                :employee {:type :has-one, :entity :employees, :fk :id}}}
                :users {:table :users
                        :pk :id
                        :rels {:peopl {:type :belongs-to, :entity :people, :fk :id}
                               :secret {:type :has-one, :entity :secrets, :fk :id}}}}
               entities))))))