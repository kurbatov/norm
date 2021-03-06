# norm is not an ORM

A Clojure library designed to construct SQL (and maybe other types of)
queries dynamically and fetch persistent entities from a storage.

Here *entities* are the units of long-living state in a domain model of an application: person, user,
customer, order, etc. A simplest entity *instance* may be thought of as a row in a table or a node in a graph.

There are no macros in **norm** - just data structures and functions.

## Capabilities

+ describe entities and their relations as a map
+ autodiscovery of entities and relations in a database
+ CRUD for the entities
+ controlled eager fetching of the `has-one` and `belongs-to` related entities
+ fetch `has-many` related entities on demand
+ built-in filters for the entity and its relations
+ filter by properties of related entities when selecting, updating and deleting
+ create and update aggregates in a transaction when related entities are embedded (encouraged by DDD)
+ prepare entity data before saving
+ transform entity data after fetching
+ access to underlying Query Builder
+ sub-queries in WHERE clause
+ combine commands and requests for transactional execution
+ test entities descriptions with spec when creating a repository

TODO

- support enums trough namespaced keywords https://www.bevuta.com/en/blog/using-postgresql-enums-in-clojure/
- create specs for entity fields from column types
- test for MySQL support

## Usage

Add following dependency into the `project.clj` or `deps.edn`:

```clojure
[norm "1.0.0"]
```

Also you want to ensure that there is a JDBC driver for the database of your choice
among dependencies. **norm** supports H2 and PostgreSQL as of now, so check if at least
one of the following is presented:

```clojure
[com.h2database/h2 "1.4.200"]
[org.postgresql/postgresql "42.2.12"]
```

Require the core namespace of **norm** into a namespace in your project:

```clojure
(ns my-project.db-component
  (:require [norm.core :as norm]))

;; or

(require '[norm.core :as norm])
```

Prepare datasource to establish DB connection. Any `jdbc.next`'s `Connectable` will do.
That means any `javax.sql.DataSource` implementation will do too.

```clojure
;; prepare datasource the way you like it
;; using Hikari pool, for example
(def db (HikariDataSource.
          (doto (HikariConfig.)
                (.setJdbcUrl jdbc-url)
                (.setUsername username)
                (.setPassword password))))

;; or for testing with an in-memory H2 instance
(with-open [db (next.jdbc/get-connection {:dbtype "h2:mem"})]
  ;; your code goes here
)
```

Describe entities and their relations in a map (let's call it *mapping*).
There is a helper function that builds the mapping based on foreign keys of
a provided database (described in the section below).

```clojure
(def mapping {:address {:table :addresses}})
```

See [`doc/mapping-example.md`](doc/mapping-example.md) for the full-featured example of a mapping.

Build a `repository` using the mapping and the database connection from the previous steps.

```clojure
(def repository (norm/create-repository :sql mapping {:db db}))
```

Use entities from the repository to build queries and commands.
All queries get executed only with explicit `fetch!` (or `execute!` for commands)
call or when called with a function that has the `!` suffix.

```clojure
(let [page 0
      size 40
      sort :role
      clause {:active true}
      query (-> (norm/find (:employee repository) clause)
                ;; (norm/where clause) may be here when clause is not specified above
                (norm/order sort)
                (norm/skip (* page size))
                (norm/limit size))
      total (norm/fetch-count! query) ;; fetches total number of the records as if limit were not specified
      employees (norm/fetch! query)]  ;; fetches entities according to the query
  {:total total
   :employees employees})
```

Adjust and combine commands before executing and execute them.

For example, that's how you would execute a command:

```clojure
(-> (:user repository)
    (norm/create {:login \"admin\"})
    norm/execute!)

;; or

(norm/create! (:user repository) {:login \"admin\"})
```

If you want multiple commands executed in the same transaction it may be achieved using `then` function:

```clojure
(let [{:keys [user person]} repository]
  (-> (norm/create user {:login \"admin\"})
      (norm/then (norm/create person {:name \"John Doe\"}))
      (norm/then #(norm/create-realtion user (:id (first $)) :preson (:id (second $))))
      norm/execute!))
```

Unary function may be used as a command inside a transaction. It receives a vector of previous commands' results
and is supposed to return a command (or query) built upon those data which are unknown before actual execution.

## Writing a Mapping

**norm** is easy to use on an existing database schema. You can automate writing of the mapping
using provided helper method:

```clojure
(require '[norm.sql.helper :refer [find-entities]])
(find-entities {:db db :schema "public"}) ; :schema may be ommitted for autodetection
```

This helper method works only for H2 and PostgreSQL currently.

The autogenerated entity description may suffer from weird naming of entities and their relations
due to lack of context. It doesn't handle pluralisation of irregular nouns (such as `person` - `people`).
It may miss relations when an entity references another entity more than once. But it provides a good
starting point to build a perfect version of the mapping.

Manual writing of the mapping from scratch is totally fine. Below you will find desription of mapping's parts.

Database identifiers for table names and fields are supposed to be in `kebab-case`.
Those converted to `snake_case` when building SQL query.

For a mapping example see [`doc/mapping-example.md`](doc/mapping-example.md).

### Entity

The only required property of an entity in the mapping is `:table`.
The following optional properties are supported:

- `:pk` - primary key column name (defaults to `:id` when absent)
- `:fields` - data fields of the entity (defaults to all columns of the table when absent)
- `:filter` - imposes filtering rule on the content of the table when not every row is an instance of the entity
- `:prepare` - an unary function that modifies an instance before saving
- `:transform` - an unary function that modifies an instance after fetching from storage
- `:rels` - a map that describes realations of the entity to other entities

### Relations

Relations are described with a map where keys stand for names of the relations and values
(which are maps as well) convey the essence of the relation. Required properties of a relation
are `:entity` (the main entity is related to), `:type` (of the relation) and `:fk` (stands for *foreign key*).

**norm** supports following relation types:

#### `:belongs-to`

`attachment` `belongs-to` `post` means that `attachment` has `posts_id` column that refers to
`id` column in `post`'s table. It doesn't say anything about how many attachments per post is allowed.

#### `:has-one`

`attachment` `has-one` `file` means that `file` has `attachments_id` column that refers to
`id` column in `attachment`'s table. There can be only one `file` per `attachment` though.

#### `:has-many`

`post` `has-many` `attachments` means that `attachment` has `posts_id` column that refers to
`id` column of `post`'s table and there might be more than one `attachment` for one `post`.

Two entities may be joined through an intermediary table that stores identifiers of both of them.
Such table goes to `:join-table` in the description of a relation. When description of a relation includes
`:join-table` it must include `:rfk` (stands for *relation's foreign key*) - a column in the join-table
which contains identifiers of related entity.

`employee` `has-many` `responsibilities` and `responsibility` in its turn `has-many` `employees`.
They relate through the join-table `employees-responsibilities`.

```clojure
{:employee       {:table :employees
                  :rels {:responsibilities {:entity :responsibility
                                            :type :has-many
                                            :fk :employee-id
                                            :join-table :employees-responsibilities
                                            :rfk :responsibility-id}}}
 :responsibility {:table :responsibilities
                  :rels {:employees {:entity :employee
                                     :type :has-many
                                     :fk :responsibility-id
                                     :join-table :employees-responsibilities
                                     :rfk :employee-id}}}}
```

#### Optional Properties of Relation

- `:filter` - clause to filter related entities
- `:eager` - when true, **norm** fetches a related entity eagerly (`false` by default).
Works only for `:has-one` and `:belongs-to` relationships.

## Building Queries

### for Entities

TODO describe `norm/find`, `norm/fetch!` and `norm/fetch-by-id!`

TODO describe `norm/where`

TODO describe `norm/create`, `norm/update`, `norm/delete` and `norm/execute!`

### for Relations

TODO describe `norm/find-related`, `norm/create-relation` and `norm/delete-relaton`

### Adjusting Entities

TODO describe `norm/with-filter`, `norm/with-eager` and `norm/with-relations`

### Low-Level Query Builder

Require the sql namespace of **norm** into a namespace in your project:

```clojure
(ns my-project.db-component
  (:require [norm.sql :as sql]))

;; or

(require '[norm.sql :as sql])
```

TODO describe `sql/insert`, `sql/update`, `sql/select` and `sql/delete`.

A query object can be compiled into SQL statement with `str` call:

```clojure
(-> (sql/select db :users [:id :login])
    (norm/where {:active true
                 :role "admin"})
    str)
;=> "SELECT id AS \"id\", login AS \"login\" FROM users WHERE (active IS true AND role = ?)"
```

## Creating Entities

TODO describe creating aggregates in a transaction

### Transactional Execution

TODO describe usage of `norm/then`, `:tx-result-fn`, `:tx-result` and functions in transactions.

TODO nested transactions and `:tx-propagation`.

## Tips

If you want to use your own datatype as a parameter then the idiomatic approach of
implementing `next.jdbc`'s `SettableParameter` protocol is enough.

### How To Use `or` in `where`

TODO using vector for a list of choises

TODO using `:or` in a where-map

TODO using `'(or ...)`

TODO prefixing identifiers when building query with the name of an entity and `^:exact` in where and order

## Clear Exit Plan

TODO describe how to get down to SQL building functions, `HoneySQL`, `sqlingvo` and bare `jdbc.next`.

## Similar Solutions

If **norm** is not quite what you are looking for, there are other libraries similar in some ways
to **norm** which I can positively or negatively recommend to spare you some time reading docs and testing.

In the world of lots of slightly different solutions, I wish all of them included such section
into their documentation.

[seql](https://github.com/exoscale/seql) may be a better fit if you want to add predefined set of
mutations into the entity model.

[Toucan](https://github.com/metabase/toucan) looks really close to **norm** in its intentions.
It is older and has a company behind it. So the library probably had seen more usage in production.
On the negative side of things, Toucan seems to be too object oriented for a Clojure library:
it requires defining a record for every entity out there.

[Hyperion](https://github.com/8thlight/hyperion) allows defining entities however dealing with
relations may be tricky.

[Relational Mapper](https://github.com/netizer/relational_mapper) offers a lightweight solution for
querying related entities based on [HoneySQL](https://github.com/seancorfield/honeysql).
It has a namespace with functions to perform DB migrations based on the mapping.
However it looks like creation, modification and deletion of entities is out of scope for this library.

I recommend you to skip [Korma](https://github.com/korma/Korma) which is mentioned in every clojure-related
resource for beginers. Korma is great for simple tasks but it is long abandoned and lacks some features
that come in handy even in a moderately complex project. Maybe someone takes over the project in the future
and makes it great again. For now I can only say many thanks to Korma for the inspiration it gave me.
I made **norm** to be a better version of Korma.

## License

Copyright © 2020

This program and the accompanying materials are made available under the
terms of the The MIT License (MIT).

See [LICENSE](https://github.com/kurbatov/norm/blob/master/LICENSE) file
for the full tex of the license.
