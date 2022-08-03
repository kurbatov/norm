# norm is not an ORM

A Clojure library designed to construct SQL (and maybe other types of)
queries dynamically and fetch persistent entities from a storage.

Here *entities* are the units of long-living state in a domain model of an application: person, user,
customer, order, etc. A simplest entity *instance* may be thought of as a row in a table or a node in a graph.

There are no macros in **norm** - just data structures and functions.

## Capabilities

+ describe entities and their relations as a map
+ auto-discovery of entities and relations in a database
+ CRUD for the entities
+ controlled eager fetching of the `has-one` and `belongs-to` related entities
+ fetch `has-many` related entities on demand
+ built-in filters for the entity and its relations
+ filter by properties of related entities when selecting, updating and deleting
+ create and update aggregates in a transaction when related entities are embedded (encouraged by DDD)
+ build derived (slightly different) queries without repeating yourself
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

## Usage in a nutshell

Add following dependency into the `project.clj` or `deps.edn`:

```clojure
[norm "0.1.0"]
```

Also you want to ensure that there is a JDBC driver for the database of your choice
among dependencies. **norm** supports H2 and PostgreSQL as of now, so check if at least
one of the following is present:

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
    (norm/create {:login "admin"})
    norm/execute!)

;; or

(norm/create! (:user repository) {:login "admin"})
```

If you want multiple commands executed in the same transaction it may be achieved using `then` function:

```clojure
(let [{:keys [user person]} repository]
  (-> (norm/create user {:login "admin"})
      (norm/then (norm/create person {:name "John Doe"}))
      (norm/then #(norm/create-relation user (:id (first $)) :preson (:id (second $))))
      norm/execute!))
```

Unary function may be used as a command inside a transaction. It receives a vector of previous commands' results
and is supposed to return a command (or query) built upon those data which are unknown before actual execution.

## Writing a Mapping

The mapping is a regular clojure map that enlists entities of your domain keyed by entity names.

**norm** is easy to use on an existing database schema. You can automate writing of the mapping
using provided helper method:

```clojure
(require '[norm.sql.helper :refer [find-entities]])
(find-entities {:db db :schema "public"}) ; :schema may be ommitted for autodetection
```

This helper method works only for H2 and PostgreSQL currently.

The autogenerated entity description may suffer from weird naming of entities and their relations
due to lack of context. It doesn't handle pluralization of irregular nouns (such as `person` - `people`).
It may miss relations when an entity references another entity more than once. But it provides a good
starting point to build a perfect version of the mapping.

Manual writing of the mapping from scratch is totally fine. Below you will find description of mapping's parts.

Database identifiers for table names and fields are supposed to be in `kebab-case`.
Those converted to `snake_case` when building SQL query.

For a mapping example see [`doc/mapping-example.md`](doc/mapping-example.md).

The mapping has to contain at least one entity.

### Entity

The only required property of an entity in the mapping is `:table`. It specifies which table of the databases
contains main data that consists the entity.

The following optional properties are supported:

- `:pk` - primary key column name (defaults to `:id` when absent)
- `:fields` - data fields of the entity (defaults to all columns of the table when absent)
- `:filter` - imposes filtering rule on the rows of the table in case not every row represents an instance of the entity
- `:prepare` - an unary function that modifies an instance before saving
- `:transform` - an unary function that modifies an instance after fetching from storage (TODO consider renaming to :post-fetch)
- `:rels` - a map that describes relations of the entity to other entities

### Relations

Some entities may have bound or related entities.

Relations of an entity are described with a map where keys stand for names of the relations and values
(which are maps as well) convey the essence of the relation. Required properties of a relation
are `:entity` (the main entity is related to), `:type` (of the relation) and `:fk` (stands for *foreign key*).

**norm** supports following relation types:

#### `:has-one`

This kind of relationship tells that the entity is an aggregate root (main entity) for another subordinate entity.

`attachment` `has-one` `file` means that there can be only one `file` bound to an `attachment`.

In the database that would look the following way: `file` has the `attachment-id` property that refers to the `id`
property in `attachment`'s storage.

```clojure
{:attachment {:table :attachments
              :rels  {:file {:type   :has-one
                             :entity :file
                             :fk     :attachment-id}}}
 :file       {:table :files}}
```

#### `:has-many`

This relationship allows the entity to aggregate several instances of a subordinate entity.

`post` `has-many` `attachments` means that there might be more than one `attachment` for a `post`.

In the database it would look the following way: `attachment` has the `post-id` property that refers to the `id`
property of `post`'s storage.

```clojure
{:post       {:table :posts
              :rels  {:attachments {:type   :has-many
                                    :entity :attachment
                                    :fk     :post-id}}}
 :attachment {:table :attachments}}
```

#### `:belongs-to`

This kind of relationship is reciprocal to the previous two. It tells that the entity is a subordinate part of another
entity.

`attachment` `belongs-to` `post` means that `attachment` has the `post-id` property that refers to the `id` property in
`post`'s storage. It doesn't say anything about how many attachments per post is allowed.

```clojure
{:attachment {:table :attachments
              :rels  {:post {:type   :belongs-to
                             :entity :post
                             :fk     :post-id}}}
 :post       {:table :posts}}
```

#### Many-to-many Relationship

In case two entities aggregate each other's instances, they are in many-to-many relationship. Entities of this
relationship are both aggregate roots and we cannot tell that one is a subordinate of another.

In the database, two entities are joined through an intermediary table that stores identifiers of both of them.
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

## Repository

Repository is a collection of entities backed by a data source.

You can create a repository of chosen type using `norm/create-repository` function.

```clojure
;; creating a SQL repository
(def repository (norm/create-repository :sql entities {:db db}))
```

Now **norm** implements only SQL repositories which are backed by relational databases.
I would love to implement support for graph databases (Neo4j for example) and other kinds of
non-relational databases just to check if the general concept works fine with them.

You can create a derived repository using the following methods: `norm/add-entity`, `norm/except` and
`norm/only`. Please, refer to their documentation for details. Derived repositories can be handed over
to different parts of an application so that they cannot construct queries for certain entities.

`norm/transaction` allows creating a new transaction for a repository and use this object to collect
several queries and commands for transactional execution. Commands and queries may be appended into
the transaction using `conj` or `norm/then`.

```clojure
(-> (norm/transaction repository)
    (conj (norm/create (:user repository) {:login "admin"}))
    (norm/then (norm/create (:person repository) {:name "John Doe"}))
    norm/execute!)
```

## Building Queries and Commands

Most of the **norm**'s core functions generate queries and commands which do not execute immediately.
Instead you can pass and modify those objects between different functions of an application before
they reach the right place to be actually executed. That gives us an opportunity to split the application
to pure functional (free from side-effects) and imperative (with side-effects) parts. All the functions
which cause side-effects have an exclamation mark `!` at the end of their names.

What is the benefit of **norm** commands and queries instead of just plain old SQL queries in strings?
**norm** represents them as structured objects which can be analyzed and adjusted so that the final query
can be built in several independent steps. **norm**'s queries allow building derived queries without
repeating yourself and writing two slightly different queries. If you want to change one little piece
in the middle of a query, it is easier to achieve modifying a structured object than trying to split
a string in the right places and recombine it correctly.

### Queries

You can create a query using `norm/find` function specifying the entity you want and optionally
a list of fields you are interested in and filtration criteria. `norm/fetch!` performs the query
and fetches matching entities from the database. You can also use `norm/find!` in order to fetch
the same entities immediately.

```clojure
(def active-users (norm/find (:user repository) {:active true}))
(def users (norm/fetch! active-users))
;; or
(norm/find! (:user repository) {:active true})
;=> [{:id 1, :login "admin", :active true}, ...]
```

If you already have a base query where you would like to specify an additional filtration criteria, you
can use `norm/where` function. The additional criteria is conjunct with the existing criterion using logical `and`.

```clojure
(-> active-users
    (norm/where {:id [< 3]})
    norm/fetch!)
;=> [{:id 1, ...}, {:id 2, ...}]
```

When you need a certain instance of an entity and its id is known, you can use `norm/fetch-by-id!` to fetch that in
short line of code.

```clojure
(norm/fetch-by-id! (:user repository) 5)
;=> {:id 5, ...}
```

In order to fetch only related entities of an aggregate root, you can use `norm/find-related` function.

```clojure
;; getting personal info of a user with a known login
(-> (:user repository)
    (norm/find-related :person {:login "jdoe"})
    norm/fetch!)
;=> {:id 3, :name "John Doe"}
```

### Commands

Commands, by contrast to the queries, modify data in the storage. Similar to queries, commands do not
take effect immediately. In order to execute a command, use `norm/execute!` function or one of
the following functions with an exclamation mark at the end.

`norm/create` allows creating a new instance of an entity in the backing storage.

```clojure
(-> (:user repository)
    (norm/create {:login "guest"})
    norm/execute!)
;=> {:id 42}

;; or this way for brevity
(norm/create! (:user repository) {:login "guest"})
```

`norm/create` accepts an aggregate with relations of the entity as the value. This way related entities are created
in the same transaction as the aggregate root. In case if the related entity already exists and its id provided,
a relation between newly created entity and the existing one will be created in the same transaction.

```clojure
;; this call creates a new user and a related person in the same transaction
(-> (:user repository)
    (norm/create {:login "john", :person {:name "John Doe"}})
    norm/execute!)
;=> {:id 43}

;; this call creates a new user and a relation to an existing person by the person's id in one transaction
(-> (:user repository)
    (norm/create {:login "jane", :person {:id 63}})
    norm/execute!)
;=> {:id 44}
```

`norm/update` updates existing entities in the storage. It takes a patch that will be applied to matched entities
and a filtration clause as the arguments and returns a number of updated instances.

```clojure
;; activate a user with id = 42
(-> (:user repository)
    (norm/update {:active true} {:id 42})
    norm/execute!)
;=> 1
```

`norm/delete` removes entities from the storage. It takes a filtration clause as the argument and returns
a number of removed entities.

```clojure
;; remove all inactive users
(let [user (:user repository)]
  (-> (norm/delete user {:active false})
      norm/execute!))
;=> 99
```

You can manage relations between existing entities using `norm/create-relation` and `norm/delete-relation`
functions. They are especially useful for management of [many-to-many relationships](#many-to-many-relationship).

### Adjusting Entities

You can modify existing entity or create a derived one with the following functions.

`norm/with-filter` applies a default filter to all the queries for specified entity.
For example, it may be useful in order to make different entities which reside in the same table
but can be distinguished using some discriminator field.

```clojure
;; let's find top 10 incoming and outgoing documents
(let [document (:document repository)
      incoming (-> document (norm/with-filter {:type "incoming"}))
      outgoing (-> document (norm/with-filter {:type "outgoing"}))]
  {:incoming (-> (norm/find incoming) (norm/limit 10) norm/fetch!)
   :outgoing (-> (norm/find outgoing) (norm/limit 10) norm/fetch!)})
```

`norm/with-rels` amends relations of the specified entity (adds new or replaces an old one with the same name).

 ```clojure
 (def user-with-personal-data
      (norm/with-rels (:user repository)
                      {:person {:entity :person
                                :type   :has-one
                                :fk     :user-id
                                :eager  true}}))
 ```

`norm/with-eager` makes specified relations to be fetched eagerly (works only for `:has-one` and
`:belongs-to` relations). It is just a special case of `norm/with-rels` which sets `true` to
the `:eager` property of specified relations which already exist.

```clojure
(-> (:user repository)
    (norm/with-eager [:person])
    (norm/where {:id 1})
    (norm/fetch!))
;=> {:login "admin", :person {:name "John Doe"}}
```

### Low-Level Query Builder

Require the sql namespace of **norm** into a namespace in your project:

```clojure
(ns my-project.db-component
  (:require [norm.sql :as sql]))

;; or

(require '[norm.sql :as sql])
```

There are four functions to build different kinds of queries: `sql/insert`, `sql/update`,
`sql/select` and `sql/delete`.
They take a database object as the first argument and a table-name keyword as the second.
The rest of the arguments differs for all of them so, please, refer to their docs for the details.

A query object can be compiled into SQL statement with `str` call:

```clojure
(-> (sql/select db :users [:id :login])
    (norm/where {:active true
                 :role "admin"})
    str)
;=> "SELECT id AS \"id\", login AS \"login\" FROM users WHERE (active IS true AND role = ?)"
```

### Transactional Execution

`norm/then` allows combining multiple commands and queries into a single transaction.

```clojure
;; 3 users get created in the same transaction
(let [{:keys [user]} repository]
    (-> (norm/create user {:login "john"})
        (norm/then (norm/create user {:login "jane"}))
        (norm/then (norm/create user {:login "bob"}))
        norm/execute!))
```

You can use a unary function as a command inside a transaction. It receives a vector of previous commands' results
and is supposed to return a command (or query) built upon the data unknown before actual execution.

```clojure
(let [{:keys [user person]} repository]
  (-> (norm/create user {:login "john"})
      (norm/then (norm/create person {:name "John Doe"}))
      (norm/then #(norm/create-relation user (:id (first $)) :person (:id (second $))))
      norm/execute!))
```

By default the result of transaction execution is a vector of individual commands' and queries' results.

In case when one function generates a command/transaction and another executes it and processes the result,
refactoring of the first function is complicated as additional commands in the transaction change the resulting vector
of `norm/execute!`. You can explicitly manage the result of transaction execution.

Assigning `{:tx-result true}`to metadata of one of the commands/queries in a transaction makes it the result provider of
the whole transaction.

```clojure
;; making result of the last query in the transaction the result of the whole transaction
(let [{:keys [user]} repository
      create-john (norm/create user {:login "john"})
      create-jane (norm/create user {:login "jane"})
      select-john-and-jane (norm/find user {:login ["john" "jane"]})]
  (-> create-john
      (norm/then create-jane)
      (norm/then (vary-meta select-john-and-jane assoc :tx-result true))
      norm/execute!))
```

`:tx-result-fn` metadata of a transaction object allows even more flexibility in modification of a transaction result
before return from the `norm/execute!` function. It allows providing a unary function that receives a vector of results
of individual commands in a transaction at the end of the execution and modifies the final result of the transaction.

```clojure
;; transaction execution returns a vector of id for newly created entities
(let [{:keys [user]} repository
      create-john (norm/create user {:login "john"})
      create-jane (norm/create user {:login "jane"})]
  (-> create-john
      (norm/then create-jane)
      (vary-meta assoc :tx-result-fn #(mapv :id %))
      norm/execute!))
```

TODO nested transactions and `:tx-propagation`.

## Tips

If you want to use your own datatype as a parameter then the idiomatic approach of
implementing `next.jdbc`'s `SettableParameter` protocol is enough.

### How To Use `or` in `where`

TODO using vector for a list of choices

TODO using `:or` in a where-map

TODO using `'(or ...)`

TODO prefixing identifiers when building query with the name of an entity and `^:exact` in where and order

## Clear Exit Plan

TODO describe how to get down to SQL building functions, `HoneySQL`, `sqlingvo` and bare `jdbc.next`.

## Similar Solutions

If **norm** is not quite what you are looking for, there are other libraries similar in some ways
which I can positively or negatively recommend to spare you some time reading docs and testing.

In the world of lots of slightly different solutions, I wish all of them included such section
into their documentation.

[seql](https://github.com/exoscale/seql) may be a better fit if you want to add predefined set of
mutations into the entity model.

[Toucan](https://github.com/metabase/toucan) looks really close to **norm** in its intentions.
It is older and has a company behind it. So the library probably have seen more usage in production.
On the negative side of things, Toucan seems to be too object oriented for a Clojure library:
it requires defining a record for every entity out there.

[Hyperion](https://github.com/8thlight/hyperion) allows defining entities however dealing with
relations may be tricky.

[Relational Mapper](https://github.com/netizer/relational_mapper) offers a lightweight solution for
querying related entities based on [HoneySQL](https://github.com/seancorfield/honeysql).
It has a namespace with functions to perform DB migrations based on the mapping.
However it looks like creation, modification and deletion of entities is out of scope for this library.

I recommend you to skip [Korma](https://github.com/korma/Korma) which is mentioned in every clojure-related
resource for beginners. Korma is great for simple tasks but it is long abandoned and lacks some features
that come in handy even in a moderately complex project. Maybe someone takes over the project in the future
and makes it great again. For now I can only say many thanks to Korma for the inspiration it gave me.
I made **norm** to be a better version of Korma.

## License

Copyright © 2020-2022

This program and the accompanying materials are made available under the
terms of the The MIT License (MIT).

See [LICENSE](https://github.com/kurbatov/norm/blob/master/LICENSE) file
for the full tex of the license.
