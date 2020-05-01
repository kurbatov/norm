(ns norm.core
  (:refer-clojure :exclude [find update remove]))

(defprotocol Command
  "A command to an abstract storage."
  :extend-via-metadata true
  (execute! [command]
    "Executes the `command` returning an execution result.

    Example:

    ```
    (execute! delete-inactive-users)
    ```")
  (then ^Command [command next-command]
    "Combines two commands for execution in a transaction.

    Execution returns a vector containing the results of execution
    of each particular command.

    If an exception occurs during the execution, the whole chain
    of commands is rolled back.

    Example:

    ```
    ; execute all commands in a transaction
    (-> first-command
        (then following-command)
        (then final-command)
        execute!)
    ```"))

(extend-protocol Command
  nil
  (execute! [command] command)
  (then [_ next-command] next-command))

(defprotocol Query
  "A query to an abstract storage."
  :extend-via-metadata true
  (join ^Query [query op source clause]
    "Adds `source` to the query linking it with specified `op`eration.

    Example:

    ```
    (-> users-query
        (join :left-join :people {:users/person-id :people/id})
        (where {:people/name \"John Doe\"})
        fetch!)
    ```")
  (where ^Query [query clauses]
    "Adds clauses to the query.

    `clauses` is a map where keys represent fields, and values impose restrictions on corresponding fields:

    - a scalar value imposes the field value to be equal to the scalar;
    - a vector of scalar values imposes equality to one of the values in the vector;
    - a vector with a leading keyword (or symbol) forms the predicate for testing the field value.

    A key-value pair constructs a predicate. All predicates of the `clauses` map get conjuncted (combined using `and` operator).

    There are two special keys `'or` and `'and` which take a nested `clauses` map as the value and make that map
    to be combined using corresponding operator.

    The `clauses` are conjuncted with the previously existing clauses of the `query`.

    Example:

    ```
    (-> users-query
        (where {:user/role \"admin\"
                :user/login ['ilike \"a%\"]})
        fetch!)
    ```")
  (order ^Query [query order]
    "Adds or replaces the order of records of the query result.

    `order` is a vector of ASC-ordered fields or a map of fields to
    ordering direction (`:asc` or `:desc`).

    Examples:

    ```
    (-> users-query
        (order [:name :id])
        fetch!)

    (-> employees-query
        (order {:salary :desc})
        fetch!)
    ```")
  (skip ^Query [query amount]
    "Skips specified amount of records of the result.

    Example:

    ```
    (-> users-query
        (skip (* page page-size))
        (limit page-size)
        fetch!)
    ```")
  (limit ^Query [query amount]
    "Limits the max number of records in the result.

    Example:

    ```
    (-> users-query
        (skip (* page page-size))
        (limit page-size)
        fetch!)
    ```")
  (fetch! [query] [query fields]
    "Fetches the data that comply to the query.

    If `fields` specified, only those fields will be fetched.

    Example:

    ```
    (-> users-query
        (where {:role \"admin\"})
        (skip (* page page-size))
        (limit page-size)
        (fetch! [:id :login]))
    ```")
  (fetch-count! ^Long [query]
    "Fetches the amount of records for the given query.

    Example:

    ```
    (defn get-users-page [page page-size]
      {:page page
       :size page-size
       :total (fetch-count! active-users-query)
       :items (-> active-users-query
                  (skip (* page page-size))
                  (limit page-size)
                  fetch!)})
    ```"))

(defprotocol Entity
  "A persistent entity."
  :extend-via-metadata true
  (create ^Command [entity data]
    "Builds a command that creates a new instance of the entity in the storage when executed.
    `data` may contain data for related entities.")
  (fetch-by-id! [entity id]
    "Fetches an instance of the entity with specified id from the storage immediately.")
  (find ^Query [entity] ^Query [entity where] ^Query [entity fields where]
    "Builds a query that provides instances of the entity when fetched.")
  (find-related ^Query [entity relation where]
    "Builds a query that provides instances of the related entity when fetched.")
  (update ^Command [entity patch where]
    "Builds a command that updates state of entities in the storage when executed.
    `patch` may contain data for related entities.")
  (delete ^Command [entity where]
    "Builds a command that deletes entities from the storage when executed.")
  (create-relation ^Command [entity id relation rel-id]
    "Builds a command that creates relation between exesting entities when executed.")
  (delete-relation ^Command [entity id relation rel-id]
    "Builds a command that deletes relation between exesting entities when executed.
    The entities will sill be presented in the storage. The command only breaks binding between them.")
  (with-filter ^Entity [entity where]
    "Creates a new entity based on the specified one applying the filter.

    Example:

    ```
    (def active-user (with-filter user {:active true}))
    ```")
  (with-relations ^Entity [entity relations]
    "Creates a new entity based on the specified one amending relations.

    Example:

    ```
    (def user-with-personal-data
      (with-relations user
                      {:person {:entity :person
                                :type :has-one
                                :fk :user-id
                                :eager true}}))
    ```")
  (with-eager ^Entity [entity rel-keys]
    "Makes specified relations to be fetched eagerly.

    Example:

    ```
    (def user-with-personal-data (with-eager user [:person]))
    ```"))

(defn create!
  "Creates a new instance of the entity in the storage immediately.
  Returns a map that contains id (and generated fields if any) of created entity."
  [entity data]
  (-> (create entity data) execute!))

(defn find!
  "Finds and fetches instances of the entity immediately."
  ([entity] (-> (find entity) fetch!))
  ([entity where] (-> (find entity where) fetch!))
  ([entity fields where] (-> (find entity fields where) fetch!)))

(defn find-related!
  "Finds and fetches instances of the related entity immediately."
  [entity relation where]
  (-> (find-related entity relation where) fetch!))

(defn update! 
  "Updates state of entities in the storage immediately.
  Returns the count of updated entities."
  [entity patch where]
  (-> (update entity patch where) execute!))

(defn delete!
  "Deletes entities from the storage imeediately.
  Returns the count of deleted entities."
  [entity where]
  (-> (delete entity where) execute!))

(defn create-relation!
  "Creates relation immediately."
  [entity id relation rel-id]
  (-> (create-relation entity id relation rel-id) execute!))

(defn delete-relation!
  "Deletes relation immediately."
  [entity id relation rel-id]
  (-> (delete-relation entity id relation rel-id) execute!))

(defprotocol Instance
  "An instance of a persistent entity."
  :extend-via-metadata true
  (persist ^Command [instance] "Creates a command that persists the state of the entity.")
  (remove ^Command [instance] "Creates a command that removes the entity from the storage."))

(def instance-meta
  "Default implementation of the `Instance` protocol."
  {`persist (fn persist [instance]
              (let [{:keys [entity]} (meta instance)
                    {:keys [pk fields]} entity]
                (update entity (select-keys instance (disj (set fields) pk)) {pk (pk instance)})))
   `remove (fn remove [instance]
             (let [{:keys [entity]} (meta instance)
                   {:keys [pk]} entity]
               (delete entity {pk (pk instance)})))})

(defmulti create-repository
  "Creates a repository with specified underlying storage."
  {:arglists '([type entities & [opts]])}
  (fn create-repository-dispatch [type & _]
    (let [type (if (keyword? type)
                 (symbol (str (or (namespace type) "norm") "." (name type)))
                 type)]
      (require type)
      type)))
