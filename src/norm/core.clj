(ns norm.core
  (:refer-clojure :exclude [find update remove])
  (:require [clojure.string :refer [split]]))

(defprotocol Command
  "A command to an abstract storage."
  :extend-via-metadata true
  (execute [command] "Executes the `command` returning an execution result.")
  (then ^Command [command next-command] "Combines two commands for execution in a transaction."))

(defprotocol Query
  "A query to an abstract storage."
  :extend-via-metadata true
  (join ^Query [query op source clause] "Adds `source` to the query linking it with specified `op`eration.")
  (where ^Query [query clauses] "Adds clauses to the query.")
  (order ^Query [query order] "Adds or replaces the order of records of the query result.")
  (skip ^Query [query amount] "Skips specified amount of records of the result.")
  (limit ^Query [query amount] "Limits the max number of records in the result.")
  (fetch [query] [query fields] "Fetches the data that comply to the query.")
  (fetch-count [query] "Fetches the amount of records for the given query."))

(defprotocol Entity
  "A persistent entity."
  :extend-via-metadata true
  (create ^Command [entity data] "Builds a command that creates a new instance of the entity in the storage when executed.")
  (fetch-by-id ^Instance [entity id] "Fetches an instance of the entity with specified id from the storage.")
  (find ^Query [entity] ^Query [entity where] "Builds a query that provides instances of the entity when fetched.")
  (find-related ^Query [entity relation where] "Builds a query that provides instances of the related entity when fetched.")
  (update ^Command [entity where patch] "Builds a command that updates state of entities in the storage when executed.")
  (delete ^Command [entity where] "Builds a command that deletes entities from the storage when executed.")
  (with-relations ^Entity [entity relations] "Creates a new entity based on the specified one amending relations.")
  (with-eager ^Entity [entity rel-keys] "Makes specified relations to be fetched eagerly."))

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
                (update entity {pk (pk instance)} (select-keys instance (disj (set fields) pk)))))
   `remove (fn remove [instance]
             (let [{:keys [entity]} (meta instance)
                   {:keys [pk]} entity]
               (delete entity {pk (pk instance)})))})

(defmulti create-repository
  "Creates a repository with specified underlying storage."
  {:arglists '([type entities & [opts]])}
  (fn create-repository-dispatch [type & _]
    (let [type (if (keyword? type)
                 (-> (str *ns*) (split #"\.") first (str "." (name type)) symbol)
                 type)]
      (require type)
      type)))
