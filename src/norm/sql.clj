(ns norm.sql
  (:refer-clojure :exclude [find update remove])
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [next.jdbc.date-time]
            [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.spec.test.alpha :as st]
            [norm.core :as core :refer [where create-repository]]
            [norm.sql.format :as f]
            [norm.sql.jdbc :refer [as-entity-maps]]
            [norm.util :refer [flatten-map meta?]])
  (:import [norm.core Command Query]))

(def default-jdbc-opts
  {:return-keys true
   :builder-fn rs/as-unqualified-lower-maps})

(defmulti generate-sql-command
  "Generates SQL command depending on its type."
  :type)

(defmethod generate-sql-command :insert [{:keys [target values]}]
  (let [batch (vector? values)
        fields (if batch (first values) (keys values))
        values (if batch (rest values) [(vals values)])]
    (str "INSERT INTO " (f/format-target target) " "
         (f/group ", " (map f/format-field fields))
         " VALUES " (->> values (map #(->> % (map f/format-value) (f/group ", "))) (str/join ", ")))))

(defmethod generate-sql-command :update [{:keys [target values where]}]
  (str "UPDATE " (f/format-target target)
       " SET " (->> values
                    (map #(str (f/format-field (key %)) " = " (f/format-value (val %))))
                    (str/join ", "))
       (when where (str " WHERE " (f/format-clause where)))))

(defmethod generate-sql-command :delete [{:keys [target where]}]
  (str "DELETE FROM " (f/format-target target)
       (when where (str " WHERE " (f/format-clause where)))))

(defn- execute-sql-command [command]
  (let [{:keys [type values where]} command
        db (:db (meta command))
        params (if (vector? values) (flatten (rest values)) (and values (f/extract-values values)))
        params (if where (concat params (f/extract-values where)) params)
        command (apply vector (generate-sql-command command) params)]
    (jdbc/execute-one! db command (when (= :insert type) default-jdbc-opts))))

(declare sql-command-meta)

(defn transaction
  "Constructs chain of commands that will be executed in a transaction."
  ^Command
  [db & commands]
  (if (vector? (first commands))
    (into (first commands) (rest commands))
    (with-meta (into [] commands) (assoc sql-command-meta :db db))))

(defn- execute-transaction [tx transaction]
  (loop [results []
         commands transaction
         tx-result nil]
    (if (not-empty commands)
      (let [command (-> (first commands) (vary-meta assoc :db tx))
            command (if (fn? command)
                      (-> (command results) (vary-meta merge (meta command)))
                      command)
            result (core/execute! command)]
        (recur (conj results result) (rest commands) (if (:tx-result (meta command)) result tx-result)))
      (if-let [tx-result-fn (:tx-result-fn (meta transaction))]
        (tx-result-fn results)
        (if tx-result
          tx-result
          results)))))

(def sql-command-meta
  {`core/execute! (fn execute-command [command]
                   (let [{:keys [db entity relation]} (meta command)
                         default-result (cond-> (:values command)
                                          (:pk entity) (select-keys [(:pk entity)]))
                         result (if (vector? command)
                                  (if (:tx-propagation (meta command))
                                    (execute-transaction db command)
                                    (jdbc/with-transaction [tx db] (execute-transaction tx command)))
                                  (or (execute-sql-command command) default-result))]
                     (cond-> result
                       (and (meta? result) entity)          (vary-meta assoc :entity entity )
                       (and (meta? result) relation)        (vary-meta assoc :relation relation)
                       (#{:update :delete} (:type command)) :next.jdbc/update-count)))
   `core/then (fn then [command next-command]
                (if next-command
                  (transaction (:db (meta command)) command next-command)
                  command))})

(defn insert
  "Constructs an insert command."
  ^Command
  [db target values]
  (-> {:type :insert :target target :values values}
      (with-meta (assoc sql-command-meta :db db))))

(defn update
  "Constructs an update command."
  ^Command
  [db target values where]
  (-> {:type :update :target target :values values :where where}
      (with-meta (assoc sql-command-meta :db db))))

(defn delete
  "Constructs a delete command."
  ^Command
  [db target where]
  (-> {:type :delete :target target :where where}
      (with-meta (assoc sql-command-meta :db db))))

;; Query builder

(defrecord SQLQuery [source fields where order offset limit jdbc-opts]
  core/Command
  (execute! [this]
    (let [values (cond-> (f/extract-values where)
                   limit (conj limit)
                   offset (conj offset))
          query (apply vector (str this) values)
          {:keys [db transform]} (meta this)]
      (cond->> (jdbc/execute! db query (merge default-jdbc-opts jdbc-opts))
        transform (mapv transform))))
  (then [this next-command] (transaction (:db (meta this)) this next-command))
  
  core/Query
  (where [this clauses]
    (let [exact (:exact (meta clauses))
          entity (:entity (meta this))
          clauses (if (and (not exact) entity)
                    (f/ensure-prefixed (:name entity) clauses)
                    clauses)]
     (assoc this :where (f/conjunct-clauses where clauses))))
  (order [this order]
    (let [exact (:exact (meta order))
          entity (:entity (meta this))
          order (if (and (not exact) entity)
                  (f/ensure-prefixed (:name entity) order)
                  order)]
      (assoc this :order order)))
  (skip [this amount] (assoc this :offset amount))
  (limit [this amount] (assoc this :limit amount))
  (fetch! [this] (core/execute! this))
  (fetch! [this fields] (core/fetch! (assoc this :fields fields)))
  (fetch-count! [this]
    (-> this
        (assoc :fields [['(count :*) :count]]
               :order nil
               :offset nil
               :limit nil)
        core/fetch!
        first
        :count))
  
  Object
  (toString [this]
    (cond-> (str "SELECT " (->> (or fields [:*])
                                (map #(if (and (keyword? %) (not= :* %)) [% %] %))
                                (map f/format-field)
                                (str/join ", ")))
      source (str " FROM " (f/format-source source))
      (seq where) (str " WHERE " (f/format-clause where))
      (seq order) (str " ORDER BY " (f/format-order order))
      limit (str " LIMIT ?")
      offset (str " OFFSET ?"))))

(defn select
  "Constructs a query."
  ^SQLQuery
  ([db source] (select db source nil))
  ([db source fields] (select db source fields nil))
  ([db source fields where] (select db source fields where nil))
  ([db source fields where order] (select db source fields where order nil))
  ([db source fields where order offset] (select db source fields where order offset nil))
  ([db source fields where order offset limit] (select db source fields where order offset limit nil))
  ([db source fields where order offset limit jdbc-opts]
   (-> (->SQLQuery source fields where order offset limit jdbc-opts)
       (with-meta (merge jdbc-opts {:db db})))))

(defn join
  "Adds `source` to the query joining it with specified `op`eration.

  Example:

  ```
  (-> users-query
      (join :left-join :people {:users/person-id :people/id})
      (where {:people/name \"John Doe\"})
      fetch!)
  ```"
  [query op r-source clause] (assoc query :source [(:source query) op r-source clause]))

;; Relational entity

(defn- build-query [entity where with-eager-fetch]
  (let [{:keys [name table pk transform]} entity
        repository @(or (:repository (meta entity)) (delay {}))
        relations (->> (:relations entity)
                       (filter (comp (partial contains? repository) :entity val))
                       (map (fn [[k v]] [(f/prefix name k) v]))
                       (into {}))
        eager (when with-eager-fetch
                (->> relations
                     (filter (comp :eager val))
                     (filter (comp #{:has-one :belongs-to} :type val))))
        fields (reduce (fn [result [k v]]
                         (->> (:fields ((:entity v) repository))
                              (map (partial f/prefix k))
                              (into result)))
                       (f/ensure-prefixed name (:fields entity))
                       eager)
        where (f/conjunct-clauses
               (f/ensure-prefixed name (:filter entity))
               (f/ensure-prefixed name where))
        ks (->> (flatten-map where) (concat fields) flatten (filter keyword?))
        source (->> relations
                    (filter #(some (partial f/prefixed? (key %)) ks))
                    (reduce (fn [left-src [k v]]
                              (let [r-entity ((:entity v) repository)
                                    clause (condp = (:type v)
                                             :has-one {(f/prefix name pk) (f/prefix k (:fk v))}
                                             :belongs-to {(f/prefix name (:fk v)) (f/prefix k (:pk r-entity))}
                                             (-> (str "Filtering " name " by (or eager fetching a) property of " k " is not supported.\n"
                                                      "Eager fetching and filtering by property of a relation is supported for :has-one and :belongs-to relations only.")
                                                 IllegalArgumentException.
                                                 throw))]
                                [left-src :left-join [(:table r-entity) k] clause]))
                            [table name]))
        opts (cond-> {:builder-fn as-entity-maps :entity entity}
               transform (assoc :transform transform))]
    (select (:db (meta entity)) source fields where nil nil nil opts)))

(defn- filtered-by-rel
  "Determines if the `clause` map contains a keyword prefixed by one of `rel-keys`."
  [clause rel-keys]
  (->> (flatten-map clause)
       (filter keyword?)
       (some (fn [x] (some #(f/prefixed? % x) rel-keys)))))

(defrecord RelationalEntity [name table pk fields relations]
  core/Entity
  (create [this data]
    (let [prepare (:prepare this identity)
          data (prepare data)
          {:keys [db repository]} (meta this)
          repository @(or repository (delay {}))
          embedded-rels (filter (comp (partial contains? data) key) relations)
          instance (apply dissoc data (map key embedded-rels))
          dependencies (filter (comp (partial = :belongs-to) :type val) embedded-rels)
          dependents (filter (comp (partial = :has-one) :type val) embedded-rels)
          into-creation (fn into-creation [[k v]]
                          (-> (core/create ((:entity v) repository) (k data))
                              (vary-meta assoc :relation v)))
          create-dependencies (->> dependencies
                                   (map into-creation)
                                   (map #(vary-meta % assoc :tx-propagation true))
                                   (into [])
                                   (#(with-meta % (assoc sql-command-meta :db db :entity this))))
          owner-idx (count dependencies)
          create-this (if (zero? owner-idx)
                        (insert db table instance)
                        (fn creating-entity [results]
                          (->> results
                               (map #(let [relation (:relation (meta %))
                                           entity ((:entity relation) repository)]
                                       [(:fk relation) ((:pk entity) %)]))
                               (into instance)
                               (insert db table))))
          create-this (cond-> (vary-meta create-this assoc :entity this)
                        (not-empty embedded-rels) (vary-meta assoc :tx-result true))
          create-dependents (->> dependents
                                 (map (fn [[k v]]
                                        (let [entity ((:entity v) repository)
                                              instance (k data)]
                                          (if (pk data)
                                            (core/create entity (assoc instance (:fk v) (pk data)))
                                            (fn create-dependent [results]
                                              (->> (nth results owner-idx)
                                                   pk
                                                   (assoc instance (:fk v))
                                                   (core/create entity)))))))
                                 (map #(vary-meta % assoc :tx-propagation true)))]
      (if (empty? embedded-rels)
        create-this
        (-> create-dependencies
            (core/then create-this)
            (into create-dependents)))))
  (fetch-by-id! [this id] (-> (core/find! this {(or pk :id) id}) first))
  (find [this] (core/find this nil))
  (find [this where] (build-query this where true))
  (find [this fields where] (build-query (assoc this :fields fields) where false))  
  (find-related [this relation-key where]
    (let [relation (or (relation-key relations)
                       (-> (str "Relation " relation-key " does not exist in " name ". Try one of the following " (keys relations))
                           IllegalArgumentException.
                           throw))
          repository @(:repository (meta this) (delay {}))
          alias (f/prefix name relation-key)
          entity (-> ((:entity relation) repository) (assoc :name alias))
          where (f/ensure-prefixed name where)
          r-where (->> where
                       (filter (comp (partial f/prefixed? alias) key))
                       (into {})
                       not-empty)
          where (->> (keys r-where)
                     (apply dissoc where)
                     not-empty
                     (f/conjunct-clauses (f/ensure-prefixed name (:filter this))))
          relation-query (build-query entity (f/conjunct-clauses (:filter relation) r-where) true)
          base-query (build-query this where false)
          join-table (:join-table relation)
          clause (condp = (:type relation)
                   :has-one {(f/prefix name pk) (f/prefix alias (:fk relation))}
                   :belongs-to {(f/prefix name (:fk relation)) (f/prefix alias (:pk entity))}
                   :has-many (if join-table
                               {(f/prefix alias (:pk entity)) (f/prefix join-table (:rfk relation))}
                               {(f/prefix name pk) (f/prefix alias (:fk relation))}))
          r-source (if join-table
                     [(:source base-query) :left-join join-table {(f/prefix name pk) (f/prefix join-table (:fk relation))}]
                     (:source base-query))]
      (-> relation-query
          (assoc :source [(:source relation-query) (if join-table :right-join :left-join) r-source clause])
          (assoc :where (f/conjunct-clauses (:where relation-query) (:where base-query))))))
  (update [this patch where]
    (let [prepare (:prepare this identity)
          patch (prepare patch)
          {:keys [db repository]} (meta this)
          repository @(or repository (delay {}))
          present-keys (set (keys patch))
          embedded-rels (->> relations (filter (comp present-keys key)))
          belongs-to-rels (->> embedded-rels (filter (comp (partial = :belongs-to) :type val)))
          rel-keys (keys relations)
          initial-select (when (or (not-empty embedded-rels)
                                   (filtered-by-rel (:filter this) rel-keys)
                                   (filtered-by-rel where rel-keys))
                           (->> belongs-to-rels
                                (map (comp :fk val))
                                (into [pk])
                                (#(core/find this % where))))
          dependencies (->> belongs-to-rels
                            (map (fn [[k v]]
                                   (let [entity ((:entity v) repository)
                                         pk (:pk entity)
                                         patch (dissoc (k patch) pk)
                                         fk (:fk v)]
                                     (fn [results]
                                       (let [ids (->> (first results) (map fk) set)
                                             ids (if (= 1 (count ids)) (first ids) (into [] ids))]
                                         (core/update entity patch {pk ids}))))))
                            (map #(vary-meta % assoc :tx-propagation true)))
          dependents (->> embedded-rels
                          (filter (comp (partial = :has-one) :type val))
                          (map (fn [[k v]]
                                 (let [{:keys [entity fk]} v
                                       entity (entity repository)
                                       patch (dissoc (k patch) (:pk entity))]
                                   (fn [results]
                                     (let [ids (->> (first results) (map pk) set)
                                           ids (if (= 1 (count ids)) (first ids) (into [] ids))]
                                       (core/update entity patch {fk ids}))))))
                          (map #(vary-meta % assoc :tx-propagation true)))
          instance (apply dissoc patch (map key embedded-rels))
          base-command (when (not-empty instance)
                         (if initial-select
                           #(update db table instance {pk (->> (first %) (mapv pk))})
                           (update db table instance (f/conjunct-clauses (:filter this) where))))]
      (if initial-select
        (cond-> [initial-select]
          true (with-meta (assoc sql-command-meta :db db :entity this :tx-result-fn (comp (partial reduce +) (partial drop 1))))
          (not-empty dependencies) (into dependencies)
          (some? base-command) (conj base-command)
          (not-empty dependents) (into dependents))
        base-command)))
  (delete [this where]
    (let [db (:db (meta this))
          rel-keys (keys relations)
          initial-select (when (or (filtered-by-rel (:filter this) rel-keys)
                                   (filtered-by-rel where rel-keys))
                           (core/find this [pk] where))
          base-command (if initial-select
                         (fn [results]
                           (let [ids (->> (first results) (mapv pk))]
                             (delete db table (f/conjunct-clauses (:filter this) {pk ids}))))
                         (delete db table (f/conjunct-clauses (:filter this) where)))]
      (if initial-select
        (transaction db initial-select (vary-meta base-command assoc :tx-result true))
        base-command)))
  (create-relation [this id relation-key rel-id]
    (let [{:keys [db repository]} (meta this)
          repository @(or repository (delay {}))
          relation (relation-key relations)
          {:keys [type fk rfk join-table]} relation
          entity ((:entity relation) repository)]
      (cond
        (= :belongs-to type)              (core/update this {fk rel-id} {pk id})
        (and (#{:has-one :has-many} type)
             (not join-table))            (core/update entity {fk id} {(:pk entity) rel-id})
        (and (= :has-many type)
             join-table)                  (insert db join-table {fk id, rfk rel-id}))))
  (delete-relation [this id relation-key rel-id]
    (let [{:keys [db repository]} (meta this)
          repository @(or repository (delay {}))
          relation (relation-key relations)
          {:keys [type fk rfk join-table]} relation
          entity ((:entity relation) repository)]
      (cond
        (= :belongs-to type)              (core/update this {fk nil} {pk id, fk rel-id})
        (and (#{:has-one :has-many} type)
             (not join-table))            (core/update entity {fk nil} {(:pk entity) rel-id, fk id})
        (and (= :has-many type)
             join-table)                  (delete db join-table {fk id, rfk rel-id}))))
  (with-filter [this where]
    (clojure.core/update this :filter f/conjunct-clauses where))
  (with-relations [this relations]
    (clojure.core/update this :relations merge relations))
  (with-eager [this rel-keys]
    (let [rel-keys (if (sequential? rel-keys) rel-keys [rel-keys])]
      (clojure.core/update this :relations (partial reduce #(clojure.core/update %1 %2 assoc :eager true)) rel-keys))))

;; SQL repository

(defn create-entity [entity-meta entity]
  (let [table (:table entity)
        schema (or (namespace table) (:schema entity-meta) "public")
        table (if (namespace table) (keyword (name table)) table)
        fields (or
                (:fields entity)
                (->> (select (:db entity-meta)
                            :information-schema/columns
                            [:column-name]
                            {:table-schema [:ilike schema]
                             :table-name   [:ilike (f/format-keyword table)]})
                    core/fetch!
                    (mapv (comp ->kebab-case-keyword :column-name))))]
    (-> entity
        (assoc :pk (:pk entity :id))
        (assoc :fields fields)
        map->RelationalEntity
        (with-meta entity-meta))))

(defmethod create-repository (.-name *ns*) [_ entities & [opts]]
  (let [schema (or (:schema opts)
                   (->> (select (:db opts) nil [['(current-schema) :current-schema]])
                        core/fetch!
                        first
                        :current-schema))
        entity-meta (assoc opts :schema schema :repository (promise))
        build-entity (partial create-entity entity-meta)]
    (->> entities
         (map (fn [[k v]] [k (build-entity (assoc v :name k))]))
         (into {})
         (#(with-meta % opts))
         (deliver (:repository entity-meta))
         deref)))

(st/instrument `create-entity)
