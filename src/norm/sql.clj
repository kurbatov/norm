(ns norm.sql
  (:refer-clojure :exclude [find update remove select])
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [norm.core :as core :refer [where fetch find create-repository]]
            [norm.sql-format :as f]
            [norm.jdbc :refer [as-entity-maps]]
            [norm.util :refer [flatten-map]])
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
       " SET " (->> (keys values)
                    (map #(str (f/format-field %) " = ?"))
                    (str/join ", "))
       (when where (str " WHERE " (f/format-clause where)))))

(defmethod generate-sql-command :delete [{:keys [target where]}]
  (str "DELETE FROM " (f/format-target target)
       (when where (str " WHERE " (f/format-clause where)))))

(defn execute-sql-command [command]
  (let [{:keys [type values where]} command
        db (:db (meta command))
        params (if (vector? values) (flatten (rest values)) (and values (f/extract-values values)))
        params (if where (concat params (f/extract-values where)) params)
        command (if params (apply vector (generate-sql-command command) params) [(generate-sql-command command)])]
    (jdbc/execute-one! db command (when (= :insert type) default-jdbc-opts))))

(declare sql-command-meta)

(defn transaction
  "Constructs a chain of commands that will be executed in a transaction."
  ^Command
  [db & commands]
  (if (sequential? (first commands))
    (-> (first commands)
        (concat (rest commands))
        (->> (mapv identity))
        (with-meta (merge sql-command-meta {:db db})))
    (with-meta commands (merge sql-command-meta {:db db}))))

(def sql-command-meta
  {`core/execute (fn execute-command [command]
                   (let [db (:db (meta command))]
                     (if (sequential? command)
                       (jdbc/with-transaction [tx db]
                         (->> command
                              (map #(vary-meta % assoc :db tx))
                              (mapv core/execute)))
                       (execute-sql-command command))))
   `core/then (fn then [command next-command]
                (transaction (:db (meta command)) command next-command))})

(defn insert
  "Constructs an insert command."
  ^Command
  [db target values]
  (-> {:type :insert :target target :values values}
      (with-meta (merge sql-command-meta {:db db}))))

(defn update
  "Constructs an update command."
  ^Command
  [db target values where]
  (-> {:type :update :target target :values values :where where}
      (with-meta (merge sql-command-meta {:db db}))))

(defn delete
  "Constructs a delete command."
  ^Command
  [db target where]
  (-> {:type :delete :target target :where where}
      (with-meta (merge sql-command-meta {:db db}))))

;; Query builder

(defrecord SQLQuery [source fields where order offset limit jdbc-opts]
  core/Query
  (join [this op r-source clause] (SQLQuery. [source op r-source clause] fields where order offset limit jdbc-opts))
  (where [this clauses]
         (->
          (SQLQuery. source fields (f/conjunct-clauses where clauses) order offset limit jdbc-opts)
          (with-meta (meta this))))
  (order [this order] (-> (SQLQuery. source fields where order offset limit jdbc-opts) (with-meta (meta this))))
  (skip [this amount] (-> (SQLQuery. source fields where order amount limit jdbc-opts) (with-meta (meta this))))
  (limit [this amount] (-> (SQLQuery. source fields where order offset amount jdbc-opts) (with-meta (meta this))))
  (fetch [this]
    (let [values (cond-> (f/extract-values where)
                   limit (conj limit)
                   offset (conj offset))
          query (apply vector (str this) values)
          db (:db (meta this))]
      (jdbc/execute! db query (merge default-jdbc-opts jdbc-opts))))
  (fetch [this fields] (fetch (-> (SQLQuery. source fields where order offset limit jdbc-opts) (with-meta (meta this)))))
  (fetch-count [this] (-> (SQLQuery. source [['(count :*) :count]] where nil nil nil jdbc-opts) (with-meta (meta this)) fetch first :count))
  core/Command
  (execute [this] (fetch this))
  (then [this next-command] (transaction (:db (meta this)) this next-command))
  Object
  (toString [this]
    (cond->
     (str
      "SELECT " (->> (or fields [:*]) (map #(if (or (vector? %) (= :* %)) % [% %])) (map f/format-field) (str/join ", "))
      " FROM " (f/format-source source))
      where (str " WHERE " (f/format-clause where))
      order (str " ORDER BY " (f/format-order order))
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
  ([db source fields where order offset limit jdbc-opts] (-> (->SQLQuery source fields where order offset limit jdbc-opts) (with-meta {:db db}))))

;; Relational entity

(defn- build-query [entity where with-eager-fetch]
  (let [{:keys [name table pk]} entity
        repository @(:repository (meta entity) (delay {}))
        relations (->> (:relations entity)
                       (filter (comp (partial contains? repository) :entity val))
                       (map (fn [[k v]] [(f/prefix name k) v]))
                       (into {}))
        eager (when with-eager-fetch
                (->> relations
                     (filter (comp :eager val))
                     (filter (comp #{:has-one :belongs-to} :type val))
                     (into {})))
        fields (reduce (fn [result [k v]]
                         (->> (:fields ((:entity v) repository))
                              (map (partial f/prefix k))
                              (into result)))
                       (mapv (partial f/prefix name) (:fields entity))
                       eager)
        where (f/conjunct-clauses
               (f/ensure-prefixed name (:filter entity))
               (f/ensure-prefixed name where))
        ks (->> (flatten-map where) (filter keyword?))
        implicit-rels (->> (apply dissoc relations (keys eager))
                           (filter #(some (partial f/prefixed? (key %)) ks))
                           (into {}))
        source (reduce (fn [left-src [k v]]
                         (let [r-entity ((:entity v) repository)
                               clause (condp = (:type v)
                                        :has-one {(f/prefix name pk) (f/prefix k (:fk v))}
                                        :belongs-to {(f/prefix name (:fk v)) (f/prefix k (:pk r-entity))}
                                        :has-many (-> (str "Filtering by property of a :has-many relation (" k ") is not supported")
                                                      IllegalArgumentException.
                                                      throw))]
                           [left-src :left-join [(:table r-entity) k] clause]))
                       [table name]
                       (merge eager implicit-rels))]
    (select (:db (meta entity)) source fields where nil nil nil {:builder-fn as-entity-maps :entity entity})))

(defrecord RelationalEntity [name table pk fields relations]
  core/Entity
  (create [this data] (insert (:db (meta this)) table data))
  (fetch-by-id [this id] (-> (find this {(or pk :id) id}) fetch first))
  (find [this] (find this nil))
  (find [this where] (build-query this where true))
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
          clause (condp = (:type relation)
                   :has-one {(f/prefix name pk) (f/prefix alias (:fk relation))}
                   :belongs-to {(f/prefix name (:fk relation)) (f/prefix alias (:pk entity))}
                   :has-many (if (:join-table relation)
                               {(f/prefix alias (:pk entity)) (f/prefix (:join-table relation) (:rfk relation))}
                               {(f/prefix name pk) (f/prefix alias (:fk relation))}))
          r-source (if (:join-table relation)
                     [(:source base-query) :left-join (:join-table relation) {(f/prefix name pk) (f/prefix (:join-table relation) (:fk relation))}]
                     (:source base-query))]
      (-> relation-query
          (assoc :source [(:source relation-query) :right-join r-source clause])
          (assoc :where (f/conjunct-clauses (:where relation-query) (:where base-query))))))
  (update [this where patch] (update (:db (meta this)) table patch where))
  (delete [this where] (delete (:db (meta this)) table where))
  (with-filter [this where] (assoc this :filter where))
  (with-relations [this new-relations]
    (-> (RelationalEntity. name table pk fields (merge relations new-relations))
        (with-meta (meta this))))
  (with-eager [this rel-keys]
    (-> (RelationalEntity. name table pk fields (reduce #(clojure.core/update %1 %2 assoc :eager true) relations rel-keys))
        (with-meta (meta this)))))

;; SQL repository

(defn create-entity [entity-meta entity]
  (let [fields (->> (select (:db entity-meta)
                            :information-schema/columns
                            [:column-name]
                            {:table-schema [:ilike "public"]
                             :table-name   [:ilike (f/format-keyword (:table entity))]})
                    fetch
                    (mapv (comp ->kebab-case-keyword :column-name)))
        fields (if (empty? fields) (:fields entity fields) fields)]
    (-> entity
        (assoc :pk (:pk entity :id))
        (assoc :fields fields)
        map->RelationalEntity
        (with-meta entity-meta))))

(defmethod create-repository (.-name *ns*) [_ entities & [opts]]
  (let [entity-meta (assoc opts :repository (promise))
        build-entity (partial create-entity entity-meta)]
    (->> entities
         (map (fn [[k v]] [k (build-entity (assoc v :name k))]))
         (into {})
         (#(with-meta % opts))
         (deliver (:repository entity-meta))
         deref)))
