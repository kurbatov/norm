(ns norm.sql
  (:refer-clojure :exclude [find update remove select])
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [norm.core :as core :refer [Query Command Entity where fetch find create-repository]]
            [norm.sql-format :as f]
            [norm.jdbc :refer [as-entity-maps]]))

;; Query builder

(def default-jdbc-opts
  {:return-keys true
   :builder-fn rs/as-unqualified-lower-maps})

(defrecord SQLQuery [db source fields where order offset limit jdbc-opts]
  Query
  (join [this op r-source clause] (SQLQuery. db [source op r-source clause] fields where order offset limit jdbc-opts))
  (where [this clauses] (SQLQuery. db source fields (merge where clauses) order offset limit jdbc-opts))
  (order [this order] (SQLQuery. db source fields where order offset limit jdbc-opts))
  (skip [this amount] (SQLQuery. db source fields where order amount limit jdbc-opts))
  (limit [this amount] (SQLQuery. db source fields where order offset amount jdbc-opts))
  (fetch [this]
    (let [values (cond-> (f/extract-values where)
                   limit (conj limit)
                   offset (conj offset))
          query (apply vector (str this) values)]
      (jdbc/execute! db query (merge default-jdbc-opts jdbc-opts))))
  (fetch [this fields] (fetch (SQLQuery. db source fields where order offset limit jdbc-opts)))
  (fetch-count [this] (-> (SQLQuery. db source [['(count :*) :count]] where nil nil nil jdbc-opts) fetch first :count))
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
  ([db source fields where order offset limit jdbc-opts] (->SQLQuery db source fields where order offset limit jdbc-opts)))

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

(defrecord SQLCommand [type db target values where]
  Command
  (execute [this]
    (let [params (if (vector? values) (flatten (rest values)) (and values (f/extract-values values)))
          params (if where (concat params (f/extract-values where)) params)
          command (if params (apply vector (str this) params) [(str this)])]
      (jdbc/execute-one! db command (when (= :insert type) default-jdbc-opts))))
  Object
  (toString [this] (generate-sql-command this)))

(defn insert
  "Constructs an insert command."
  ^SQLCommand
  [db target values]
  (->SQLCommand :insert db target values nil))

(defn update
  "Constructs an update command."
  ^SQLCommand
  [db target values where]
  (->SQLCommand :update db target values where))

(defn delete
  "Constructs a delete command."
  ^SQLCommand
  [db target where]
  (->SQLCommand :delete db target nil where))

;; Relational entity

(defrecord RelationalEntity [db table name pk fields relations]
  Entity
  (create [this data] (insert db table data))
  (fetch-by-id [this id] (-> (find this {(or pk :id) id}) fetch first))
  (find [this] (find this nil))
  (find [this where]
    (let [source [table name]
          fields (mapv (partial f/prefix name) fields)
          repository @(:repository (meta this) (delay {}))
          eager (->> relations
                     (filter (comp :eager val))
                     (filter (comp #{:has-one :belongs-to} :type val))
                     (filter (comp (partial contains? repository) :entity val))
                     (into {}))
          fields (reduce (fn [result [k v]]
                           (->> (:fields ((:entity v) repository))
                                (map (partial f/prefix (f/prefix name k)))
                                (into result)))
                         fields
                         eager)
          source (reduce (fn [left-src [k v]]
                           (let [entity ((:entity v) repository)
                                 alias (f/prefix name k)
                                 clause (condp = (:type v)
                                          :has-one {(f/prefix name pk) (f/prefix alias (:fk v))}
                                          :belongs-to {(f/prefix name (:fk v)) (f/prefix alias (:pk entity))})]
                             [left-src :left-join [(:table entity) alias] clause]))
                         source
                         eager)
          where (walk/postwalk
                 #(if (and (keyword? %) (not (f/prefixed? name %)))
                    (f/prefix name %)
                    %)
                 where)]
      (select db source fields where nil nil nil {:builder-fn as-entity-maps :entity this})))
  (find-related [this relation-key where]
    (let [repository @(:repository (meta this) (delay {}))
          relation (relation-key relations)
          entity (-> ((:entity relation) repository) (assoc :name relation-key))
          base (core/find entity)
          {:keys [source fields]} base
          clause (condp = (:type relation)
                   :has-one {(f/prefix name pk) (f/prefix relation-key (:fk relation))}
                   :belongs-to {(f/prefix name (:fk relation)) (f/prefix relation-key (:pk entity))}
                   :has-many (if (:join-table relation)
                               {(f/prefix relation-key (:pk entity)) (f/prefix (:join-table relation) (:rfk relation))}
                               {(f/prefix name pk) (f/prefix relation-key (:fk relation))}))
          r-source (if (:join-table relation)
                     [[table name] :left-join (:join-table relation) {(f/prefix name pk) (f/prefix (:join-table relation) (:fk relation))}]
                     [table name])
          source [source :right-join r-source clause]
          where (walk/postwalk
                 #(if (and (keyword? %) (not (namespace %))) (f/prefix name %) %)
                 where)]
      (select db source fields where nil nil nil {:builder-fn as-entity-maps :entity entity})))
  (update [this where patch] (update db table patch where))
  (delete [this where] (delete db table where))
  (with-relations [this new-relations]
    (-> (RelationalEntity. db table name pk fields (merge relations new-relations))
        (with-meta (meta this))))
  (with-eager [this rel-keys]
    (-> (RelationalEntity. db table name pk fields (reduce #(clojure.core/update %1 %2 assoc :eager true) relations rel-keys))
        (with-meta (meta this)))))

;; SQL repository

(defn- build-relational-entity [opts entity-meta name entity]
  (let [fields (->> (select (:db opts)
                            :information-schema/columns
                            [:column-name]
                            {:table-schema [:ilike "public"]
                             :table-name   [:ilike (f/format-keyword (:table entity))]})
                    fetch
                    (mapv (comp ->kebab-case-keyword :column-name)))]
    (-> entity
        (assoc :name name)
        (assoc :pk (:pk entity :id))
        (assoc :fields fields)
        (->> (merge opts))
        map->RelationalEntity
        (with-meta entity-meta))))

(defmethod create-repository (.-name *ns*) [_ entities & [opts]]
  (let [entity-meta {:repository (promise)}
        build-entity (partial build-relational-entity opts entity-meta)]
    (->> entities
         (map (fn [[k v]] [k (build-entity k v)]))
         (into {})
         (#(with-meta % opts))
         (deliver (:repository entity-meta))
         deref)))
