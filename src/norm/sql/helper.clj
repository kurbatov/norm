(ns norm.sql.helper
  (:require [clojure.string :as str]
            [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [norm.core :as core]
            [norm.sql :refer [select get-db-meta]]))

(def ->key ->kebab-case-keyword)

(defn find-entities
  "Detects entities and their relations in the specified database using
   primary and foreign key constraints.
   
   The result of this function call is supposed to be used as a starting point
   to make a handcrafted entities description.
   
   The result may be used to build a repository directly:
   
   ```
   (require '[norm.core :as norm]
            '[norm.sql.helper :refer [find-entities]])
   
   (def opts
     {:db db
      :schema \"public\"}) ; :schema may be ommitted for autodetection
   
   (def repository
     (norm/create-repository :sql (find-entities opts) opts))
   ```
   
   The autogenerated entity description may suffer from weird naming
   of entities and their relations due to lack of context. It doesn't
   handle pluralisation of irregular nouns (such as `person` - `people`).
   It may miss relations when an entity references another entity more than once.
   For example, `employee` `:has-many` `supervisors` and `subordinants` which
   are `employee`s themselves - in such case the later encountered relations overrides
   the former one under the name `employees` because the function lacks our knowlege of
   semantics of those relations.
   
   If foreign key constraints are missing, relations are not detected.
   However it is possible to build a relation using unconstrained column
   with handwritten entity description."
  [{:keys [db schema]}]
  (let [db-meta (get-db-meta db)
        db-type (.getDatabaseProductName db-meta)
        schema (or schema
                   (->> (select db nil [['(current-schema) :current-schema]])
                        core/fetch!
                        first
                        :current-schema))
        tables (->> (select db
                            :information-schema/tables
                            [:table-name]
                            {:table-schema [:ilike schema]})
                    core/fetch!
                    (mapv :table-name))
        tc [:information-schema/table-constraints :tc]
        kcu [:information_schema/key_column_usage :kcu]
        pks (-> (select db
                        [tc :join kcu {:tc/constraint-schema :kcu/constraint-schema :tc/constraint-name :kcu/constraint-name}]
                        [[:kcu/table-name :table-name] ['(string-agg :kcu/column-name ",") :column-list]]
                        {:tc/constraint-type [:ilike "primary key"]
                         :kcu/table-schema [:ilike schema]})
                (assoc :group-by [:kcu/table-name])
                core/fetch!
                (->> (map (juxt :table-name :column-list))
                     (into {})))
        rc [:information-schema/referential-constraints :rc]
        ccu [:information_schema/constraint_column_usage :ccu]
        refs (-> db
                 (select [[rc :join kcu {:rc/constraint-schema :kcu/constraint-schema :rc/constraint-name :kcu/constraint-name}]
                          :join ccu {:rc/constraint-schema :ccu/constraint-schema :rc/unique-constraint-name :ccu/constraint-name}]
                         [[:kcu/table-name :fktable-name]
                          [:kcu/column-name :fkcolumn-name]
                          [:ccu/table-name :pktable-name]
                          [:ccu/column-name :pkcolumn-name]]
                         {:rc/constraint-schema [:ilike schema]})
                 core/fetch!)
        join-tables (->> tables (filter (comp #(or (nil? %) (str/includes? % ",")) pks)) (into #{}))
        belongs-to (->> refs
                        (map (juxt :fktable-name
                                   (fn [ref]
                                     {:type :belongs-to
                                      :entity (->key (:pktable-name ref))
                                      :fk (->key (:fkcolumn-name ref))})))
                        (reduce #(clojure.core/update %1 (first %2) conj (second %2)) {}))
        has (->> refs
                 (map (juxt :pktable-name
                            (fn [ref]
                              (let [table (:fktable-name ref)
                                    column (:fkcolumn-name ref)
                                    join-table (some-> table join-tables ->key)
                                    relation (when join-table
                                               (->> (get belongs-to table)
                                                    (filter (comp (partial not= (->key (:pktable-name ref))) :entity))
                                                    first))]
                                (cond-> {:type (if (= (get pks table) column) :has-one :has-many) ;TODO :has-one when "unique" constraint
                                         :entity (if relation (:entity relation) (->key table))
                                         :fk (->key column)}
                                  join-table (assoc :join-table join-table)
                                  relation (assoc :rfk (:fk relation)))))))
                 (reduce #(clojure.core/update %1 (first %2) conj (second %2)) {}))
        create-rel-name #(cond
                           (and (= :belongs-to (:type %)) (str/ends-with? (name (:fk %)) "-id"))
                           (->> (:fk %) name (drop-last 3) str/join keyword)
                           (#{:has-one :belongs-to} (:type %))
                           (->> (:entity %) name drop-last str/join keyword)
                           :else (:entity %))]
    (->> tables
         (filter (complement join-tables))
         (map (fn [t]
                {:table (->key t)
                 :pk (->key (get pks t))
                 :rels (->> (get has t)
                            (concat (get belongs-to t))
                            (reduce #(assoc %1 (create-rel-name %2) %2) {}))}))
         (map #(if (empty? (:rels %)) (dissoc % :rels) %))
         (reduce #(assoc %1 (:table %2) %2) {}))))