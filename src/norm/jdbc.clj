(ns norm.jdbc
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [next.jdbc.result-set :refer [RowBuilder ResultSetBuilder read-column-by-index]]
            [norm.core :refer [instance-meta]])
  (:import [java.sql ResultSet ResultSetMetaData]
           [java.util Locale]))

(defn- assoc-in!
  "Associates a value in a transient nested associative structure, where ks is
  a sequence of keys and v is the new value and returns the same transient structure.
  If any levels do not exist, transient maps will be created."
  {:static true}
  [m [k & ks] v]
  (if ks
    (assoc! m k (assoc-in! (or (get m k) (transient {})) ks v))
    (assoc! m k v)))

(def ^:private transient? (partial instance? clojure.lang.ITransientCollection))

(defn- persist! [x] (if (transient? x) (persistent! x) x))

(defn- map-entry [k v] (clojure.lang.MapEntry/create k v))

(defn- wrap-relations [relations repository x]
  (cond
    (and (map-entry? x) (map? (val x)) ((key x) relations))
    (if-let [entity (get repository (:entity ((key x) relations)))]
      (map-entry
       (key x)
       (with-meta (val x) (merge instance-meta {:entity entity})))
      x)
    :else x))

(defrecord EntityResultSetBuilder [^ResultSet rs rsmeta cols entity]
  RowBuilder
  (->row [this] (transient {}))
  (column-count [this] (count cols))
  (with-column [this row i]
    (let [value (read-column-by-index (.getObject rs ^Integer i) rsmeta i)]
      (if (nil? value) row (assoc-in! row (nth cols (dec i)) value))))
  (row! [this row] (walk/prewalk persist! row)); postwalk cannot traverse transient maps
  ResultSetBuilder
  (->rs [this] (transient []))
  (with-row [this mrs row]
    (let [repository @(or (:repository (meta entity)) (delay {}))
          wrap-relations (partial wrap-relations (:relations entity) repository)]
      (conj! mrs
             (-> (walk/postwalk wrap-relations row)
                 (with-meta (merge instance-meta {:entity entity}))))))
  (rs! [this mrs] (persistent! mrs)))

(defn get-column-names
  "Given `ResultSetMetaData`, return a vector of column names.
  Each column name is a vector of keywords itself.
  The column name vector determines nesting of the value from
  the result set in a row that `RowBuilder` makes."
  [^ResultSetMetaData rsmeta exclude]
  (->> (range 1 (inc (.getColumnCount rsmeta)))
       (map (fn [^Integer i] (.toLowerCase (.getColumnLabel rsmeta i) (Locale/US))))
       (map #(str/split % #"[\./]"))
       (map #(if (= exclude (first %)) (rest %) %))
       (mapv (partial mapv keyword))))

(defn as-entity-maps
  "Given a `ResultSet` and options, return a `RowBuilder` / `ResultSetBuilder`
  that produces bare vectors of hash map rows. Maps are nested. Nesting depends
  on a column's label in the result set."
  [^ResultSet rs opts]
  (let [{:keys [entity]} opts
        rsmeta (.getMetaData rs)
        cols   (get-column-names rsmeta (name (:name entity)))]
    (->EntityResultSetBuilder rs rsmeta cols entity)))
