(ns norm.sql.format
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [camel-snake-kebab.core :refer [->snake_case_string]]
            [norm.core :as core]))

;; Formatting

(defn prefix
  "Prefixes `name-keyword` with `ns-keyword`."
  [ns-keyword name-keyword]
  (keyword
   (str
    (when-let [n (namespace ns-keyword)] (str n "."))
    (name ns-keyword)
    (when-let [n (namespace name-keyword)] (str "." n)))
   (name name-keyword)))

(defn prefixed?
  "Determines if the `key` is prefixed with the `prefix`."
  [prefix key]
  (str/starts-with?
   (or (namespace key) "")
   (str (when-let [n (namespace prefix)] (str n ".")) (name prefix))))

(declare predicates)

(defn ensure-prefixed [pfix coll]
  (walk/postwalk
   #(if (and (keyword? %) (not (contains? predicates %)) (not (prefixed? pfix %)))
      (prefix pfix %)
      %)
   coll))

(def ^:dynamic *->db-case*
  "Transforms an identifier to comply to database naming convention."
  ->snake_case_string)

(defn sql-quote [x] (str \" x \"))
(defn wrap [x] (str "(" x ")"))
(defn infix [k op v] (str/join " " [k op v]))
(defn group [op vs] (wrap (str/join op vs)))
(defn wrapper [op v] (str op (wrap v)))
(defn ternary [k op v1 sep v2] (wrap (str/join " " [k op v1 sep v2])))

(defn format-target [x]
  (*->db-case*
   (if (namespace x)
     (str (namespace x) "." (name x))
     (name x))))

(defn format-keyword-quoted [x]
  (*->db-case*
   (if (namespace x)
     (str (sql-quote (namespace x)) "." (name x))
     (name x))))

(declare format-field)

(defn format-proc-call [x]
  (wrapper (str/upper-case (*->db-case* (first x))) (->> (rest x) (map format-field) (str/join ", "))))

(defn format-alias [x]
  (if (keyword? x)
    (if (namespace x)
      (str (namespace x) "/" (name x))
      (name x))
    x))

(defn format-field
  ([field]
   (cond
     (nil? field) "NULL"
     (boolean? field) field
     (keyword? field) (format-keyword-quoted field)
     (list? field) (if (contains? predicates (first field))
                     (apply (get predicates (first field)) (map format-field (next field)))
                     (format-proc-call field))
     (vector? field) (apply format-field field)
     (satisfies? core/Query field) (wrap (str field))
     :else "?"))
  ([field alias]
   (let [formatted (format-field field)
         formatted (if (and (not= (last formatted) \)) (str/includes? formatted " "))
                     (wrap formatted)
                     formatted)]
     (infix formatted "AS" (sql-quote (format-alias alias))))))

;; Predicates

(defn pred-= [k v]
  (if (or (nil? v) (= "NULL" v) (boolean? v))
    (infix k "IS" v)
    (infix k "=" v)))

(defn pred-not= [k v]
  (if (or (nil? v) (= "NULL" v) (boolean? v))
    (infix k "IS NOT" v)
    (infix k "<>" v)))

(defn pred-in [k & v]     (infix k "IN" (group ", " v)))
(defn pred-not-in [k & v] (infix k "NOT IN" (group ", " v)))
(defn pred-> [k v]        (infix k ">" v))
(defn pred-< [k v]        (infix k "<" v))
(defn pred->= [k v]       (infix k ">=" v))
(defn pred-<= [k v]       (infix k "<=" v))
(defn pred-+ [& args]     (group " + " args))
(defn pred-- [& args]     (group " - " args))
(defn pred-* [& args]     (group " * " args))
(defn pred-div [& args]   (group " / " args))
(defn pred-and [& args]   (group " AND " args))
(defn pred-or [& args]    (group " OR " args))
(defn pred-not [v]        (wrapper "NOT" v))
(defn pred-like [k v]     (infix k "LIKE" v))
(defn pred-ilike [k v]    (infix k "ILIKE" v))
(defn pred-exists [v]     (wrapper "EXISTS" v))
(defn pred-between [k v1 v2] (ternary k "BETWEEN" v1 "AND" v2))

(def predicates
  (let [predicates-s {'like    pred-like
                      'ilike   pred-ilike
                      'and     pred-and
                      'or      pred-or
                      'not     pred-not
                      'in      pred-in
                      'exists  pred-exists
                      'not-in  pred-not-in
                      'between pred-between
                      '>       pred->
                      '<       pred-<
                      '>=      pred->=
                      '<=      pred-<=
                      '+       pred-+
                      '-       pred--
                      '*       pred-*
                      '/       pred-div
                      'not=    pred-not=
                      '=       pred-=}
        predicates-f {>    pred->
                      <    pred-<
                      >=   pred->=
                      <=   pred-<=
                      +    pred-+
                      -    pred--
                      *    pred-*
                      /    pred-div
                      not= pred-not=
                      =    pred-=}
        predicates-k (into {} (map (fn [[k v]] {(keyword k) v}) predicates-s))]
    (merge predicates-s predicates-f predicates-k)))

;; Clause

(defn- convert-clause-map-entry [[k v]]
  (if (and (fn? k) (map? v))
    (apply k (map convert-clause-map-entry v))
    (if (coll? v)
      (if (fn? (first v))
        (apply (first v) (format-field k) (map format-field (rest v)))
        (apply pred-in (format-field k) (map format-field v)))
      (pred-= (format-field k) (format-field v)))))

(defn- convert-clause-map [x]
  (if (map? x)
    (->> x
         (map convert-clause-map-entry)
         (apply pred-and))
    x))

(defn- convert-clause-list [x]
  (if (and (list? x) (fn? (first x)))
    (apply (first x) (rest x))
    x))

(defn format-clause [clause]
  (->> clause
       (walk/postwalk-replace predicates)
       (walk/prewalk convert-clause-map)
       (walk/postwalk convert-clause-list)))

(defn conjunct-clauses
  ([x] x)
  ([x y]
   (if (and (not-empty x) (not-empty y))
     (list 'and x y)
     (or (not-empty x) (not-empty y))))
  ([x y & more] (apply conjunct-clauses (conjunct-clauses x y) more)))

;; Source and target

(defn format-source
  ([source]
   (cond
     (keyword? source) (format-source source source)
     (vector? source) (apply format-source source)
     :else source))
  ([source alias]
   (infix
    (if (keyword? source) (format-target source) (wrap source))
    "AS" (sql-quote (format-target alias))))
  ([left op right clause]
   (let [op (str/upper-case (str/replace (name op) "-" " "))]
     (ternary (format-source left) op (format-source right) "ON" (format-clause clause)))))

;; Order

(defn format-order
  ([order]
   (cond
     (vector? order) (apply format-order order)
     (map? order) (->> order (map (fn [[k v]] (str (format-field k) " " (str/upper-case (name v))))) (str/join ", "))
     (keyword? order) (format-field order)
     :else order))
  ([order & more]
   (str/join ", " (apply vector (format-order order) (map format-order more)))))

;; Values

(declare extract-values)

(defn- extract-value [v]
  (cond
    (satisfies? core/Query v) (cond-> (extract-values (:fields v))
                                (:where v) (into (extract-values (:where v)))
                                (:having v) (into (extract-values (:having v)))
                                (:limit v) (conj (:limit v))
                                (:offset v) (conj (:offset v)))
    (map-entry? v) (extract-value (val v))
    (or (map? v) (coll? v)) (map extract-value v)
    :else v))

(defn extract-values [clause]
  (->> (if (or (map? clause) (coll? clause)) clause [clause])
       (map extract-value)
       flatten
       (filter some?)
       (filter (complement boolean?))
       (filter (complement keyword?))
       (filter (complement symbol?))
       (into [])))
