(ns norm.sql.format
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [camel-snake-kebab.core :refer [->snake_case_string]]
            [norm.core :as core]))

;; Formatting

(defn prefix [ns-keyword name-keyword]
  (keyword
   (str
    (when-let [n (namespace ns-keyword)] (str n "."))
    (name ns-keyword)
    (when-let [n (namespace name-keyword)] (str "." n)))
   (name name-keyword)))

(defn prefixed? [prefix key]
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

(defn sql-quote [x] (str \" x \"))
(defn wrap [x] (str "(" x ")"))
(defn infix [k op v] (str/join " " [k op v]))
(defn group [op vs] (wrap (str/join op vs)))
(defn wrapper [op v] (str op (wrap v)))
(defn ternary [k op v1 sep v2] (wrap (str/join " " [k op v1 sep v2])))

(defn format-keyword [x] (cond->> (->snake_case_string (name x))
                           (namespace x) (str (->snake_case_string (namespace x)) ".")))

(defn format-keyword-quoted [x] (cond->> (->snake_case_string (name x))
                                  (namespace x) (str (sql-quote (->snake_case_string (namespace x))) ".")))

(defn format-value [x]
  (cond
    (nil? x) "NULL"
    (boolean? x) x
    (keyword? x) (format-keyword-quoted x)
    (satisfies? core/Query x) (wrap (str x))
    :else "?"))

(defn format-alias [x]
  (if (keyword? x)
    (if (namespace x)
      (str/join "/" ((juxt namespace name) x))
      (name x))
    x))

(defn format-field
  ([field]
   (cond
     (keyword? field) (format-keyword-quoted field)
     (list? field) (if (contains? predicates (first field))
                     (wrap (apply (get predicates (first field)) (format-field (second field)) (map format-value (nnext field))))
                     (wrapper (str/upper-case (->snake_case_string (first field))) (format-field (second field))))
     (vector? field) (apply format-field field)
     :else field))
  ([field alias]
   (infix (format-field field) "AS" (sql-quote (format-alias alias)))))

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
                      'not=    pred-not=
                      '=       pred-=}
        predicates-k (into {} (map (fn [[k v]] {(keyword k) v}) predicates-s))]
    (merge predicates-s predicates-k)))

;; Clause

(defn- convert-clause-map-entry [[k v]]
  (if (and (fn? k) (map? v))
    (apply k (map convert-clause-map-entry v))
    (if (coll? v)
      (if (fn? (first v))
        (apply (first v) (format-field k) (map format-value (rest v)))
        (apply pred-in (format-field k) (map format-value v)))
      (pred-= (format-field k) (format-value v)))))

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
  ([x y] (if (and x y) (list 'and x y) (or x y)))
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
    (if (keyword? source) (format-keyword source) (wrap source))
    "AS" (sql-quote (format-keyword alias))))
  ([left op right clause]
   (let [op (str/upper-case (str/replace (name op) "-" " "))]
     (ternary (format-source left) op (format-source right) "ON" (format-clause clause)))))

(defn format-target [target] (format-keyword target))

;; Order

(defn format-order
  ([order]
   (cond
     (vector? order) (apply format-order order)
     (map? order) (->> order (map (fn [[k v]] (str (format-field k) " " (name v)))) (str/join ", "))
     (keyword? order) (format-field order)
     :else order))
  ([order & more]
   (str/join ", " (apply vector (format-order order) (map format-order more)))))

;; Values

(declare extract-values)

(defn- extract-value [v]
  (cond
    (map? v) (map extract-value v)
    (and (coll? v) (keyword? (first v))) (->> (rest v) (mapv extract-value))
    (satisfies? core/Query v) (cond-> (extract-values (:where v))
                                (:limit v) (conj (:limit v))
                                (:offset v) (conj (:offset v)))
    :else v))

(defn extract-values [clause]
  (cond
    (map? clause) (->> clause
                       (map (comp extract-value val))
                       flatten
                       (filter some?)
                       (filter (complement boolean?))
                       (filter (complement keyword?))
                       (into []))
    (or (vector? clause)
        (list? clause)) (->> clause (map extract-values) flatten (into []))
    :else []))
