(ns norm.sql.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as st]
            [norm.sql :as sql]))

;; Commands and Queries

(s/def ::identifier keyword?)

(s/def ::alias keyword?)

(s/def ::predicate (s/or :keyword keyword?
                         :symbol symbol?))

(s/def ::value any?)

(s/def ::id-or-val (s/or :id ::identifier :val ::value))

(s/def ::field (s/or :plain            ::id-or-val
                     :stored-procedure (s/cat :fn ::predicate :args (s/* ::id-or-val))
                     :aliased          (s/tuple ::field ::alias)))

(s/def ::fields (s/coll-of ::field))

(s/def ::target ::identifier)

(s/def ::clause (s/map-of keyword? ::value))

(s/def ::where (s/or :simple  ::clause
                     :complex (s/cat :predicate ::predicate
                                     :first-clause ::where
                                     :second-clause ::where)))

(s/def ::join-op #{:join :left-join :right-join :inner-join :cross-join})

(s/def ::source (s/or
                 :id      ::identifier
                 :aliased (s/tuple ::identifier ::alias)
                 :complex (s/tuple ::source ::join-op ::source ::clause)))

(s/def ::order (s/or :ascending (s/coll-of keyword?)
                     :mixed     (s/map-of keyword? #{:asc :desc})))

(s/def ::offset int?)

(s/def ::limit int?)

(s/fdef sql/insert
  :args (s/cat :db (s/nilable any?)
               :target ::target
               :values (s/or :map map? :vector vector?)))

(s/fdef sql/update
  :args (s/cat :db (s/nilable any?)
               :target ::target
               :values map?
               :where ::where))

(s/fdef sql/delete
  :args (s/cat :db (s/nilable any?)
               :target ::target
               :where ::where))

(s/fdef sql/select
  :args (s/alt
         :base (s/cat :db (s/nilable any?)
                      :source ::source)
         :with-fields (s/cat :db (s/nilable any?)
                             :source (s/nilable ::source)
                             :fields (s/nilable ::fields))
         :with-where (s/cat :db (s/nilable any?)
                            :source (s/nilable ::source)
                            :fields (s/nilable ::fields)
                            :where (s/nilable ::where))
         :with-order (s/cat :db (s/nilable any?)
                            :source (s/nilable ::source)
                            :fields (s/nilable ::fields)
                            :where (s/nilable ::where)
                            :order (s/nilable ::order))
         :with-offset (s/cat :db (s/nilable any?)
                             :source (s/nilable ::source)
                             :fields (s/nilable ::fields)
                             :where (s/nilable ::where)
                             :order (s/nilable ::order)
                             :offset (s/nilable ::offset))
         :with-limit (s/cat :db (s/nilable any?)
                            :source (s/nilable ::source)
                            :fields (s/nilable ::fields)
                            :where (s/nilable ::where)
                            :order (s/nilable ::order)
                            :offset (s/nilable ::offset)
                            :limit (s/nilable ::limit))
         :full (s/cat :db (s/nilable any?)
                      :source (s/nilable ::source)
                      :fields (s/nilable ::fields)
                      :where (s/nilable ::where)
                      :order (s/nilable ::order)
                      :offset (s/nilable ::offset)
                      :limit (s/nilable ::limit)
                      :jdbc-opts (s/nilable map?))))

;; Entities

(s/def ::pk keyword?)

(s/def ::table keyword?)

(s/def :entity/fields (s/coll-of keyword?))

(s/def :rel/entity keyword?)

(s/def ::type #{:belongs-to :has-one :has-many})

(s/def ::fk keyword?)

(s/def ::rfk keyword?)

(s/def ::join-table keyword?)

(s/def ::filter ::clause)

(s/def ::prepare fn?)

(s/def ::transform fn?)

(s/def ::eager boolean?)

(s/def ::rel-required (s/keys :req-un [:rel/entity ::type ::fk]))

(s/def ::belongs-to-rel
  (s/and
   ::rel-required
   (s/keys ::opt-un [::eager])
   (comp (partial = :belongs-to) :type)
   #(not (contains? % :filter))
   #(not (contains? % :join-table))
   #(not (contains? % :rfk))))

(s/def ::has-one-rel
  (s/and
   ::rel-required
   (s/keys ::opt-un [::eager])
   (comp (partial = :has-one) :type)
   #(not (contains? % :filter))
   #(not (contains? % :join-table))
   #(not (contains? % :rfk))))

(s/def ::has-many-to-one-rel
  (s/and
   ::rel-required
   (comp (partial = :has-many) :type)
   #(not (contains? % :eager))
   #(not (contains? % :join-table))
   #(not (contains? % :rfk))
   (s/keys :opt-un [::filter])))

(s/def ::has-many-to-many-rel
  (s/and
   ::rel-required
   (s/keys :req-un [::join-table ::rfk]
           :opt-un [::filter])
   #(not (contains? % :eager))))

(s/def ::rel (s/or :belongs-to   ::belongs-to-rel
                   :has-one      ::has-one-rel
                   :many-to-one  ::has-many-to-one-rel
                   :many-to-many ::has-many-to-many-rel))

(s/def ::rels (s/map-of keyword? ::rel))

(s/def ::entity (s/keys :req-un [::table]
                        :opt-un [::pk :entity/fields ::rels ::filter ::prepare ::transform]))

(s/def ::repository (s/map-of keyword? ::entity))

(s/def :meta/db any?)

(s/def :meta/repository any?)

(s/def :meta/schema string?)

(s/def ::entity-meta (s/keys :req-un [:meta/db :meta/repository]
                             :opt-un [:meta/schema]))

(s/fdef sql/create-entity
  :args (s/cat :meta map? :entity ::entity))

(def ^:private fns-with-specs
  [`sql/insert
   `sql/update
   `sql/delete
   `sql/select
   `sql/create-entity])

(defn instrument []
  (st/instrument fns-with-specs))

(defn unstrument []
  (st/unstrument fns-with-specs))