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

(s/def ::field (s/or :id         ::identifier
                     :addregated (s/tuple symbol? ::identifier)
                     :calculated (s/tuple symbol? ::identifier ::value)
                     :alliased   (s/tuple ::field ::alias)))

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
  :args (s/or
         :base (s/cat :db (s/nilable any?)
                      :source ::source)
         :with-fields (s/cat :db (s/nilable any?)
                             :source ::source
                             :fields (s/nilable ::fields))
         :with-where (s/cat :db (s/nilable any?)
                            :source ::source
                            :fields (s/nilable ::fields)
                            :where (s/nilable ::where))
         :with-order (s/cat :db (s/nilable any?)
                            :source ::source
                            :fields (s/nilable ::fields)
                            :where (s/nilable ::where)
                            :order (s/nilable ::order))
         :with-offset (s/cat :db (s/nilable any?)
                             :source ::source
                             :fields (s/nilable ::fields)
                             :where (s/nilable ::where)
                             :order (s/nilable ::order)
                             :offset (s/nilable ::offset))
         :with-limit (s/cat :db (s/nilable any?)
                            :source ::source
                            :fields (s/nilable ::fields)
                            :where (s/nilable ::where)
                            :order (s/nilable ::order)
                            :offset (s/nilable ::offset)
                            :limit (s/nilable ::limit))
         :full (s/cat :db (s/nilable any?)
                      :source ::source
                      :fields (s/nilable ::fields)
                      :where (s/nilable ::where)
                      :order (s/nilable ::order)
                      :offset (s/nilable ::offset)
                      :limit (s/nilable ::limit)
                      :jdbc-opts (s/nilable map?))))

;; Entities

(s/def ::pk keyword?)

(s/def ::table keyword?)

(s/def :rel/entity keyword?)

(s/def ::type #{:belongs-to :has-one :has-many})

(s/def ::fk keyword?)

(s/def ::join-table keyword?)

(s/def ::filter ::clause)

(s/def ::relation (s/keys :req-un [:rel/entity ::type ::fk]
                          :opt-un [::join-table ::filter]))

(s/def ::relations (s/map-of keyword? ::relation))

(s/def ::entity (s/keys :req-un [::table]
                        :opt-un [::pk ::relations ::filter]))

(s/def ::repository (s/map-of keyword? ::entity))

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