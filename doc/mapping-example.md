An example mapping may look like this:

```clojure
(def mapping
  {:address {:table :addresses} ; a simplest entity
   :person {:table :people
            :rels
            {:home-address {:type :belongs-to
                            :entity :address
                            :fk :home-address-id}
             ; a second relation to the same entity with different semantics
             :work-address {:type :belongs-to
                            :entity :address
                            :fk :work-address-id}
             ; a self-relation through a :join-table (many-to-many relationship)
             :friends {:type :has-many
                       :entity :person
                       :fk :person-id
                       :join-table :person-friends
                       :rfk :friend-id}}}
   :employee {:table :employees
              :pk :id
              :rels
              {:person {:type :belongs-to
                        :entity :employee
                        ; :fk is the :pk at the same time (one-to-one relationship)
                        :fk :id
                        :eager true}
               ; a recurrent relation
               :supervisor {:type :belongs-to
                            :entity :employee
                            :fk :supervisor-id}
               :responsibilities {:type :has-many
                                  :entity :responsibility
                                  :fk :employee-id
                                  :join-table :employees-resp
                                  :rfk :responsibility-id
                                  :filter {:active true}}}}
   :responsibilities {:table :responsibilities
                      :rels
                      {:employees {:type :has-many
                                   :entity :employee
                                   :fk :responsibility-id
                                   :join-table :employees-resp
                                   :rfk :employee-id
                                   :filter {:active true}}}}
   :user {:table :users
          :pk :uid
          :rels
          {:person {:type :belongs-to
                    :entity :person
                    :fk :person-id}}}
   :user-secret {:table :secrets
                 :pk :uid
                 :rels
                 {:owner {:type :belongs-to
                                :entity :user
                                :fk :uid}}}})
```