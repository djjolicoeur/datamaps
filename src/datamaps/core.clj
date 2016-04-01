(ns datamaps.core
  (:require [datamaps.query :as d]
            [datamaps.facts :as df])
  (:import [datamaps.facts FactStore] ))



(defn q
  "Query against all non-meta facts.  Same arguments as
   datomic.api/q and should be transparent to the end user.
   Note we can query across our map and a datomic DB here if
   we pass the datomic DB as an additional param"
  [query facts & params]
  {:pre [(satisfies? df/IFactStore facts)]}
  (apply d/q (concat [query facts] params)))

(defn meta-q
  "Query against all attribute meta data facts. Same notes
   as q"
  [query facts & params]
  {:pre [(satisfies? df/IFactStore facts)]}
  (apply d/q (concat [query (df/attr-partition facts)] params)))



(defn- facts*
  "Given a map or a list of maps, generate a table of facts"
  [m]
  (cond
    (map? m) (df/map->facts m)
    (sequential? m) (df/maps->facts m)
    true (throw
          (ex-info "Unsupported Type for generating facts!"
                   {:causes #{:unsupported-type}
                    :type (type m)}))))

(defn facts
  [m]
  (let [facts (facts* m)]
    (FactStore. (df/filter-meta facts) (set (df/filter-not-meta facts)))))


(defn entity->datums
  "Find the datums related to this ID"
  [facts id]
  (q '[:find ?a ?v
       :in $ ?id
       :where
       [?id ?a ?v]]
     facts id))

(declare entity)

(defn datums->map
  "Given a set of facts, and entity ID, and a set of related datums,
   output the map representation of the datums relations.  Recursivly
   rehydrates child maps"
  [facts id datums]
  (if (seq datums)
    (loop [datums (seq datums) out {}]
      (if-let [[a v] (first datums)]
        (let [type (df/attr-meta facts id a)]
          (recur (rest datums)
                 (condp = type
                   df/coll-type (update-in out [a] conj (or (entity facts v) v))
                   df/ref-type (assoc out a (entity facts v))
                   (assoc out a v))))
        out))))

(defn entity
  "Given facts and an entity ID, output the map
   representation of the entity"
  [facts id]
  {:pre [(df/factstore? facts)]}
  (->> id
       (entity->datums facts)
       (datums->map facts id)))

(defn pull
  "Given a fact store, a pattern, and an entity ID,
   output the entity in the given pattern"
  [facts pattern eid]
  (q '[:find (pull ?e pattern) .
       :in $ pattern ?e
       :where [?e _ _]]
     facts pattern eid))
