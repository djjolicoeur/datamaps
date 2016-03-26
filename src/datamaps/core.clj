(ns datamaps.core
  (:require [datomic.api :as d]))


;; Due to the unstsructured nature of map keys (i.e. no attribute constraints),
;; each generated entity will get its own attribute meta data for its keys.
;; this allows us to have duplicate keys in child entities with
;; different semantics and not lose the ability to rebuild the
;; entity/map from the facts (see entity and datums->map).

(defn filter-meta
  "Find all facts that are not attribute meta data"
  [facts]
  (filter (fn [[e a v]] (not (#{:coll :ref :val} v))) facts))

(defn filter-not-meta
  "Find all facts that are attribute metadata"
  [facts]
  (filter (fn [[e a v]] (#{:coll :ref :val} v)) facts))

(defn q
  "Query against all non-meta facts.  Same arguments as
   datomic.api/q and should be transparent to the end user"
  [query facts & params]
  (apply d/q (concat [query (filter-meta facts)] params)))

(defn meta-q
  "Query against all attribute meta data facts. Same notes
   as q"
  [query facts & params]
  (apply d/q (concat [query (filter-not-meta facts)] params)))


;; Functions for converting maps to fact tuples

(declare map->facts)
(declare map-entry->fact)

(defn coll->facts
  "Handles converting collections to facts, including collections
   of maps"
  [id k coll]
  (->> (map (fn [k v] [k v]) (take (count coll) (repeat k)) coll)
       (map (partial map-entry->fact true id))
       (reduce concat)
       (cons [id k :coll])))

(defn map-entry->fact
  "Generate a fact given an id and [key value] tuple.
   the tuple may contain nested structures"
  ([is-child? id [k v]]
   (cond (map? v) (map->facts id k v)
         (sequential? v) (coll->facts id k v)
         (set? v) (coll->facts id k v)
         is-child? [[id k v]]
         v [[id k v] [id k :val]]
         true nil))
  ([id [k v]]
   (map-entry->fact nil id [k v])))

(defn map->facts
  "takes a single map and converts it to a set of
   three-tuples  of [<generated id> <key> <value>]. Nested
   maps well be assoicated by
   [<parents generated id> <key> <generated id of child>], including
   maps nested within a collections. Collections will imply a many
   relation"
  ([m]
   (let [id (java.util.UUID/randomUUID)]
     (->> (seq m)
          (map (partial map-entry->fact id))
          (filter identity)
          (reduce concat)
          set)))
  ([parent key m]
   (let [id (java.util.UUID/randomUUID)]
     (->> (seq m)
          (mapcat (partial map-entry->fact id))
          (filter identity)
          (cons [parent key id])
          (cons [parent key :ref])))))


(defn maps->facts
  "Application of map->facts to a sequence of maps to
   create a set of facts"
  [ms]
  (set (mapcat map->facts ms)))


(defn facts
  "Given a map or a list of maps, generate a table of facts"
  [m]
  {:pre [(or (map? m) (sequential? m))]}
  (cond
    (map? m) (map->facts m)
    (sequential? m) (maps->facts m)
    true (throw
          (ex-info "Unsupported Type for generating facts!"
                   {:causes #{:unsupported-type}
                    :type (type m)}))))

;; Functions for retrieving entities by their generated ID

(defn attr-meta
  "Find the attributes meta data => (:coll | :ref | :val)"
  [facts id attr]
  (meta-q '[:find ?type .
            :in $ ?id ?attr
            :where
            [?id ?attr ?type]]
          facts id attr))

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
        (let [type (attr-meta facts id a)]
          (recur (rest datums)
                 (cond
                   (= type :coll) (update-in out [a] conj (or (entity facts v) v))
                   (= type :ref) (assoc out a (entity facts v))
                   :else (assoc out a v))))
        out))))

(defn entity
  "Given a map and an entity ID, output the map
   representation of the entity"
  [facts id]
  (->> id
       (entity->datums facts)
       (datums->map facts id)))
