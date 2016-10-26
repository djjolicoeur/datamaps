(ns datamaps.facts
  (:require [datascript.query :as dq]))

;; Due to the unstsructured nature of map keys (i.e. no attribute constraints),
;; each generated entity will get its own attribute meta data for its keys.
;; this allows us to have duplicate keys in child entities with
;; different semantics and not lose the ability to rebuild the
;; entity/map from the facts (see entity and datums->map).

;; Attribute Types -- namespaced keywords

(def coll-type ::coll)
(def val-type  ::val)
(def ref-type  ::ref)
(def reverse-ref-type ::reverse-ref)

(def attr-types #{coll-type val-type ref-type reverse-ref-type})


;; Define a protocol around splitting out fact datums and attribute
;; datums

(defprotocol IFactStore
  (fact-partition [this] "Entity related facts")
  (reverse-partition [this] "Reverse relational lookups")
  (attr-partition [this]  "Attribute (key) metadata"))


(defn factstore? [v]
  (satisfies? IFactStore v))

(defrecord FactStore [fact-partition attr-partition reverse-partition]
  IFactStore
  (fact-partition [this] fact-partition)
  (attr-partition [this] attr-partition)
  (reverse-partition [this] reverse-partition))

(defn- reverse-attr? [meta [e a v]]
  (let [a-type (dq/q '[:find ?v .
                       :in $ ?e ?a
                       :where [?e ?a ?v]]
                     meta e a)]
    (identical? ::reverse-ref a-type)))

(defn- fact? [meta [e a v]]
  (and (not (attr-types v))
       (not (reverse-attr? meta [e a v]))))

(defn- reverse? [meta [e a v]]
  (and (not (attr-types v))
       (reverse-attr? meta [e a v])))


(defn- raw-facts->meta
  "Find all facts that are attribute metadata"
  [facts]
  (set (filter (fn [[e a v]] (attr-types v)) facts)))

(defn- raw-facts->facts
  [meta facts]
  (filter (partial fact? meta) facts))

(defn- raw-facts->reverse
  [meta facts]
  (filter (partial reverse? meta) facts))

(defn raw-facts->fact-store [raw-facts]
  (let [meta (raw-facts->meta raw-facts)
        facts (raw-facts->facts meta raw-facts)
        reverse (raw-facts->reverse meta raw-facts)]
    (->FactStore facts meta reverse)))


;; Functions for converting maps to fact tuples

(declare map->facts)
(declare map-entry->fact)

(defn- coll->facts
  "Handles converting collections to facts, including collections
   of maps"
  [id k coll]
  (->> (map (fn [v] [k v]) coll)
       (map (partial map-entry->fact true id))
       (reduce concat)
       (cons [id k coll-type])))

(defn- map-entry->fact
  "Generate a fact given an id and [key value] tuple.
   the tuple may contain nested structures"
  ([is-child? id [k v]]
   (cond (map? v) (map->facts id k v)
         (sequential? v) (coll->facts id k v)
         (set? v) (coll->facts id k v)
         is-child? [[id k v]]
         v [[id k v] [id k val-type]]
         true nil))
  ([id [k v]]
   (map-entry->fact nil id [k v])))

(defn reverse-index-kw [k]
  (keyword (namespace k) (str "_" (name k))))

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
          (reduce concat))))
  ([parent key m]
   (let [id (java.util.UUID/randomUUID)
         reverse-key (reverse-index-kw key)]
     (->> (seq m)
          (mapcat (partial map-entry->fact id))
          (filter identity)
          (cons [parent key id])
          (cons [id reverse-key parent])
          (cons [id reverse-key reverse-ref-type])
          (cons [parent key ref-type])))))


(defn maps->facts
  "Application of map->facts to a sequence of maps to
   create a set of facts"
  [ms]
  (mapcat map->facts ms))

;; Functions for retrieving entities by their generated ID

(defn attr-meta
  "Find the attributes meta data => (::coll | ::ref | ::val | ::reverse-ref)"
  [facts id attr]
  (dq/q '[:find ?type .
          :in $ ?id ?attr
          :where
          [?id ?attr ?type]]
        (attr-partition facts) id attr))
