(ns datamaps.facts)

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

(defn- reverse-attr? [meta [e a _]]
  (let [a-type (some (fn [[me ma mv]]
                       (when (and (= me e) (= ma a))
                         mv))
                     meta)]
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


;; ID Generation

(defprotocol IGenId
  (take-id [_] "Atomic, unique ID"))

(defrecord GenId [atomic-counter]
  IGenId
  (take-id [_] (swap! atomic-counter inc)))

;; Functions for converting maps to fact tuples

(declare map->facts)
(declare map-entry->fact)

(defn- coll->facts
  "Handles converting collections to facts, including collections
   of maps"
  [id-gen id k coll]
  (->> (map (fn [v] [k v]) coll)
       (map (partial map-entry->fact id-gen true id))
       (reduce concat)
       (cons [id k coll-type])))

(defn- map-entry->fact
  "Generate a fact given an id and [key value] tuple.
   the tuple may contain nested structures"
  ([id-gen is-child? id [k v]]
   (cond (map? v) (map->facts id-gen id k v)
         (sequential? v) (coll->facts id-gen id k v)
         (set? v) (coll->facts id-gen id k v)
         is-child? [[id k v]]
         v [[id k v] [id k val-type]]
         true nil))
  ([id-gen id [k v]]
   (map-entry->fact id-gen nil id [k v])))

(defn reverse-index-kw [k]
  (keyword (namespace k) (str "_" (name k))))

(defn map->facts
  "takes a single map and converts it to a set of
   three-tuples  of [<generated id> <key> <value>]. Nested
   maps well be assoicated by
   [<parents generated id> <key> <generated id of child>], including
   maps nested within a collections. Collections will imply a many
   relation"
  ([m] (map->facts (->GenId (atom 0)) m))
  ([id-gen m]
   (let [id (take-id id-gen)]
     (->> (seq m)
          (map (partial map-entry->fact id-gen id))
          (filter identity)
          (reduce concat))))
  ([id-gen parent key m]
   (let [id (take-id id-gen)
         reverse-key (reverse-index-kw key)]
     (->> (seq m)
          (mapcat (partial map-entry->fact id-gen id))
          (filter identity)
          (cons [parent key id])
          (cons [id reverse-key parent])
          (cons [id reverse-key reverse-ref-type])
          (cons [parent key ref-type])))))


(defn maps->facts
  "Application of map->facts to a sequence of maps to
   create a set of facts"
  [ms]
  (let [id-gen (->GenId (atom 0))]
    (mapcat (partial map->facts id-gen) ms)))

;; Functions for retrieving entities by their generated ID

(defn attr-meta
  "Find the attribute metadata => (::coll | ::ref | ::val | ::reverse-ref)
   Prefers collection metadata when multiple markers exist for the same attribute."
  [facts id attr]
  (let [types (keep (fn [[eid a type]]
                      (when (and (= eid id) (= a attr))
                        type))
                    (attr-partition facts))
        preferred [coll-type ref-type reverse-ref-type val-type]]
    (some (set types) preferred)))

(defn entity-exists?
  "True when the fact partition contains any datoms for eid."
  [facts eid]
  (some (fn [[e _ _]] (= e eid)) (fact-partition facts)))

(defn entity-datoms
  "Return all datoms for eid from the fact partition."
  [facts eid]
  (filter (fn [[e _ _]] (= e eid)) (fact-partition facts)))

(defn entity-attrs
  "Return the set of attributes present for eid."
  [facts eid]
  (->> (entity-datoms facts eid)
       (map second)
       set))

(defn attr-values
  "Return all values for eid/attr from the fact partition preserving duplicates."
  [facts eid attr]
  (->> (fact-partition facts)
       (keep (fn [[e a v]]
               (when (and (= e eid) (= a attr))
                 v)))))

(defn first-attr-value
  "Return the first value for eid/attr or nil."
  [facts eid attr]
  (first (attr-values facts eid attr)))

(defn reverse-value
  "Return the single entity id referenced by the reverse attribute, if present."
  [facts eid reverse-attr]
  (some (fn [[e a v]]
          (when (and (= e eid) (= a reverse-attr))
            v))
        (reverse-partition facts)))
