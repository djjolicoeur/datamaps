(ns datamaps.core
  (:require [datamaps.query :as d]
            [datamaps.facts :as df]
            [clojure.pprint :as pprint])
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
  "Convert a map or seq of maps into a factstore"
  [m]
  (df/raw-facts->fact-store (facts* m)))

(defn id->entity
  "Find entity for id, if id exists"
  [facts id]
  (when (df/entity-exists? facts id)
    id))

(defn entity->datums
  "Find the datums related to this ID"
  [facts id]
  (->> (df/entity-datoms facts id)
       (map (fn [[_ a v]] [a v]))))

(declare entity entity-lookup resolve-coll resolve-ref)

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
                   df/coll-type (resolve-coll a out facts v)
                   df/ref-type (resolve-ref a out facts v)
                   (assoc out a v))))
        out))))

(defn- entity-keys
  "find all attribute keys for a given entity"
  [e]
  (df/entity-attrs @(.-facts e) (.-id e)))


(defn- touch-entity
  "Given facts and an entity ID, output the map
   representation of the entity"
  [e]
  (let [e-keys (entity-keys e)]
    (-> (reduce (fn [m k]
                  (if-let [v (entity-lookup e k)] (assoc m k v) m))
                {} e-keys)
        (assoc :db/id (.-id e)))))

(defprotocol IEntity
  (->map [e] "Reconstitute entity to it's origanl map")
  (touch [e] "Analogous to datomic's datomic.api/touch"))

;; Entity Impl

(deftype Entity [facts id touched]
  IEntity
  (->map [e]
    (let [fs @facts]
      (->> id
           (entity->datums fs)
           (datums->map fs id))))
  (touch [e]
    (if touched e
        (Entity. facts id (touch-entity e))))

  Object
  (toString [_]
    (if touched
      (str touched)
      (str {:db/id id})))
  clojure.lang.Seqable
  (seq [e]
    (seq (touch-entity e)))

  clojure.lang.Associative
  (equiv [e o]
    (and (instance? Entity o) (= (.-id e) (.-id o))))
  (containsKey [e k]
    (not= ::nf (entity-lookup e k ::nf)))
  (entryAt [e k]
    (some->> (entity-lookup e k) (clojure.lang.MapEntry. k)))

  (empty [e]         (throw (UnsupportedOperationException.)))
  (assoc [e k v]     (throw (UnsupportedOperationException.)))
  (cons  [e [k v]]   (throw (UnsupportedOperationException.)))
  (count [e]         (count  (touch-entity e)))

  clojure.lang.ILookup
  (valAt [e k]       (entity-lookup e k))
  (valAt [e k not-found] (entity-lookup e k not-found))

  clojure.lang.IFn
  (invoke [e k]      (entity-lookup e k))
  (invoke [e k not-found] (entity-lookup e k not-found)))

;; Implement print methods

(defmethod print-method Entity [e ^java.io.Writer w]
  (.write w (str e)))

(defmethod print-dup Entity [e w]
  (print-method e w))


(defn- use-method
  [^clojure.lang.MultiFn multifn dispatch-val func]
  (. multifn addMethod dispatch-val func))

(use-method clojure.pprint/simple-dispatch Entity pr)


(defn- output-val
  "Find value given an entity and an attribute key"
  [e k]
  (df/first-attr-value @(.-facts e) (.-id e) k))

(defn- output-ref
  "Ensure we convert ref values to entities referencing the same factstore"
  [e k]
  (when-let [ref (df/first-attr-value @(.-facts e) (.-id e) k)]
    (Entity. (.-facts e) ref nil)))

(defn- nested-ref
  "Is a nested value a reference?"
  [e coll-member]
  (df/entity-exists? @(.-facts e) coll-member))

(defn- coll-member
  "Output an entity if collection member is a ref, otherwise the value"
  [e member]
  (if-let [nested-entity (nested-ref e member)]
    (Entity. (.-facts e) member nil)
    member))

(defn- output-coll
  "output entity collections"
  [e k]
  (let [coll (df/attr-values @(.-facts e) (.-id e) k)]
    (mapv (partial coll-member e) coll)))

(defn- output-reverse-ref
  "lookup reverse refs in the reverse attr partition and convert to entity"
  [e k]
  (when-let [ref (df/reverse-value @(.-facts e) (.-id e) k)]
    (Entity. (.-facts e) ref nil)))

(defn- output-member
  "handle each attribute type for entity"
  [e k t]
  (condp = t
    ::df/val (output-val e k)
    ::df/ref (output-ref e k)
    ::df/coll (output-coll e k)
    ::df/reverse-ref (output-reverse-ref e k)
    nil))

(defn- lookup-type
  "find type for attribute k and entity e"
  [e k]
  (let [attr-type (df/attr-meta @(.-facts e) (.-id e) k)]
    (if (= attr-type df/coll-type)
      df/coll-type
      attr-type)))

(defn entity-lookup
  "lookup entity e's attribute k"
  ([e k nf]
   (if (= k :db/id)
     (.-id e)
     (let [attr-type (lookup-type e k)]
       (or (output-member e k attr-type) nf))))
  ([e k]
   (entity-lookup e k nil)))

(defn entity
  "given a factstore and an id, out instance of IEntity"
  [facts id]
  (Entity. (atom facts) id nil))

(defn resolve-coll
  [attr out facts v]
  (if (id->entity facts v)
    (update-in out [attr] conj (->map (entity facts v)))
    (update-in out [attr] conj v)))

(defn resolve-ref
  [attr out facts v]
  (assoc out attr (->map (entity facts v))))

(defn pull
  "Given a fact store, a pattern, and an entity ID,
   output the entity in the given pattern"
  [facts pattern eid]
  (q '[:find (pull ?e pattern) .
       :in $ pattern ?e
       :where [?e _ _]]
     facts pattern eid))
