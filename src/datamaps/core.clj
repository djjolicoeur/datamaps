(ns datamaps.core
  (:require [datamaps.query :as d]
            [datamaps.facts :as df]
            [datascript.query :as dq]
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
  [m]
  (df/raw-facts->fact-store (facts* m)))


(defn id->entity
  [facts id]
  (q '[:find ?e .
       :in $ ?e
       :where
       [?e _ _]]
     facts id))

(defn entity->datums
  "Find the datums related to this ID"
  [facts id]
  (q '[:find ?a ?v
       :in $ ?id
       :where
       [?id ?a ?v]]
     facts id))

(declare entity entity-lookup)

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

(defn- entity-keys
  [e]
  (->> (d/q '[:find [?a ...]
              :in $ ?e
              :where [?e ?a _]]
            @(.-facts e) (.-id e))
       set))


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
  (touch [e]))

(deftype Entity [facts id touched]
  IEntity
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

(defmethod print-method Entity [e ^java.io.Writer w]
  (.write w (str e)))

(defmethod print-dup Entity [e w]
  (print-method e w))


(defn- use-method
  [^clojure.lang.MultiFn multifn dispatch-val func]
  (. multifn addMethod dispatch-val func))

(use-method clojure.pprint/simple-dispatch Entity pr)

(defn- output-val
  [e k]
  (d/q '[:find ?v .
         :in $ ?e ?a
         :where [?e ?a ?v]]
       @(.-facts e) (.-id e) k))

(defn- output-ref
  [e k]
  (when-let [ref (d/q '[:find ?v .
                        :in $ ?e ?a
                        :where [?e ?a ?v]]
                      @(.-facts e) (.-id e) k)]
    (Entity. (.-facts e) ref nil)))

(defn- nested-ref
  [e coll-member]
  (d/q '[:find ?e .
         :in $ ?e
         :where [?e _ _]]
       @(.-facts e) coll-member))

(defn- coll-member
  [e member]
  (if-let [nested-entity (nested-ref e member)]
    (Entity. (.-facts e) member nil)
    member))

(defn- output-coll
  [e k]
  (let [coll (d/q '[:find [?v ...]
                    :in $ ?e ?k
                    :where [?e ?k ?v]]
                  @(.-facts e) (.-id e) k)]
    (mapv (partial coll-member e) coll)))

(defn- output-reverse-ref
  [e k]
  (when-let [ref (dq/q '[:find ?v .
                         :in $ ?e ?k
                         :where [?e ?k ?v]]
                       (df/reverse-partition @(.-facts e)) (.-id e) k)]
    (Entity. (.-facts e) ref nil)))

(defn- output-member
  [e k t]
  (condp = t
    ::df/val (output-val e k)
    ::df/ref (output-ref e k)
    ::df/coll (output-coll e k)
    ::df/reverse-ref (output-reverse-ref e k)
    nil))

(defn- lookup-type
  [e k]
  (let [t (dq/q '[:find [?v ...]
                  :in $ ?e ?a
                  :where [?e ?a ?v]]
                (df/attr-partition @(.-facts e)) (.-id e) k)]
    (if (some #{::df/coll} t) ::df/coll (first t))))

(defn entity-lookup
  ([e k nf]
   (if (= k :db/id)
     (.-id e)
     (let [attr-type (lookup-type e k)]
       (or (output-member e k attr-type) nf))))
  ([e k]
   (entity-lookup e k nil)))

(defn entity [facts id]
  (Entity. (atom facts) id nil))

(defn pull
  "Given a fact store, a pattern, and an entity ID,
   output the entity in the given pattern"
  [facts pattern eid]
  (q '[:find (pull ?e pattern) .
       :in $ pattern ?e
       :where [?e _ _]]
     facts pattern eid))
