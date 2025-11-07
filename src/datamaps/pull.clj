(ns datamaps.pull
  (:require [clojure.set :as set]
            [datamaps.facts :as df]))

(def ^:private default-selector '[*])

(defn- ensure-selector [spec]
  (cond
    (nil? spec) default-selector
    (vector? spec) spec
    (map? spec) [spec]
    :else (vector spec)))

(defn- selector->spec [selector]
  (reduce (fn [acc entry]
            (cond
              (= entry '*)
              (assoc acc :wildcard? true)

              (= entry :db/id)
              (assoc acc :include-id? true)

              (keyword? entry)
              (assoc-in acc [:attrs entry] nil)

              (map? entry)
              (reduce (fn [spec [k v]]
                        (assoc-in spec [:attrs k] v))
                      acc
                      entry)

              :else acc))
          {:wildcard? false
           :include-id? false
           :attrs {}}
          (or selector [])))

(defn- attr-type [facts eid attr]
  (df/attr-meta facts eid attr))

(declare pull-entity*)

(defn- hydrate-ref [facts selector eid visited]
  (when eid
    (pull-entity* facts selector eid visited)))

(defn- pull-ref [facts eid attr sub-selector visited]
  (let [selector (ensure-selector sub-selector)
        ref-id (df/first-attr-value facts eid attr)]
    (hydrate-ref facts selector ref-id visited)))

(defn- pull-reverse [facts eid attr sub-selector visited]
  (let [selector (ensure-selector sub-selector)
        ref-id (df/reverse-value facts eid attr)]
    (hydrate-ref facts selector ref-id visited)))

(defn- pull-coll [facts eid attr sub-selector visited]
  (let [selector (ensure-selector sub-selector)
        values (df/attr-values facts eid attr)]
    (mapv (fn [value]
            (if (df/entity-exists? facts value)
              (hydrate-ref facts selector value visited)
              value))
          values)))

(defn- pull-scalar [facts eid attr]
  (df/first-attr-value facts eid attr))

(defn- pull-attr [facts eid attr sub-selector visited]
  (let [atype (attr-type facts eid attr)]
    (cond
      (= atype df/coll-type)
      (pull-coll facts eid attr sub-selector visited)

      (= atype df/ref-type)
      (pull-ref facts eid attr sub-selector visited)

      (= atype df/reverse-ref-type)
      (pull-reverse facts eid attr sub-selector visited)

      :else
      (pull-scalar facts eid attr))))

(defn- attrs-for [facts eid spec]
  (let [entity-attrs (df/entity-attrs facts eid)
        requested (set (keys (:attrs spec)))]
    (if (:wildcard? spec)
      (set/union entity-attrs requested)
      requested)))

(defn- pull-entity* [facts selector eid visited]
  (when (df/entity-exists? facts eid)
    (if (visited eid)
      {:db/id eid}
      (let [visited (conj visited eid)
            spec (selector->spec selector)
            include-id? (or (:include-id? spec) (:wildcard? spec))
            attrs (attrs-for facts eid spec)]
        (reduce (fn [acc attr]
                  (let [sub-selector (get-in spec [:attrs attr])
                        value (pull-attr facts eid attr sub-selector visited)]
                    (if (some? value)
                      (assoc acc attr value)
                      acc)))
                (cond-> {}
                  include-id? (assoc :db/id eid))
                attrs)))))

(defn pull
  "Return a map matching selector for entity id."
  [facts selector eid]
  {:pre [(df/factstore? facts)]}
  (pull-entity* facts selector eid #{}))

(defn pull-many
  "Return a vector of pull results for the provided entity ids."
  [facts selector eids]
  {:pre [(df/factstore? facts)]}
  (mapv #(pull-entity* facts selector % #{}) eids))
