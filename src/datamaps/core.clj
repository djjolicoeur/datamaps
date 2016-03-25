(ns datamaps.core
  (:require [datomic.api :as d]))


(declare map->facts)
(declare map-entry->fact)

(defn seq->facts
  [id k seq]
  (->> (map (fn [k v] [k v]) (take (count seq) (repeat k)) seq)
       (map (partial map-entry->fact id))
       (reduce concat)
       (cons [k :coll])))

(defn map-entry->fact
  [id [k v]]
  (cond (map? v) (map->facts id k v)
        (sequential? v) (seq->facts id k v)
        v [[id k v] [k :val]]
        true nil))

(defn map->facts
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
          (cons [key :ref])))))


(defn maps->facts
  [ms]
  (set (mapcat map->facts ms)))


(defn attr-meta
  [facts attr]
  (d/q '[:find ?type .
         :in $ ?attr
         :where
         [?attr ?type]]
       facts attr))

(defn entity->datums
  [facts id]
  (d/q '[:find ?a ?v
         :in $ ?id
         :where
         [?id ?a ?v]]
       facts id))

(defn entity->map [facts id datums]
  (loop [datums (seq datums) out {}]
    (if-let [datum (first datums)]
      (let [attr (first datum)
            type (attr-meta facts attr)]
        (recur (rest datums)
               (if (= type :coll)
                 (update-in out [attr] conj (second datum))
                 (assoc out attr (second datum)))))
      out)))

(defn entity [facts id]
  (->> id
       (entity->datums facts)
       (entity->map facts id)))

(def q d/q)
