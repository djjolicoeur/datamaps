(ns datamaps.facts-test
  (:require [clojure.test :refer :all]
            [datamaps.facts :as df]))

(def sample-map
  {:person/name "Ada"
   :person/pets ["Islay" "Islay" "Zorro"]
   :person/location {:city/name "Annapolis"
                     :city/state "MD"}})

(def sample-store
  (-> sample-map
      df/map->facts
      df/raw-facts->fact-store))

(defn find-entity [facts attr value]
  (->> (df/fact-partition facts)
       (some (fn [[e a v]]
               (when (and (= a attr) (= v value))
                 e)))))

(deftest factstore-partitions
  (is (df/factstore? sample-store))
  (is (seq (df/fact-partition sample-store)))
  (is (seq (df/attr-partition sample-store)))
  (is (seq (df/reverse-partition sample-store))))

(deftest attribute-metadata
  (let [root (find-entity sample-store :person/name "Ada")]
    (is (= df/val-type (df/attr-meta sample-store root :person/name)))
    (is (= df/coll-type (df/attr-meta sample-store root :person/pets)))
    (is (= df/ref-type (df/attr-meta sample-store root :person/location)))))

(deftest reverse-reference-detected
  (let [root (find-entity sample-store :person/name "Ada")
        location-id (df/first-attr-value sample-store root :person/location)
        reverse-attr (df/reverse-index-kw :person/location)]
    (is (= df/reverse-ref-type
           (df/attr-meta sample-store location-id reverse-attr)))
    (is (= root (df/reverse-value sample-store location-id reverse-attr)))))

(deftest entity-lookup-helpers
  (let [root (find-entity sample-store :person/name "Ada")
        location-id (df/first-attr-value sample-store root :person/location)
        cat-values (df/attr-values sample-store root :person/pets)]
    (is (true? (df/entity-exists? sample-store root)))
    (is (not (df/entity-exists? sample-store 999)))
    (is (= #{:person/name :person/pets :person/location}
           (df/entity-attrs sample-store root)))
    (is (= 3 (count cat-values)))
    (is (= 2 (count (filter #(= "Islay" %) cat-values))))
    (is (= #{[:person/name "Ada"]
             [:person/location location-id]}
           (->> (df/entity-datoms sample-store root)
                (filter (fn [[_ attr _]]
                          (not= attr :person/pets)))
                (map (fn [[_ a v]] [a v]))
                set)))))
