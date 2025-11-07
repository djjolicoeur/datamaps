(ns datamaps.pull-test
  (:require [clojure.test :refer :all]
            [datamaps.core :as dm]
            [datamaps.pull :as pull]))

(def pull-data
  [{:person/name "Ada"
    :person/pets [{:pet/type :cat :pet/name "Islay"}
                  {:pet/type :dog :pet/name "Ginny"}]
    :person/favorite {:city/name "Annapolis"
                      :city/state "MD"}}])

(defn facts []
  (dm/facts pull-data))

(defn entity-id [facts attr value]
  (dm/q '[:find ?e .
          :in $ ?attr ?value
          :where [?e ?attr ?value]]
        facts attr value))

(deftest wildcard-pull-includes-id
  (let [store (facts)
        ada (entity-id store :person/name "Ada")
        result (pull/pull store '[* {:person/favorite [:city/name]}] ada)]
    (is (= "Ada" (:person/name result)))
    (is (= ada (:db/id result)))
    (is (= "Annapolis" (get-in result [:person/favorite :city/name])))
    (is (= 2 (count (:person/pets result))))))

(deftest selective-pull-of-nested-refs
  (let [store (facts)
        ada (entity-id store :person/name "Ada")
        result (pull/pull store '[{:person/pets [:pet/type]}] ada)]
    (doseq [pet (:person/pets result)]
      (is (= #{:pet/type}
             (if (map? pet) (set (keys pet)) :not-a-map))))))

(deftest reverse-reference-pull
  (let [store (facts)
        favorite (entity-id store :city/name "Annapolis")
        result (pull/pull store '[{:person/_favorite [:person/name]}] favorite)]
    (is (= "Ada" (get-in result [:person/_favorite :person/name])))))

(deftest pull-many-support
  (let [store (facts)
        pets (pull/pull-many store '[{:person/pets [:pet/name]}]
                              (dm/q '[:find [?e ...] :where [?e :person/name _]] store))]
    (is (= 1 (count pets)))
    (is (= #{"Islay" "Ginny"}
           (->> pets first :person/pets (map :pet/name) set)))))
