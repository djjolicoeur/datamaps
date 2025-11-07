(ns datamaps.query-test
  (:require [clojure.test :refer :all]
            [datamaps.core :as dm]
            [datamaps.query :as dq]
            [datamaps.facts :as df]))

(def user-data
  [{:person/name "Ada"
    :person/age 40
    :person/city {:city/name "Annapolis"}
    :person/pets ["Islay" "Islay" "Zorro"]}
   {:person/name "Ben"
    :person/age 35
    :person/city {:city/name "Baltimore"}
    :person/pets ["Luna"]}])

(def alias-data
  [{:alias/name "Annapolis" :alias/nickname "Sailing Capital"}
   {:alias/name "Baltimore" :alias/nickname "Charm City"}])

(defn user-facts []
  (dm/facts user-data))

(def alias-facts
  (dm/facts alias-data))

(deftest relation-query
  (let [facts (user-facts)
        results (dm/q '[:find ?name ?age
                        :where
                        [?e :person/name ?name]
                        [?e :person/age ?age]]
                      facts)]
    (is (= #{["Ada" 40] ["Ben" 35]} (set results)))))

(deftest scalar-find
  (let [facts (user-facts)
        result (dm/q '[:find ?name .
                       :where [?e :person/name ?name]]
                     facts)]
    (is (#{"Ada" "Ben"} result))))

(deftest collection-find-preserves-duplicates
  (let [facts (user-facts)
        cats (dm/q '[:find [?pet ...]
                     :where
                     [?e :person/name "Ada"]
                     [?e :person/pets ?pet]]
                   facts)]
    (is (= 3 (count cats)))
    (is (= 2 (count (filter #(= "Islay" %) cats))))))

(deftest aggregate-support
  (let [facts (user-facts)
        total (dm/q '[:find (sum ?age) .
                      :where
                      [?e :person/age ?age]]
                    facts)]
    (is (= 75 total))))

(deftest predicate-clauses
  (let [facts (user-facts)
        result (dm/q '[:find ?name
                       :where
                       [?e :person/name ?name]
                       [(= ?name "Ada")]]
                     facts)]
    (is (= [["Ada"]] result))))

(deftest multi-source-joins
  (let [people (user-facts)
        aliases alias-facts
        results (dq/q '[:find ?name ?nickname
                        :in $people $aliases
                        :where
                        [$people ?p :person/name ?name]
                        [$people ?p :person/city ?city]
                        [$people ?city :city/name ?city-name]
                        [$aliases ?alias :alias/name ?city-name]
                        [$aliases ?alias :alias/nickname ?nickname]]
                      people aliases)]
    (is (= #{["Ada" "Sailing Capital"]
             ["Ben" "Charm City"]}
           (set results)))))

(deftest inline-pull-from-query
  (let [facts (user-facts)
        result (dm/q '[:find (pull ?e [:person/name {:person/city [:city/name]}]) .
                       :where
                       [?e :person/name "Ada"]]
                     facts)]
    (is (= {:person/name "Ada"
            :person/city {:city/name "Annapolis"}}
           result))))
