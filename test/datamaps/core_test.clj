(ns datamaps.core-test
  (:require [clojure.test :refer :all]
            [datamaps.core :refer :all]))


(def test-users
  [{:firstname "Dan"
    :lastname "Joli"
    :cats ["islay" "zorro" "lily"]
    :dogs ["ginny"]
    :location {:city "Annapolis"
               :state "MD"
               :neighborhood {:name "Baltimore"
                              :zip 21224}}}
   {:firstname "Casey"
    :lastname "Joli"
    :cats ["islay" "zorro" "lily"]
    :dogs ["ginny"]
    :location {:city "Salisbury"
               :state "MD"
               :neighborhood {:name "Cape"
                              :zip 21409}}}
   {:firstname "Mike"
    :lastname "Joli"
    :dogs ["penny" "stokely"]
    :location {:city "Annapolis"
               :state "MD"
               :neighborhood {:name "West Annapolis"
                              :zip 21401}}}
   {:firstname "Katie"
    :lastname "Joli"
    :dogs ["penny" "stokely"]
    :location {:city "Annapolis"
               :state "MD"
               :neighborhood {:name "West Annapolis"
                              :zip 21401}}}])



(defn west-annapolitans [facts]
  (q '[:find [?f ...]
       :where
       [?e :firstname ?f]
       [?e :location ?l]
       [?l :neighborhood ?n]
       [?n :zip 21401]]
     facts))

(defn get-dan [facts]
  (q '[:find ?e .
       :where [?e :firstname "Dan"]]
     facts))

(defn city [facts location-ref]
  (q '[:find ?v .
       :in $ ?loc
       :where
       [?loc :city ?v]]
     facts location-ref))

(deftest entity-mapping []
  (let [facts (maps->facts test-users)
        dan (get-dan facts)
        dan-entity (entity facts dan)]
    (is (= "Dan" (:firstname dan-entity)))
    (is (= "Joli" (:lastname dan-entity)))
    (is (= #{"lily" "zorro" "islay"} (set (:cats dan-entity))))
    (is (= #{"ginny"} (set (:dogs dan-entity))))
    (is (= "Annapolis" (city facts (:location dan-entity))))))


(deftest queryable []
  (let [facts (maps->facts test-users)]
    (is (= #{"Mike" "Katie"} (set (west-annapolitans facts))))))
