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
  (let [facts (facts test-users)
        dan (get-dan facts)
        dan-entity (entity facts dan)]
    (is (= "Dan" (:firstname dan-entity)))
    (is (= "Joli" (:lastname dan-entity)))
    (is (= #{"lily" "zorro" "islay"} (set (:cats dan-entity))))
    (is (= #{"ginny"} (set (:dogs dan-entity))))
    (is (= "Annapolis" (get-in dan-entity [:location :city])))
    (is (= 21224 (get-in dan-entity [:location :neighborhood :zip])))))


(deftest queryable []
  (let [facts (facts test-users)]
    (is (= #{"Mike" "Katie"} (set (west-annapolitans facts))))))


(def key-collisions
  {:foo {:bar [{:foo [1 2 3]}] :baz {:bar 5}}})

(deftest test-collisions []
  (let [facts (facts key-collisions)
        sub-id (q '[:find ?e .
                    :where
                    [?e :baz ?b]
                    [?b :bar 5]]
                  facts)
        sub-ent (entity facts sub-id)
        root-id (q '[:find ?e .
                     :where
                     [?e :foo ?f]
                     [?f :bar ?b]
                     [?b :foo 1]]
                   facts)
        root-ent (entity facts root-id)]
    (is (= 5 (get-in sub-ent [:baz :bar])))
    (is (= 5 (get-in root-ent [:foo :baz :bar])))))
