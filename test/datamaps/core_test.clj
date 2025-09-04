(ns datamaps.core-test
  (:require [clojure.test :refer :all]
            [datamaps.core :refer :all]
            [datamaps.facts :as df]
            [datamaps.pull :as dpull]
            [datomic.api :as datomic]))


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
  (testing "Test map elements get mapped to entities"
    (let [facts (facts test-users)
          dan (get-dan facts)
          dan-entity (entity facts dan)]
      (is (= "Dan" (:firstname dan-entity)))
      (is (= "Joli" (:lastname dan-entity)))
      (is (= #{"lily" "zorro" "islay"} (set (:cats dan-entity))))
      (is (= #{"ginny"} (set (:dogs dan-entity))))
      (is (= "Annapolis" (get-in dan-entity [:location :city])))
      (is (= 21224 (get-in dan-entity [:location :neighborhood :zip]))))))


(deftest queryable []
  (testing "Test generated facts are queryable"
    (let [facts (facts test-users)]
      (is (= #{"Mike" "Katie"} (set (west-annapolitans facts)))))))

(deftest find-tuple []
  (testing "Query returning a tuple yields a vector"
    (let [facts (facts test-users)
          tuple (q '[:find [?f ?c]
                     :where
                     [?e :firstname ?f]
                     [?e :location ?l]
                     [?l :city ?c]
                     [?e :firstname "Dan"]]
                   facts)]
      (is (= ["Dan" "Annapolis"] tuple)))))


(def key-collisions
  {:foo {:bar [{:foo [1 2 3]}] :baz {:bar 5}}})

(deftest test-collisions []
  (testing "Keys are defined on a per-map basis"
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
      (is (= 5 (get-in root-ent [:foo :baz :bar]))))))



(def dan-test-credits [100.0 100.0 200.0 200.0 1000.0 1000.0 157.66])

(def dan-test-debits [-500.0 -675.55])

(def casey-test-credits [1756.66 987.55 990.45 345.65])

(def casey-test-debits [-357.44 -432.11 -10.54])

(def bank-maps
  [{:user "Dan"
    :account [{:type :checking
               :credits dan-test-credits
               :debits dan-test-debits}]}
   {:user "Casey"
    :account [{:type :checking
               :credits casey-test-credits
               :debits casey-test-debits}]}])

(defn bank-credit-q [bank-facts user]
  (q '[:find (sum ?c) .
       :in $ ?u
       :where
       [?e :user ?u]
       [?e :account ?a]
       [?a :type :checking]
       [?a :credits ?c]]
     bank-facts user))

(defn bank-debit-q [bank-facts user]
  (q '[:find (sum ?d) .
       :in $ ?u
       :where
       [?e :user ?u]
       [?e :account ?a]
       [?a :type :checking]
       [?a :debits ?d]]
     bank-facts user))


(deftest collections-not-deduped []
  (testing "Collections maintain all original elements"
    (let [bank-facts (facts bank-maps)
          dc (bank-credit-q bank-facts "Dan")
          dd (bank-debit-q bank-facts "Dan")
          cc (bank-credit-q bank-facts "Casey")
          cd (bank-debit-q bank-facts "Casey")]
      (is (= (apply + dan-test-credits) dc))
      (is (= (apply + dan-test-debits) dd))
      (is (= (apply + casey-test-credits) cc))
      (is (= (apply + casey-test-debits) cd)))))


(deftest querying-across-fact-sets []
  (testing "Test the ability to query across generated fact sets"
    (let [user-facts (facts test-users)
          bank-facts (facts bank-maps)
          results (q '[:find ?tf ?bf ?c ?at
                       :in $1 $2
                       :where
                       [$1 ?tu :firstname ?tf]
                       [$2 ?bu :user ?bf]
                       [(= ?bf ?tf)]
                       [$1 ?tu :location ?l]
                       [$1 ?l :city ?c]
                       [$2 ?bu :account ?a]
                       [$2 ?a :type ?at]] user-facts bank-facts)
          result-set (set results)]
      (is (= #{["Dan" "Dan" "Annapolis" :checking]
               ["Casey" "Casey" "Salisbury" :checking]} result-set)))))

(deftest test-pull []
  (testing "Test ability to pull"
    (let [tfacts (facts test-users)
          dan-id (get-dan tfacts)
          pulled (pull tfacts '[* {:location [:city]}] dan-id)]
      (is (= 1 (count (:location pulled))))
      (is (= "Annapolis" (get-in pulled [:location :city]))))))

(deftest parse-selector-basic []
  (testing "Parse selector handles wildcard and subpattern"
    (is (= {:wildcard? true
            :attrs {:location {:attr :location
                                :subpattern {:wildcard? false
                                             :attrs {:city {:attr :city}}}}}}
           (dpull/parse-selector '[* {:location [:city]}])))))


(def schema
  [{:db/id #db/id[:db.part/db]
    :db/ident :demo/firstname
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :demo/lastname
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def datomic-facts
  [{:db/id #db/id[:db.part/user]
    :demo/firstname "Dan"
    :demo/lastname  "Joli"}
   {:db/id #db/id[:db.part/user]
    :demo/firstname "Casey"
    :demo/lastname "Joli"}])

(deftest datomic-integration []
  (testing "Testing ability to integrate with datomic query engine"
    (let [_ (datomic/create-database "datomic:mem://datomic-talk")
          conn (datomic/connect "datomic:mem://datomic-talk")
          _ @(datomic/transact conn schema)
          _ @(datomic/transact conn datomic-facts)
          tfacts (facts test-users)
          results (datomic/q
                   '[:find ?datomic-firstname ?datomic-lastname ?map-firstname
                     ?map-lastname ?map-city ?map-state
                     :in $1 $2
                     :where
                     [$1 ?e :demo/firstname ?datomic-firstname]
                     [$1 ?e :demo/lastname ?datomic-lastname]
                     [$2 ?fe :firstname ?map-firstname]
                     [$2 ?fe :lastname ?map-lastname]
                     [$2 ?fe :location ?l]
                     [$2 ?l :city ?map-city]
                     [$2 ?l :state ?map-state]
                     [(= ?datomic-firstname ?map-firstname)]
                     [(= ?datomic-lastname ?map-lastname)]]
                   (datomic/db conn) (df/fact-partition tfacts))
          _ (datomic/delete-database "datomic:mem://datomic-talk")]
      (is (= results
             #{["Casey" "Joli" "Casey" "Joli" "Salisbury" "MD"]
               ["Dan" "Joli" "Dan" "Joli" "Annapolis" "MD"]})))))
