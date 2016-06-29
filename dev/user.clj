(ns user
  (:require [datamaps.core :as d]
            [datamaps.facts :as df]
            [datomic.api :as datomic]))


;; Basic Examples

(def demographic-fields
  [:demo/firstname :demo/lastname :demo/city :demo/state :demo/zip])

(def base-q
  '[:find ?f ?l ?c ?s ?z
    :where
    [?e :firstname ?f]
    [?e :lastname ?l]
    [?e :locations ?loc]
    [?loc :city ?c]
    [?loc :state ?s]
    [?loc :zip ?z]])

(def demographics-data
  [{:firstname "Dan"
    :lastname "Joli"
    :cats ["islay" "zorro" "lily"]
    :dogs ["ginny"]
    :locations [{:city "Annapolis"
                 :state "MD"
                 :zip 21409
                 :primary true}
                {:city "Baltimore"
                 :state "MD"
                 :zip 21224}]}
   {:firstname "Casey"
    :lastname "Joli"
    :cats ["islay" "zorro" "lily"]
    :dogs ["ginny"]
    :locations [{:city "Salisbury"
                 :state "MD"
                 :zip 21256
                 :primary nil}
                {:city "Annapolis"
                 :state "MD"
                 :zip 21409
                 :primary true}]}])

(defn show-facts []
  (clojure.pprint/pprint (d/facts demographics-data)))

(defn basic-demographics []
  (let [demographics-q (conj base-q '[?loc :primary true])]
    (->> demographics-data
         d/facts
         (d/q demographics-q)
         (map (partial zipmap demographic-fields))
         clojure.pprint/pprint)))

(defn filtered-demographics []
  (let  [canton-query (conj base-q '[(= ?z 21224)])]
    (->> demographics-data
         d/facts
         (d/q canton-query)
         (map (partial zipmap demographic-fields))
         clojure.pprint/pprint)))


;; More nested Demographics and d/entity usage

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

(def people-by-zip
  '[:find [?e ...]
    :in $ ?z
    :where
    [?e :location ?l]
    [?l :neighborhood ?n]
    [?n :zip ?z]])

(defn west-annapolis-ids []
  (as-> test-users thunk
    (d/facts thunk)
    (d/q people-by-zip thunk 21401)))

(defn west-annapolis-entities []
  (let [facts (d/facts test-users)
        ids (d/q people-by-zip facts 21401)]
    (->> ids
        (map (partial d/entity facts))
        (clojure.pprint/pprint))))


;; Aggregations

(def bank-accounts
  [{:user "Dan"
    :account [{:type :checking
               :balance 1000.0}
              {:type :savings
               :balance 10000.0}
              {:type :ira
               :balance 15000.0}]}
   {:user "Casey"
    :account [{:type :checking
               :balance 2000.0}
              {:type :savings
               :balance 14000.0}
              {:type :ira
               :balance 16000.0}]}])


(def checking-q
  '[:find (sum ?b) .
    :where
    [?a :type :checking]
    [?a :balance ?b]])


(defn total-checking []
  (->> bank-accounts
       d/facts
       (d/q checking-q)))

;; Pull API -- Inline w/ query or given an ID

(defn inline-pull []
  (->> (d/facts test-users)
       (d/q '[:find (pull ?e [* {:location [:city :state]}]) .
              :where [?e :firstname "Dan"]])
       (clojure.pprint/pprint)))


(defn  standalone-pull []
  (let [facts (d/facts test-users)
        me (d/q '[:find ?e . :where [?e :firstname "Dan"]] facts)]
    (-> (d/pull facts '[* {:location [:city :state]}] me)
        (clojure.pprint/pprint))))

;; Integrating with Datomic

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


(defn create-datomic-db []
  (let [unique (str (java.util.UUID/randomUUID))
        uri (str "datomic:mem://" unique)
        _ (datomic/create-database uri)
        conn (datomic/connect uri)]
    @(datomic/transact conn schema)
    {:uri uri :conn conn :val (:db-after @(datomic/transact conn datomic-facts))}))

(defn with-db [f]
  (let [db (create-datomic-db)]
    (try (f (:val db))
         (finally (datomic/delete-database (:uri db))))))

(def facts-datomic-join-q
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
    [(= ?datomic-lastname ?map-lastname)]])

(defn facts-datomic-join [db]
  (datomic/q facts-datomic-join-q db (df/fact-partition (d/facts test-users))))

(defn datomic-example []
  (clojure.pprint/pprint (with-db facts-datomic-join)))
