# datamaps

A Clojure library designed to leverage datomics query engine to
query structured maps via datalog.



## Example Usage

```clojure
(require '[datamaps.core :as dm])

;; load up some arbitrary maps

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

;; convert them into facts

(def facts (dm/maps->facts test-users))

;; now we have a queryable set of facts!!

(dm/q '[:find [?f ...]
        :where
        [?e :firstname ?f]
        [?e :location ?l]
        [?l :neighborhood ?n]
        [?n :zip 21401]] facts) ;;=> ["Mike" "Katie"]


;; I store some metadata about keywords so we can reconstitute
;; entities as well

(def dan-id (dm/q '[:find ?e .
                    :where [?e :firstname "Dan"]] facts))

;;=> #uuid "16a89d94-5451-4329-ac67-78d1ba39476f"

(dm/entity facts dan-id)

;;=> {:cats ("islay" "lily" "zorro"),
;;=>  :dogs ("ginny"),
;;=>  :location #uuid "380b177f-115d-412d-a530-db8b484e57fd",
;;=>  :firstname "Dan",
;;=>  :lastname "Joli"}

```

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
