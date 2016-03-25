# datamaps

A Clojure library designed to leverage datomics query engine to
query structured maps via datalog.

## Status

 * Brand spanking new.  I hacked this together while I was stuck on a commuter bus
   sans-internet so it's rough at present -- Pet Project status. 
 * Performance is atrocious, but can be improved
 * Needs more robust testing and validation. 

## Motivation

I spend a lot of time writing code to pull apart maps and structure them in a
way that suits our domain needs.  Spector does a great job of reducing the complexity
that is inherent in dealing with this situation, but I couldn't help thinking there
was a way to apply datalog to this problem to simplify the proccess.

So instead of writing code like this:

```clojure
(defn user->demographics [m]
   {:demo/firstname (:firstname m)
    :demo/lastname (:lastname m)
    :demo/city (get-in m [:location :city])
    :demo/state (get-in m [:location :state])
    :demo/zip (get-in m [:location :neighborhood :zip])})

(map user->demographics test-users)
```

Which arguably isn't too bad in this example, but quickly gets out of hand the deeper things
get nested. Especially if you have to filter a nested collection for a specific member for
instance dealing with something like this

```clojure
(def test-users
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
```

If I can just query for the values I want without having to make several map access calls,
and avoid filtering component collections that would be ideal!
datamaps lets you do something like this.

```clojure
(require '[datamaps.core :as dm])

(->> test-users
     dm/maps->facts
     (dm/q '[:find ?f ?l ?c ?s ?z
             :where
             [?e :firstname ?f]
             [?e :lastname ?l]
             [?e :locations ?loc]
             [?loc :city ?c]
             [?loc :state ?s]
             [?loc :primary true]
             [?loc :zip ?z]])
      (map (partial zipmap [:demo/firstname :demo/lastname
                            :demo/city :demo/state :demo/zip])))

```

and get back

```clojure
({:demo/firstname "Dan",
  :demo/lastname "Joli",
  :demo/city "Annapolis",
  :demo/state "MD",
  :demo/zip 21409}
 {:demo/firstname "Casey",
  :demo/lastname "Joli",
  :demo/city "Annapolis",
  :demo/state "MD",
  :demo/zip 21409})
```

in one shot

Even better, we can filter on params in-line with the query.  Lets say I only
want to bring back people from the 21401 zip code


```clojure
(->> test-users
     dm/maps->facts
     (dm/q '[:find ?f ?l ?c ?s ?z
             :where
             [?e :firstname ?f]
             [?e :lastname ?l]
             [?e :location ?loc]
             [?loc :city ?c]
             [?loc :state ?s]
             [?loc :zip ?z]
             [?loc primary true]
             [(= ?z 21401)]])
      (map (partial zipmap [:demo/firstname :demo/lastname
                            :demo/city :demo/state :demo/zip])))

;;=> ({:demo/firstname "Mike",
;;=>   :demo/lastname "Joli",
;;=>   :demo/city "Annapolis",
;;=>   :demo/state "MD",
;;=>   :demo/zip 21401}
;;=>  {:demo/firstname "Katie",
;;=>   :demo/lastname "Joli",
;;=>   :demo/city "Annapolis",
;;=>   :demo/state "MD",
;;=>  :demo/zip 21401})
```

As opposed to filtering a list, then transforming, or loop recuring over the list to
build the results set.



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
;;->  :firstname "Dan",
;;=>  :location
;;=>  {:neighborhood {:name "Baltimore", :zip 21224},
;;=>   :state "MD",
;;=>   :city "Annapolis"},
;;=>  :lastname "Joli"}
```

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
