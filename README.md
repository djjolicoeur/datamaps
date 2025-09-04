# datamaps

A Clojure library designed to leverage datalog queries to
query arbitrary maps.

This version targets Clojure 1.11 and Datascript 1.6.

## Build Status and Dependency Information

[![Circle CI](https://circleci.com/gh/djjolicoeur/datamaps.svg?style=shield)](https://circleci.com/gh/djjolicoeur/datamaps)


[![Clojars Project](https://img.shields.io/clojars/v/datamaps.svg)](https://clojars.org/datamaps)

## Project Status

 * Brand spanking new.  I hacked this together while I was stuck on a commuter bus
   sans-internet so it's rough at present -- Pet Project status.
 * Performance is atrocious, but can be improved. Space and time complexities have not
   even been considered at this point.
 * ~~Does not support the Pull api currently~~  added in 0.1.1-SNAPSHOT
 * ~~Need to figure out if I can add the ability to choose query engine provider
   so people can use datascripts engine, should they choose.  I use datomic in my
   projects, so I went with that first.~~ as of 0.1.1-SNAPSHOT,
   the datascript query engine is being used
 * Needs more robust testing and validation as it is almost certainly full of bugs and
  warts.

### NEW!

 * Custom Pull API adapted from datscripts pull-api
 * Collections maintain all original elements rather than being
   truncated by set semantics.  In a true DB, this is desired, but we are
   abstracting maps, and should maintain consistency with the original map.
 * Datascript is included as a dependency.
 * Entity interface analogous to datomic and datascripts enabling the ability to
   "walk" the map via cursor.
 * Reverse reference implementation for entities.



## Motivation

I spend a lot of time writing code to pull apart maps and structure them in a
way that suits our domain needs.  Spector does a great job of reducing the complexity
that is inherent in dealing with this situation, but I couldn't help thinking there
was a way to apply datalog to this problem to simplify the process, especially since most
of the applications I write already inlcude the datomic api and leveraging that would elide the
need to keep track of another DSL.

Consider the following example:

```clojure

(defn location [m]
  (->> (:locations m)
       (filter :primary)
       first))


(defn user->demographics [m]
  (let [loc (location m)]
   {:demo/firstname (:firstname m)
    :demo/lastname (:lastname m)
    :demo/city (:city loc)
    :demo/state (:state loc)
    :demo/zip (:zip loc)}))

(map user->demographics test-users)
```

Which would parse something like the data below.

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

Arguably this example isn't too bad, but this strategy quickly gets out of hand
the deeper things get nested. Especially if you have to filter a nested
collection for a specific member. The number of auxillary functions we need to write
grows with the amount of nested structures.


Collections nested in maps can lead to some ugly code, fast. If I can just query
for the values I want without having to make several map access calls,
and avoid filtering component collections that would be ideal!
datamaps lets you do something like this to tackle the problem.

```clojure
(require '[datamaps.core :as dm])

(def demo-fields
     [:demo/firstname :demo/lastname :demo/city :demo/state :demo/zip])

(->> test-users
     dm/facts
     (dm/q '[:find ?f ?l ?c ?s ?z
             :where
             [?e :firstname ?f]
             [?e :lastname ?l]
             [?e :locations ?loc]
             [?loc :city ?c]
             [?loc :state ?s]
             [?loc :primary true]
             [?loc :zip ?z]])
      (map (partial zipmap demo-fields)))

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

Without having to result to filter fns and other strategies.

Even better, we can filter on params in-line with the query.  Lets say I only
want to bring back people who currently live in the 21401 zip code.


```clojure
(->> test-users
     dm/facts
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
      (map (partial zipmap demo-fields)))

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

As opposed to filtering a list, then transforming, or loop recurring over the list to
build the results set.

## How does it work?

* Each map or nested map is assigned a unique identifier.
* each `[k v]` tuple in a map is added to the fact
  table as `[<ID> k v]`. this is applied recusivly
  to all child structures.
* Each `k` in each `[k v]` tuple is stored as `[<ID> k <TYPE>]` where
  type can be:
    * `:datamaps.core/val` for scalars
    * `:datamaps.core/ref` for nested maps
    * `:datamaps.core/coll` for nested collections
* Since attributes aren't installed, storing basic metadata like
  this allows for rebuilding maps from their IDs.  Very useful if
  the data you want is actually a nested map.
* the `datamaps.core/q` function maintains the invariant of `datomic.api/q`,
  but filters all of our metadata facts out before applying the
  query.
* You can also query the metadata via `meta-q` which filters
  all of the non-metadata facts from the table.


## Isn't this like [Datascript](https://github.com/tonsky/datascript)?

Kind of, but not really. Datascript allows you to create a true database in memory, whereas
the goal of datamaps is to allow for the ad-hoc generation of fact lists from arbitrary maps to
allow for easy querying of structured data. If you are looking for a way to store data for
use later in program execution, Datascript is the way to go.  If you are only looking only to
manipulate and query data while in scope, this will do the trick.
The facts generated by datamaps are not intended to be added to or retracted from so
datamaps is very much _not_ a database solution.

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

(def facts (dm/facts test-users))

;; now we have a queryable set of facts!!

(dm/q '[:find [?f ...]
        :where
        [?e :firstname ?f]
        [?e :location ?l]
        [?l :neighborhood ?n]
        [?n :zip 21401]] facts) ;;=> ["Mike" "Katie"]

```

## Entity Interface

Entities are analogous to datomic entities and implement reverse
references, and are instantiated in the same manner. Additionally,
datamaps entites implement `->map`, which reconstitutes the map
to it's original form from the facts

```clojure
(def dan-id (dm/q '[:find ?e .
                    :where [?e :firstname "Dan"]] facts))

;;=> #uuid "16a89d94-5451-4329-ac67-78d1ba39476f"

(->> dan-id
     (dm/entity facts)
     dm/->map)

;;=> {:cats ("islay" "lily" "zorro"),
;;=>  :dogs ("ginny"),
;;->  :firstname "Dan",
;;=>  :location
;;=>  {:neighborhood {:name "Baltimore", :zip 21224},
;;=>   :state "MD",
;;=>   :city "Annapolis"},
;;=>  :lastname "Joli"}
```

This is really handy if you want to filter and pull a sub-map from that data, for instance
if we want all neighborhoods with a 21224 zip code

```clojure
(def canton-id (dm/q '[:find ?n . :where [?n :zip 21224]] facts))

(def dan-neighborhood (dm/entity facts canton-id))

(dm/touch dan-neighborhood)

;;=> {:name "Baltimore", :zip 21224, :db/id #uuid "e452a705-0d1f-43d2-a5b1-7a1a39be1f7a"}


;; walk the reverse refs back to the top level

(def dan (:_location (:_neighborhood dan-neighborhood)))

(d/touch dan)

;;=> {:lastname "Joli",
;;=>  :cats ["islay" "zorro" "lily"],
;;=>  :dogs ["ginny"],
;;=>  :firstname "Dan",
;;=>  :location {:db/id #uuid "375fcb5e-5f4c-4c43-a570-c006a06475df"},
;;=>  :db/id #uuid "64e4ab3f-1040-4c3f-bb8c-80f16849b48a"}

```

We can also use Datomic's aggregation functions to easily run aggregations over
maps as opposed to building up sub collections to operate on.
for example some bank account data

```clojure
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

;; build a fact table

(def account-facts (dm/facts bank-accounts))

;; Lets see how much the bank has in all checking accounts

(dm/q '[:find (sum ?b) .
        :where
        [?a :type :checking]
        [?a :balance ?b]]
      account-facts) ;;=> 3000.0

```

## Using the Pull API

you can use the pull API in-line in queries or via the `datamaps.core/pull` function.

Examples:

```clojure
(require '[datamaps.core :as d])

(def facts (d/facts test-users))

(d/q '[:find (pull ?e [* {:location [:city :state]}]) .
       :where [?e :firstname "Dan"]] facts)

;;=> {:cats ["zorro" "lily" "islay"],
;;=>  :firstname "Dan",
;;=>  :lastname "Joli",
;;=>  :dogs ["ginny"],
;;=>  :location {:city "Annapolis", :state "MD"}}

(def me (d/q '[:find ?e . :where [?e :firstname "Dan"]] facts))
(d/pull facts '[* {:location [:city :state]}] me)

;;=> {:cats ["zorro" "lily" "islay"],
;;=>  :firstname "Dan",
;;=>  :lastname "Joli",
;;=>  :dogs ["ginny"],
;;=>  :location {:city "Annapolis", :state "MD"}}
```

## Querying Across Multiple Sets of Facts

We can query across multiple sets of facts via `datamaps.core/q`, however
`datamaps.core/q` is only designed to query against facts that satisfy the `IFactStore`
protocol.  If you wish to query against a database and a set of facts you should use
the databases query engine, i.e. datascript or datomic (more on this later).

Example:

```clojure
(require '[datamaps.core :as d])

;;from the above map definitions
(def user-facts (d/facts test-users))

(def bank-facts (d/facts bank-maps))

(d/q '[:find ?tf ?bf ?c ?at
       :in $1 $2
       :where
       [$1 ?tu :firstname ?tf]
       [$2 ?bu :user ?bf]
       [(= ?bf ?tf)]
       [$1 ?tu :location ?l]
       [$1 ?l :city ?c]
       [$2 ?bu :account ?a]
       [$2 ?a :type ?at]] user-facts bank-facts)

;;=> (["Dan" "Dan" "Annapolis" :checking] ["Casey" "Casey" "Salisbury" :checking])

```

## Combining Queries with Datomic/Datascript

When querying against a set of facts generated from maps and a database, using the
databases query engine against the fact partition of the `IFactStore` is the best course
of action. It should be noted that in-lining `pull` on an entity from the facts datasource
will not work in this scenario.

For example:

```clojure
(require '[datamaps.core :as d])
(require '[datamaps.facts :as df])
(require '[datomic.api :as datomic])

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

(datomic/create-database "datomic:mem://test-db")

(def conn (datomic/connect "datomic:mem://test-db"))

@(datomic/transact conn schema)

@(datomic/transact conn datomic-facts)

(def facts (d/facts test-users)) ;;from above examples

(datomic/q ;;find matches in our db who have the same first and last name as our facts
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
    (datomic/db conn) (df/fact-partition facts)) ;;must use the fact partition!!

;;=> #{["Casey" "Joli" "Casey" "Joli" "Salisbury" "MD"]
;;=>   ["Dan" "Joli" "Dan" "Joli" "Annapolis" "MD"]}
```
These are all trivial examples, but I think they illustrate the larger potential, here.

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
