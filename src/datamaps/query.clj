(ns datamaps.query
  (:require [datascript.query :as dq]
            [datascript.parser :as dp]
            [datascript.lru :as lru]))

(def ^:private lru-cache-size 100)

(defn collect
  "Override datascript's collect fn to not
   restrict results to set"
  [context symbols]
  (->> (dq/-collect context symbols)
       (map vec)))

(def ^:private query-cache (volatile! (lru/lru lru-cache-size)))

(defn memoize-parse-query
  "Cache parsed queries"
  [q]
  (if-let [cached (get @query-cache q nil)]
    cached
    (let [qp (dp/parse-query q)]
      (vswap! query-cache assoc q qp)
      qp)))


(defn q
  "Override datascript's query fn to avoid casting results
   to a set. This allows the ability to run aggregations on
   collections contained within our maps as well as get our
   maps back from the collection of facts as they were passed in,
   cardinality not withstanding."
  [q & inputs]
  (let [parsed-q      (memoize-parse-query q)
        find          (:find parsed-q)
        find-elements (dp/find-elements find)
        find-vars     (dp/find-vars find)
        result-arity  (count find-elements)
        with          (:with parsed-q)
        all-vars      (concat find-vars (map :symbol with))
        q             (cond-> q
                        (sequential? q) dp/query->map)
        wheres        (:where q)
        context       (-> (datascript.query.Context. [] {} {})
                          (dq/resolve-ins (:in parsed-q) inputs))
        results       (-> context
                          (dq/-q wheres)
                          (collect all-vars))]
    (cond->> results
      (:with q)
      (mapv #(vec (subvec % 0 result-arity)))
      (some dp/aggregate? find-elements)
      (dq/aggregate find-elements context)
      ;;(some dp/pull? find-elements)  ;;TODO: integrate pull
      ;;(dq/pull find-elements context)
      true (dq/-post-process find))))
