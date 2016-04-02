(ns datamaps.query
  (:require [datascript.query :as dq]
            [datascript.parser :as dp]
            [datascript.pull-parser :as dpp]
            [datascript.lru :as lru]
            [datamaps.facts :as df]
            [datamaps.pull :as dpa])
  (:import [datascript.parser BindColl BindIgnore BindScalar BindTuple
            Constant FindColl FindRel FindScalar FindTuple PlainSymbol
                                RulesVar SrcVar Variable]))

(def ^:private lru-cache-size 100)

(defn rel-with-attr [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn context-resolve-val [context sym]
  (when-let [rel (rel-with-attr context sym)]
    (when-let [tuple (first (:tuples rel))]
      (get tuple ((:attrs rel) sym)))))


(defprotocol IContextResolve
  (-context-resolve [var context]))

(extend-protocol IContextResolve
  Variable
  (-context-resolve [var context]
    (context-resolve-val context (.-symbol var)))
  SrcVar
  (-context-resolve [var context]
    (get-in context [:sources (.-symbol var)]))
  PlainSymbol
  (-context-resolve [var _]
    (get dq/built-in-aggregates (.-symbol var)))
  Constant
  (-context-resolve [var _]
        (.-value var)))

(defn resolve-pull-source [var context]
  (get-in context [:pull-sources (.-symbol var)]))

(defn pull [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (dp/pull? find)
                     [(resolve-pull-source (:source find) context)
                      (dpp/parse-pull
                       (-context-resolve (:pattern find) context))]))]
    (for [tuple resultset]
      (mapv (fn [env el]
              (if env
                (let [[src spec] env]
                  (dpa/pull-spec src spec [el] false))
                el))
            resolved
            tuple))))



(defn resolve-in [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (:variable binding)))
    (-> context
        (update-in [:sources]
                   assoc (get-in binding [:variable :symbol]) (df/fact-partition value))
        (update-in [:pull-sources] assoc (get-in binding [:variable :symbol]) value))
    (and (instance? BindScalar binding)
         (instance? RulesVar (:variable binding)))
    (assoc context :rules (dq/parse-rules value))
    :else
    (update-in context [:rels] conj (dq/in->rel binding value))))

(defn resolve-ins [context bindings values]
    (reduce resolve-in context (zipmap bindings values)))

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
                          (resolve-ins (:in parsed-q) inputs))
        results       (-> context
                          (dq/-q wheres)
                          (collect all-vars))]
    (cond->> results
      (:with q)
      (mapv #(vec (subvec % 0 result-arity)))
      (some dp/aggregate? find-elements)
      (dq/aggregate find-elements context)
      (some dp/pull? find-elements)
      (pull find-elements context)
      true (dq/-post-process find))))
