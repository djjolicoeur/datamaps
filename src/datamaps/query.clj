(ns datamaps.query
  (:require [clojure.string :as str]
            [datamaps.facts :as df]
            [datamaps.pull :as dpull]))

;; Basic aggregation support
(def ^:private aggregations
  {'sum   (fn [values] (reduce + 0 values))
   'count (fn [values] (count values))
   'max   (fn [values] (when (seq values) (reduce max values)))
   'min   (fn [values] (when (seq values) (reduce min values)))
   'avg   (fn [values]
            (if (seq values)
              (/ (reduce + values) (double (count values)))
              0.0))})

(defn- wildcard? [v]
  (= '_ v))

(defn- source-symbol? [sym]
  (and (symbol? sym)
       (-> sym name (str/starts-with? "$"))))

(defn- query-var? [sym]
  (and (symbol? sym)
       (-> sym name (str/starts-with? "?"))))

(defn- collect-sections [items]
  (loop [forms items
         current nil
         sections {}]
    (if (seq forms)
      (let [form (first forms)]
        (if (keyword? form)
          (recur (rest forms) form sections)
          (recur (rest forms)
                 current
                 (if current
                   (update sections current (fnil conj []) form)
                   sections))))
      sections)))

(defn- coerce-vector [v]
  (cond
    (nil? v) []
    (vector? v) v
    (seq? v) (vec v)
    :else [v]))

(defn- normalize-query [q]
  (cond
    (map? q)
    (-> q
        (update :find coerce-vector)
        (update :where coerce-vector)
        (update :in coerce-vector))

    (vector? q)
    (let [sections (collect-sections q)]
      {:find  (coerce-vector (:find sections))
       :where (coerce-vector (:where sections))
       :in    (coerce-vector (:in sections))
       :with  (coerce-vector (:with sections))
       :rules (:rules sections)})

    :else
    (throw (ex-info "Unsupported query representation" {:query q}))))

(defn- ensure-section [q k]
  (if-let [section (get q k)]
    section
    (throw (ex-info (str "Query missing " k) {:query q}))))

(defn- normalize-source [value]
  (cond
    (df/factstore? value)
    {:datoms (vec (df/fact-partition value))
     :fact-store value}

    (coll? value)
    {:datoms (vec value)}

    :else (throw (ex-info "Unsupported data source" {:value value}))))

(defn- bind-inputs [in-specs inputs]
  (when (not= (count in-specs) (count inputs))
    (throw (ex-info "Input arity mismatch"
                    {:expected (count in-specs)
                     :provided (count inputs)})))
  (loop [specs in-specs
         vals inputs
         context {:sources {}
                  :default-source nil
                  :default-fact-store nil}
         binding {}]
    (if (seq specs)
      (let [spec (first specs)
            value (first vals)]
        (cond
          (= '_ spec)
          (recur (rest specs) (rest vals) context binding)

          (source-symbol? spec)
          (let [source (normalize-source value)
                context (-> context
                            (assoc-in [:sources spec] source)
                            (update :default-source #(or % spec))
                            (update :default-fact-store #(or % (:fact-store source))))]
            (recur (rest specs) (rest vals) context binding))

          (symbol? spec)
          (recur (rest specs) (rest vals) context (assoc binding spec value))

          :else
          (throw (ex-info "Unsupported :in binding form" {:binding spec}))))
      {:context context
       :binding binding})))

(defn- variable-symbol? [term binding]
  (and (symbol? term)
       (or (query-var? term)
           (contains? binding term))))

(defn- unify-term [binding term value]
  (cond
    (nil? binding) nil
    (wildcard? term) binding
    (variable-symbol? term binding)
    (if (contains? binding term)
      (when (= (binding term) value) binding)
      (assoc binding term value))
    :else
    (when (= term value) binding)))

(defn- predicate-clause? [clause]
  (and (vector? clause)
       (= 1 (count clause))
       (seq? (first clause))))

(defn- resolve-fn [sym]
  (let [ns-name (namespace sym)
        name (symbol (name sym))]
    (if ns-name
      (do (require (symbol ns-name))
          (or (ns-resolve (symbol ns-name) name)
              (throw (ex-info "Unable to resolve function" {:symbol sym}))))
      (or (ns-resolve 'clojure.core sym)
          (throw (ex-info "Unable to resolve clojure.core function"
                          {:symbol sym}))))))

(defn- resolve-value [binding term]
  (cond
    (and (symbol? term) (contains? binding term))
    (binding term)

    (and (symbol? term) (query-var? term))
    (throw (ex-info "Unbound logic variable" {:variable term}))

    :else term))

(defn- eval-predicate [bindings clause]
  (let [form (first clause)
        f (resolve-fn (first form))
        args (rest form)]
    (reduce (fn [acc binding]
              (let [values (map #(resolve-value binding %) args)]
                (if (apply f values)
                  (conj acc binding)
                  acc)))
            [] bindings)))

(defn- source-datoms [context sym]
  (or (get-in context [:sources sym :datoms])
      (throw (ex-info "Unknown data source" {:source sym}))))

(defn- eval-pattern [bindings clause context]
  (let [[maybe-source & parts] clause
        [source terms] (if (source-symbol? maybe-source)
                         [maybe-source parts]
                         [(:default-source context) clause])]
    (when (or (not source) (not= 3 (count terms)))
      (throw (ex-info "Malformed pattern clause" {:clause clause})))
    (let [[e-term a-term v-term] terms
          datoms (source-datoms context source)]
      (reduce (fn [acc binding]
                (reduce (fn [inner [e a v]]
                          (if-let [b (-> binding
                                         (unify-term e-term e)
                                         (unify-term a-term a)
                                         (unify-term v-term v))]
                            (conj inner b)
                            inner))
                        acc
                        datoms))
              [] bindings))))

(defn- eval-clause [bindings clause context]
  (cond
    (predicate-clause? clause) (eval-predicate bindings clause)
    (vector? clause) (eval-pattern bindings clause context)
    :else (throw (ex-info "Unsupported clause type" {:clause clause}))))

(defn- parse-find [find-spec]
  (let [scalar? (and (seq find-spec) (= '. (last find-spec)))
        elements (vec (if scalar? (butlast find-spec) find-spec))
        coll-spec (some (fn [el]
                          (when (and (vector? el)
                                     (<= 2 (count el))
                                     (= '... (last el)))
                            el))
                        elements)
        type (cond
               scalar? :scalar
               coll-spec :collection
               :else :relation)
        parsed (if coll-spec
                 {:type type
                  :elements [(first coll-spec)]}
                 {:type type
                  :elements elements})]
    (when (and (= :scalar (:type parsed))
               (not= 1 (count (:elements parsed))))
      (throw (ex-info "Scalar :find expects exactly one element" {:find find-spec})))
    (when (and (= :collection (:type parsed))
               (not= 1 (count (:elements parsed))))
      (throw (ex-info "Collection :find expects exactly one element" {:find find-spec})))
    parsed))

(defn- aggregator-form? [form]
  (and (seq? form)
       (contains? aggregations (first form))))

(defn- pull-form? [form]
  (and (seq? form)
       (= 'pull (first form))))

(defn- aggregate-value [bindings form]
  (let [agg-sym (first form)
        term (second form)
        agg-fn (get aggregations agg-sym)]
    (when-not agg-fn
      (throw (ex-info "Unknown aggregate" {:aggregate agg-sym})))
    (let [values (->> bindings
                      (map #(resolve-value % term))
                      (remove nil?))]
      (agg-fn values))))

(defn- pull-source [source context binding]
  (cond
    (nil? source)
    (:default-fact-store context)

    (source-symbol? source)
    (get-in context [:sources source :fact-store])

    (symbol? source)
    (let [value (resolve-value binding source)]
      (when (df/factstore? value) value))

    (df/factstore? source)
    source

    :else nil))

(defn- pull-value [binding form context]
  (let [[_ & args] form
        [source-expr eid-expr selector-expr] (if (>= (count args) 3)
                                               [(first args) (second args) (nth args 2)]
                                               [nil (first args) (second args)])
        fact-store (pull-source source-expr context binding)
        eid (resolve-value binding eid-expr)
        selector (resolve-value binding selector-expr)]
    (when-not fact-store
      (throw (ex-info "Pull requires a fact store bound to default source or explicit source"
                      {:source source-expr})))
    (when eid
      (dpull/pull fact-store selector eid))))

(defn- evaluate-element [binding element context]
  (cond
    (pull-form? element) (pull-value binding element context)
    (symbol? element) (resolve-value binding element)
    :else element))

(defn- distinct-if-needed [values with-vars]
  (if (seq with-vars)
    (-> values distinct vec)
    values))

(defn- render-aggregate [bindings elements type]
  (let [row (mapv #(aggregate-value bindings %) elements)]
    (case type
      :scalar (first row)
      :collection row
      :relation [row])))

(defn- render-relation [bindings elements context]
  (mapv (fn [binding]
          (mapv #(evaluate-element binding % context) elements))
        bindings))

(defn- format-results [bindings {:keys [type elements]} context with-vars]
  (let [agg-count (count (filter aggregator-form? elements))]
    (cond
      (pos? agg-count)
      (do
        (when (not= agg-count (count elements))
          (throw (ex-info "Mixing aggregates with raw fields is not supported"
                          {:find elements})))
        (let [result (render-aggregate bindings elements type)]
          (if (and (= :relation type) (seq with-vars))
            (distinct-if-needed result with-vars)
            result)))

      (= :collection type)
      (let [values (mapv #(evaluate-element % (first elements) context) bindings)]
        (distinct-if-needed values with-vars))

      :else
      (let [rows (render-relation bindings elements context)
            rows (distinct-if-needed rows with-vars)]
        (case type
          :scalar (some-> rows first first)
          :relation rows)))))

(defn q
  "Evaluate a datalog-style query against one or more fact partitions."
  [query & inputs]
  (let [qmap        (normalize-query query)
        find-spec   (parse-find (coerce-vector (ensure-section qmap :find)))
        where-spec  (coerce-vector (ensure-section qmap :where))
        raw-in      (coerce-vector (:in qmap))
        in-spec     (vec (if (seq raw-in) raw-in '[$]))
        with-vars   (coerce-vector (:with qmap))
        inputs      (vec inputs)
        {:keys [context binding]} (bind-inputs in-spec inputs)
        initial-binding (or binding {})
        start-bindings  [initial-binding]
        final-bindings  (reduce (fn [result clause]
                                  (eval-clause result clause context))
                                start-bindings
                                where-spec)]
    (format-results final-bindings find-spec context with-vars)))
