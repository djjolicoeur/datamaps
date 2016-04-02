(ns datamaps.pull
  (:require [datamaps.facts :as df]
            [datascript.query :as dq]
            [datascript.pull-parser :as dpp])
  (:import [datascript.pull_parser PullSpec]))

;; Datascript's pull API adpated for use with arbitrary maps
;; implemeting the IFactStore protocol

(defn eid-datoms [facts eid]
  (dq/q '[:find ?e ?a ?v
          :in $ ?e
          :where [?e ?a ?v]]
        (df/fact-partition facts) eid))

(defn forward-attr-datoms [facts eid attr]
  (dq/q '[:find ?e ?a ?v
          :in $ ?e ?a
          :where [?e ?a ?v]]
        (df/fact-partition facts) eid attr))

(defn reverse-attr-datoms [facts attr eid]
  (dq/q '[:find ?e ?a ?v
          :in $ ?a ?v
          :where [?e ?a ?v]]
        (df/fact-partition facts) attr eid))

(defn- into!
  [transient-coll items]
  (reduce conj! transient-coll items))

(def ^:private ^:const +default-limit+ 1000)

(defn- initial-frame
  [pattern eids multi?]
  {:state     :pattern
   :pattern   pattern
   :wildcard? (:wildcard? pattern)
   :specs     (-> pattern :attrs seq)
   :results   (transient [])
   :kvps      (transient {})
   :eids      eids
   :multi?    multi?
   :recursion {:depth {} :seen #{}}})

(defn- subpattern-frame
  [pattern eids multi? attr]
  (assoc (initial-frame pattern eids multi?) :attr attr))

(defn- reset-frame
  [frame eids kvps]
  (let [pattern (:pattern frame)]
    (assoc frame
           :eids      eids
           :specs     (seq (:attrs pattern))
           :wildcard? (:wildcard? pattern)
           :kvps      (transient {})
           :results   (cond-> (:results frame)
                        (seq kvps) (conj! kvps)))))

(defn- push-recursion
  [rec attr eid]
  (let [{:keys [depth seen]} rec]
    (assoc rec
           :depth (update depth attr (fnil inc 0))
           :seen (conj seen eid))))

(defn- seen-eid?
  [frame eid]
  (-> frame
      (get-in [:recursion :seen] #{})
      (contains? eid)))

(defn- pull-seen-eid
  [frame frames eid]
  (when (seen-eid? frame eid)
    (conj frames (update frame :results conj! eid))))

(defn- single-frame-result
  [key frame]
  (some-> (:kvps frame) persistent! (get key)))

(def ^:private recursion-result
  (partial single-frame-result ::recursion))

(defn- recursion-frame
  [parent eid]
  (let [attr (:attr parent)
        rec  (push-recursion (:recursion parent) attr eid)]
    (assoc (subpattern-frame (:pattern parent) [eid] false ::recursion)
           :recursion rec)))

(defn- pull-recursion-frame
  [facts [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (let [frame  (reset-frame frame (rest eids) (recursion-result frame))
          eid    (first eids)]
      (or (pull-seen-eid frame frames eid)
          (conj frames frame (recursion-frame frame eid))))
    (let [kvps    (recursion-result frame)
          results (cond-> (:results frame)
                    (seq kvps) (conj! kvps))]
      (conj frames (assoc frame :state :done :results results)))))

(defn- recurse-attr
  [facts attr multi? eids eid parent frames]
  (let [{:keys [recursion pattern]} parent
        depth  (-> recursion (get :depth) (get attr 0))]
    (if (-> pattern :attrs (get attr) :recursion (= depth))
      (conj frames parent)
      (pull-recursion-frame
       facts
       (conj frames parent
             {:state :recursion :pattern pattern
              :attr attr :multi? multi? :eids eids
              :recursion recursion
              :results (transient [])})))))

(let [pattern (PullSpec. true {})]
  (defn- expand-frame
    [parent eid attr-key multi? eids]
    (let [rec (push-recursion (:recursion parent) attr-key eid)]
      (-> pattern
          (subpattern-frame eids multi? attr-key)
          (assoc :recursion rec)))))


(defn- pull-attr-datoms
  [facts attr-key attr eid forward? datoms opts [parent & frames]]
  (let [limit (get opts :limit +default-limit+)
        found (not-empty
               (cond->> datoms
                 limit (into [] (take limit))))]
    (if found
      (let [attr-type (df/attr-meta facts eid attr)
            ref?      (= df/ref-type attr-type)
            multi?     (when forward? (= df/coll-type attr-type))
            datom-val  (if forward? (fn [[e a v]] v) (fn [[e a v]] e))]
        (cond
          (contains? opts :subpattern)
          (->> (subpattern-frame (:subpattern opts)
                                 (mapv datom-val found)
                                 multi? attr-key)
               (conj frames parent))

          (contains? opts :recursion)
          (recurse-attr facts attr-key multi?
                        (mapv datom-val found)
                        eid parent frames)

          :else
          (let [as-value  datom-val
                single?   (not multi?)]
            (->> (cond-> (into [] (map as-value) found)
                   single? first)
                 (update parent :kvps assoc! attr-key)
                 (conj frames)))))
      (->> (cond-> parent
             (contains? opts :default)
             (update :kvps assoc! attr-key (:default opts)))
           (conj frames)))))

(defn- pull-attr
  [facts spec eid frames]
  (let [[attr-key opts] spec]
    (let [attr     (:attr opts)
          forward? (= attr-key attr)
          results  (if forward?
                     (forward-attr-datoms facts eid attr)
                     (reverse-attr-datoms facts attr eid))]
      (pull-attr-datoms facts attr-key attr eid forward?
                        results opts frames))))

(def ^:private filter-reverse-attrs
  (filter (fn [[k v]] (not= k (:attr v)))))

(defn- expand-reverse-subpattern-frame
  [parent eid rattrs]
  (-> (:pattern parent)
      (assoc :attrs rattrs :wildcard? false)
      (subpattern-frame [eid] false ::expand-rev)))

(defn- expand-result
  [frames kvps]
  (->> kvps
       (persistent!)
       (update (first frames) :kvps into!)
       (conj (rest frames))))

(defn- pull-expand-reverse-frame
  [facts [frame & frames]]
  (->> (or (single-frame-result ::expand-rev frame) {})
       (into! (:expand-kvps frame))
       (expand-result frames)))

(defn- pull-expand-frame
  [facts [frame & frames]]
  (if-let [datoms-by-attr (seq (:datoms frame))]
    (let [[attr datoms] (first datoms-by-attr)
          opts          (-> frame
                            (get-in [:pattern :attrs])
                            (get attr {}))]
      (pull-attr-datoms facts attr attr (:eid frame) true datoms opts
                        (conj frames (update frame :datoms rest))))
    (if-let [rattrs (->> (get-in frame [:pattern :attrs])
                         (into {} filter-reverse-attrs)
                         not-empty)]
      (let [frame  (assoc frame
                          :state       :expand-rev
                          :expand-kvps (:kvps frame)
                          :kvps        (transient {}))]
        (->> rattrs
             (expand-reverse-subpattern-frame frame (:eid frame))
             (conj frames frame)))
      (expand-result frames (:kvps frame)))))

(defn- pull-wildcard-expand
  [facts frame frames eid pattern]
  (let [datoms (group-by (fn [[e a v]] a) (eid-datoms facts eid))
        {:keys [attr recursion]} frame
        rec (cond-> recursion
              (some? attr) (push-recursion attr eid))]
    (->> {:state :expand :kvps (transient {})
          :eid eid :pattern pattern :datoms (seq datoms)
          :recursion rec}
         (conj frames frame)
         (pull-expand-frame facts))))

(defn- pull-wildcard
  [facts frame frames]
  (let [{:keys [eid pattern]} frame]
    (or (pull-seen-eid frame frames eid)
        (pull-wildcard-expand facts frame frames eid pattern))))

(defn- pull-pattern-frame
  [facts [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (if (:wildcard? frame)
      (pull-wildcard facts
                     (assoc frame
                            :specs []
                            :eid (first eids)
                            :wildcard? false)
                     frames)
      (if-let [specs (seq (:specs frame))]
        (let [spec       (first specs)
              pattern    (:pattern frame)
              new-frames (conj frames (assoc frame :specs (rest specs)))]
          (pull-attr facts spec (first eids) new-frames))
        (->> frame :kvps persistent! not-empty
             (reset-frame frame (rest eids))
             (conj frames)
             (recur facts))))
    (conj frames (assoc frame :state :done))))

(defn- pull-pattern
  [facts frames]
  (case (:state (first frames))
    :expand     (recur facts (pull-expand-frame facts frames))
    :expand-rev (recur facts (pull-expand-reverse-frame facts frames))
    :pattern    (recur facts (pull-pattern-frame facts frames))
    :recursion  (recur facts (pull-recursion-frame facts frames))
    :done       (let [[f & remaining] frames
                      result (cond-> (persistent! (:results f))
                               (not (:multi? f)) first)]
                  (if (seq remaining)
                    (->> (cond-> (first remaining)
                           result (update :kvps assoc! (:attr f) result))
                         (conj (rest remaining))
                         (recur facts))
                    result))))

(defn pull-spec
  [facts pattern eids multi?]
  (pull-pattern facts (list (initial-frame pattern eids multi?))))

(defn pull [facts selector eid]
  {:pre [(df/factstore? facts)]}
  (pull-spec facts (dpp/parse-pull selector) [eid] false))

(defn pull-many [facts selector eids]
  {:pre [(df/factstore? facts)]}
    (pull-spec facts (dpp/parse-pull selector) eids true))
