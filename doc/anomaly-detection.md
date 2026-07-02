# Anomaly Detection (ECOD)

> **Experimental.** The `datahike.experimental.anomaly` namespace is evolving and
> its API may change between releases.

Datahike ships **ECOD** — *Empirical Cumulative Distribution-based Outlier
Detection* (Li et al., IEEE TKDE 2022, <https://arxiv.org/abs/2201.00382>). ECOD
is unsupervised, parameter-free, interpretable, and runs in `O(n log n)` per
feature. It scores each sample by how far it sits in the tails of each feature's
empirical distribution.

```clojure
(require '[datahike.experimental.anomaly :as anomaly])
```

It works on plain numeric feature vectors (a vector of samples, each a vector of
features) on both Clojure and ClojureScript.

## API

```clojure
;; Raw anomaly scores (higher = more anomalous), one per sample.
(anomaly/ecod-scores data)

;; Detect outliers. Returns [{:index :score :point} ...].
(anomaly/find-outliers data)                       ; default contamination 0.1
(anomaly/find-outliers data {:contamination 0.05})
(anomaly/find-outliers data {:top-k 5})
(anomaly/find-outliers data {:threshold 8.0})

;; Score one new point against a dataset.
(anomaly/ecod-score training-data point)

;; Per-dimension contributions, for interpretability.
(anomaly/ecod-explain data point-idx)
;; => [{:dimension 0 :value .. :ecdf-left .. :ecdf-right .. :skewness .. :score ..} ...]
```

## Example

```clojure
(def data
  (into [[5.0 5.0] [-4.0 4.0]]        ;; two clear outliers
        (repeatedly 50 #(vector (rand) (rand)))))

(anomaly/find-outliers data {:top-k 2})
;; => the two extreme points, each with its score and original feature vector

(anomaly/ecod-explain data 0)
;; => which feature dimensions made sample 0 anomalous
```

## Combining with graph algorithms

ECOD is a natural companion to the [graph algorithms](graph-algorithms.md):
compute structural features per node, then run ECOD over them to surface
structurally unusual nodes.

```clojure
(require '[datahike.experimental.graph-spec :as gs]
         '[datahike.experimental.graph :as graph])

(let [g       (gs/materialize (gs/attr-graph :follows) db)
      nodes   (gs/all-nodes g db)
      pr      (graph/page-rank g db)
      between (graph/betweenness-centrality g db)
      ;; one feature vector per node: [degree pagerank betweenness]
      feats   (mapv (fn [n] [(graph/degree g db n) (pr n 0.0) (get between n 0.0)]) nodes)
      flagged (anomaly/find-outliers feats {:contamination 0.05})]
  (map (fn [{:keys [index score]}] [(nth nodes index) score]) flagged))
```

This is the building block behind egonet-style graph anomaly detection — see
[oddball-anomaly-detection.md](oddball-anomaly-detection.md) for a worked recipe.

## Reference

- Li, Zhao, Hu, Botta, Ionescu, Chen — *ECOD: Unsupervised Outlier Detection
  Using Empirical Cumulative Distribution Functions*, IEEE TKDE 2022.
- Scores match [PyOD](https://pyod.readthedocs.io/)'s ECOD on reference inputs
  (used as a correctness baseline in the tests).
