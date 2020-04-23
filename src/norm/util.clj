(ns norm.util)

(defn flatten-map
  "Flattens any `coll` that contains a multy-level map
  into a vector of keys and values."
  [coll]
  (loop [in coll out []]
    (if-let [[h & tail] (seq in)]
      (if (coll? h)
        (recur (concat h tail) out)
        (recur tail (conj out h)))
      out)))