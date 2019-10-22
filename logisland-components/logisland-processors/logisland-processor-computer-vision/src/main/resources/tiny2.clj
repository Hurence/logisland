; foo.clj
(ns user
  (:refer-clojure :exclude [sort min merge reduce max compare repeat])
  (:require [opencv4.utils :refer :all])
  (:require [logisland.utils :as ld])
  (:require [opencv4.core :refer :all])
  (:import
    [com.hurence.logisland.record Record]))


(defn record_updater [r]
  (.setId r (str "cj-" (.getId r) )))

(defn foo [a b]
  (ld/foo-utils a b))

(defn write_mat3 [mat]
  (-> mat
   (resize-by 0.5)
   (cvt-color! COLOR_RGB2GRAY)
   (canny! 300.0 100.0 3 true)
   (bitwise-not!)
   ))