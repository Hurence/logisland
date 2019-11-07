(ns com.hurence.logisland
  (:refer-clojure :exclude
                  [sort min merge reduce max compare repeat])
  (:require [opencv4.utils :refer :all])
  (:require [opencv4.core :refer :all])
  (:import
    [com.hurence.logisland.record Record]))


(defn record_updater [r]
  (.setId r (str "cj-" (.getId r))))


(defn ld_detect_edges [mat]
  (-> mat
      (resize-by 0.5)
      (cvt-color! COLOR_RGB2GRAY)
      (canny! 300.0 100.0 3 true)
      (bitwise-not!)))


(defn ld_blur [mat]
  (-> mat
      (gaussian-blur! (new-size 17 17) 9 9)))

(defn ld_reduce_in_gray [mat]
  (-> mat
      (cvt-color! IMREAD_REDUCED_GRAYSCALE_4)))


; You may look around in the literature, but a nice sepia transformation would use the following matrix:
(def sepia-2
  (matrix-to-mat
   [[0.131 0.534 0.272]
    [0.168 0.686 0.349]
    [0.189 0.769 0.393]]))

(defn ld_sepia [mat]
  (-> mat
      (cvt-color! IMREAD_REDUCED_COLOR_4)
      (transform! sepia-2)))

; The function rotate! also takes a rotation parameter and turns the image according to it.
; Note also how clone and ->> can be used to create multiple mats from
;a single source.
(defn ld_rotate_concat [mat]
  (->> [ROTATE_90_COUNTERCLOCKWISE ROTATE_90_CLOCKWISE]
       (map #(-> mat clone (rotate! %)))
       (hconcat!)))

(defn ld_highlight_cat [mat]
  (-> mat
      (submat (new-rect 100 50 100 100))
      (cvt-color! COLOR_RGB2HLS)
      (multiply! (matrix-to-mat-of-double [[1.0 1.3 1.3]]))
      (cvt-color! COLOR_HLS2RGB)))


(defn ld_threshold [mat]
  (-> mat
      (cvt-color! COLOR_BGR2GRAY)
      (threshold! 150 250 THRESH_BINARY_INV)))