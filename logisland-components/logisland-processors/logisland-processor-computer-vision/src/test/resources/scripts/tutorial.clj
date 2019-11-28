(ns opencv4.tutorial
 (:require
   [opencv4.utils :as u]
   [opencv4.core :refer :all]))

; playing with bytes
(def image (new-mat 640 480 CV_8UC3))
(doseq [row (range 0 (.rows image))]
  (doseq [col (range 0 (.cols image))]
    (.put image row col (byte-array [(rand 250) 150 150]))))
(imwrite image "output/tutorial.png")

; mosaic
(->
  "resources/minicat.jpg"
  (imread)
  (resize! (new-size ) 0.1 0.1 INTER_NEAREST)
  (resize! (new-size) 10.0 10.0 INTER_NEAREST)
  (imwrite "output/tutorial.png"))

; denoising
(->
  "resources/nekobench.jpg"
  (imread)
  (fast-nl-means-denoising!)
  (imwrite "output/tutorial.png"))

; blurring
(->
  "resources/minicat.jpg"
  (imread)
  (blur! (new-size 5 5))
  (imwrite "output/tutorial.png"))

; grey scale
(->
  "resources/minicat.jpg"
  (imread)
  (cvt-color! COLOR_RGB2GRAY)
  (imwrite "output/tutorial.png"))

; laplacian
(->
  (imread "resources/minicat.jpg" 0)
  (laplacian! 10)
  (imwrite "output/tutorial.png"))

; levels, very slow
(def im (imread "resources/minicat.jpg"))
(def n 100)
(def sz (.size im))

(doseq [i (range 0 (.-height sz ))]
  (doseq [j (range 0 (.-width sz ))]
  (let [pixel (.get im i j)]
    (aset pixel 0 (+ (aget pixel 0) (/ n 2)))
    (aset pixel 1 (+ (aget pixel 1) (/ n 2)))
    (aset pixel 2 (+ (aget pixel 2) (/ n 2)))
    (.put im i j pixel))))

(imwrite im "output/tutorial.png")

;
; SAMPLING
;

; downsample the image
(def original (imread "resources/minicat.jpg"))
; (.size original)
(def im (clone original))
(pyr-down! im)
(imwrite im "output/tutorial.png")
; upsample the image
(pyr-up! im)
(imwrite im "output/tutorial.png")

; resize to the same size as the original
(resize! im (new-size (.height original) (.width original)))

; subtract original: this technique helps getting a nice outline of the picture
(subtract im original im)
(imwrite im "output/tutorial.png")

;
; THRESHOLD
;
(->
  "resources/minicat.jpg"
  (imread)
  (threshold! 100 240 THRESH_BINARY)
  (imwrite "output/tutorial.png"))

(->
  "resources/minicat.jpg"
  (imread)
  (threshold! 100 240 THRESH_BINARY_INV)
  (imwrite "output/tutorial.png"))

(->
  "resources/minicat.jpg"
  (imread)
  (threshold! 150 255 THRESH_BINARY)
  (imwrite "output/tutorial.png"))

; load and show image
(->
  "resources/minicat.jpg"
  (imread)
  (u/imshow))

; http://qiita.com/gutugutu3030/items/3907530ee49433420b37

;
; original gaussian blur on image
;
(->
  "resources/minicat.jpg"
  (imread)
  (cvt-color! COLOR_BGR2HSV)
  (median-blur! 3)
  (imwrite "output/tutorial.png"))

(->
  "resources/minicat.jpg"
  (imread)
  (cvt-color! COLOR_BGR2RGBA)
  (median-blur! 3)
  (imwrite "output/tutorial.png"))

;
; nice threshold on above gaussian
;
(->
  "resources/minicat.jpg"
  (imread)
  (threshold! 100 240 THRESH_BINARY)
  (imwrite "output/tutorial.png"))

;
; Transformation
;
; https://www.tutorialspoint.com/opencv/opencv_distance_transformation.htm

(def im (->
  "resources/minicat.jpg"
  (imread CV_8UC1)
  (threshold! 50 200 THRESH_BINARY)
  (distance-transform! DIST_L1 5)
  (imwrite "output/tutorial.png")))

(defn -main [& args])
