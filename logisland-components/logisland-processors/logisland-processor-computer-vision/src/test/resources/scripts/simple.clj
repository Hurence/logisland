(ns opencv4.simple
  (:require opencv4.utils)
  (:require [opencv4.core :refer :all])
  (:import
    [org.opencv.core MatOfKeyPoint MatOfRect Point Rect Mat Size Scalar Core CvType Mat MatOfByte]))


; view the bitmap of the eye mat
(def mat (Mat/eye 3 3 CvType/CV_8UC1))
(.dump mat)

; view the bitmap of an all ones mat
(def mat2 (Mat/ones 3 3 CvType/CV_8UC1))
(.dump mat2)

; simple mat to file
(def mat3 (Mat. 100 200 CV_8UC3 (new-scalar 127 127 255)))
(imwrite mat3 "output/simple.png")

; flip
; 0 : flip x
; 1 ; flip y
; -1 ; flip x and y
(-> "resources/cat.jpg"
  (imread)
  (flip! 1)
  (imwrite "output/simple.png"))

; change color map
(-> "resources/cat.jpg"
  (imread)
  (cvt-color! COLORMAP_JET)
  (imwrite "output/simple.png"))

; write circle
; just line
(def im (-> "resources/cat.jpg" (imread)))
(circle im (new-point 800 400) 200 (new-scalar 0 0 0) 10)
(imwrite  im "output/simple.png")

; colored
(circle im (new-point 800 400) 200 (new-scalar 0 0 0) -1)
(imwrite  im "output/simple.png")

; resizing a picture
(-> "resources/cat.jpg"
(imread)
(resize! (new-size 150 100))
(imwrite  "output/simple.png"))

;
; rotation a picture
;
(->
"resources/cat.jpg"
 (imread)
 (warp-affine! (get-rotation-matrix-2-d (new-point 400 400) 90 1) (new-size 600 800) INTER_NEAREST)
 (imwrite  "output/simple.png"))

;;;
; THRESHOLD
;;;

; binary
(-> "resources/cat.jpg"
 (imread CV_8UC1)
 (threshold! 100.0 200.0 THRESH_BINARY)
 (imwrite "output/simple.png"))


; binary inverse
(-> "resources/cat.jpg"
 (imread CV_8UC1)
 (threshold! 100.0 200.0 THRESH_BINARY_INV)
 (imwrite "output/simple.png"))

; binary trunc
(-> "resources/cat.jpg"
 (imread CV_8UC1)
 (threshold! 100.0 200.0 THRESH_TRUNC)
 (imwrite "output/simple.png"))

; tozero
(->
  "resources/cat.jpg"
 (imread CV_8UC1)
 (threshold! 180.0 250.0 THRESH_TOZERO)
 (imwrite "output/simple.png"))


(->
 "resources/cat.jpg"
  (imread CV_8UC1)
  (threshold! 150.0 200.0 THRESH_TOZERO_INV)
  (imwrite "output/simple.png"))


; adaptive with binary and binary inv
(->
 "resources/cat.jpg"
  (imread CV_8UC1)
  (adaptive-threshold! 200.0 ADAPTIVE_THRESH_MEAN_C THRESH_BINARY 3 8)
  (imwrite "output/simple.png"))

(->
 "resources/cat.jpg"
  (imread CV_8UC1)
  (adaptive-threshold! 200.0 ADAPTIVE_THRESH_MEAN_C THRESH_BINARY_INV 3 4)
  (imwrite "output/simple.png"))

;;;
; histogram
;;;
; need to be loaded in black and white first
(->
 "resources/cat.jpg"
 (imread CV_8UC1)
 (equalize-hist!)
 (imwrite "output/simple.png"))

;;;
; Split colors and concat into single image
;;;
(def mat (imread "resources/cat.jpg" ))
(def dst (new-arraylist 3))
(split mat dst)

(def target (new-mat))
(vconcat (map #(.get dst %) (range 3) ) target)
(imwrite target "output/simple.png")

;;;
; filters
;;;
(-> "resources/cat.jpg"
(imread)
(bitwise-not!)
(imwrite "output/simple.png"))

;;;
; Blurs
;;;
(-> "resources/cat.jpg"
(imread)
(blur! (new-size 30 30))
(imwrite "output/simple.png"))

(-> "resources/cat.jpg"
(imread)
(median-blur! 11)
(imwrite "output/simple.png"))

; speed
(-> "resources/cat.jpg"
  (imread)
  (gaussian-blur! (new-size 31 5) 80 3)
  (imwrite "output/simple.png"))

; laplacian
(-> "resources/cat.jpg"
  (imread)
  (laplacian! -1)
  (imwrite "output/simple.png"))

; Sobel
(-> "resources/cat.jpg"
  (imread)
  (sobel! -1 0 1)
  (imwrite "output/simple.png"))

; Canny
(-> "resources/cat.jpg"
  (imread CV_8UC1)
  (canny! 50.0 250.0)
  (imwrite "output/simple.png"))

(def kernel (get-structuring-element MORPH_RECT (new-size 5 5)))
; dilate
(-> "resources/cat.jpg"
  (imread CV_8UC1)
  (dilate! kernel)
  (imwrite "output/simple.png"))

; erode
(-> "resources/cat.jpg"
  (imread CV_8UC1)
  (erode! kernel)
  (imwrite "output/simple.png"))

; box filter
(-> "resources/cat.jpg"
  (imread)
  (box-filter! -1 (new-size 100 100))
  (imwrite "output/simple.png"))


(defn -main [& args])
