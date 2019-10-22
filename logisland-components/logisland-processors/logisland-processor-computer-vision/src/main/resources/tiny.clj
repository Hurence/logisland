; foo.clj
(ns user
  (:require opencv4.utils)
  (:require [opencv4.core :refer :all])
  (:import
    [org.opencv.core MatOfKeyPoint MatOfRect Point Rect Mat Size Scalar Core CvType Mat MatOfByte]))

(defn foo [a b]
  (str a " " b))

(defn write_mat3 []
  (def im (-> "/Users/tom/Documents/workspace/logisland/logisland-components/logisland-processors/logisland-processor-computer-vision/src/main/resources/cat.jpg" (imread)))
  (circle im (new-point 800 400) 200 (new-scalar 0 0 0) -1)
  (imwrite  im "/Users/tom/Documents/workspace/logisland/logisland-components/logisland-processors/logisland-processor-computer-vision/src/main/resources/cat2.jpg"))