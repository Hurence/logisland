(ns opencv4.videosample
  (:require
    [opencv4.core :refer :all]
    [opencv4.video :as v]
    [opencv4.utils :as u]))

(defn -main[ & args]
  (u/simple-cam-window
    (fn [buffer]
    (u/resize-by buffer 0.5)
    (let [ output (new-mat) bottom (-> buffer clone (flip! -1)) ]
     (-> buffer (cvt-color! COLOR_RGB2GRAY) (cvt-color! COLOR_GRAY2RGB))
     (put-text buffer (str (java.util.Date.)) (new-point 10 50) FONT_HERSHEY_PLAIN 1 (new-scalar 255 255 0) 1)
     (vconcat [buffer bottom] output)
     output))))

(comment
  (-main)
  )
