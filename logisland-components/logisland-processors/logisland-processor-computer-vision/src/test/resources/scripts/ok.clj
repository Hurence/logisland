(ns opencv4.ok
  (:require
    [opencv4.utils :as u]
    [opencv4.core :refer :all]))

(defn -main [& args]
  (println "Using OpenCV Version: " VERSION "..")
	(->
    (imread "resources/cat.jpg")
    (cvt-color! COLOR_RGB2GRAY)
    (imwrite "grey-neko.jpg")
    (println "A new gray neko has arise!")))

  (-> 
    (imread "resources/cat.jpg")
    (cvt-color! COLORMAP_JET) 
    (resize! (new-size 150 100))
    (imwrite "resources/jet-neko.jpg"))

    (-> "http://eskipaper.com/images/jump-cat-1.jpg"
      (u/mat-from-url)
      (u/resize-by 0.3)
      (u/show))