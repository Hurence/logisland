;;; to run this code from the terminal: 
;;; $ lein run -m opencv4.lena"
;;; It will save a blurred image version of resources/lena.png as
;;; target/blurred.png

(ns opencv4.lena
	(:require [opencv4.core :refer :all]))

(defn -main[& args]
	(-> "resources/lena.png"
		(imread)
		(gaussian-blur! (new-size 17 17) 9 9)
    (imwrite "resources/blurred.png")))
