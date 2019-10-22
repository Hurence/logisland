(ns logisland.utils
  (:require
    [opencv4.core :as cv]
    [opencv4.video :as vid]
    )
  (:import [org.opencv.core Size CvType Core Mat MatOfByte]
           [org.opencv.imgcodecs Imgcodecs]
           [org.opencv.videoio VideoCapture]
           [org.opencv.imgproc Imgproc]

           [java.net URL]
           [java.nio.channels ReadableByteChannel Channels]
           [java.io File FileOutputStream]

           [javax.imageio ImageIO]
           [javax.swing ImageIcon JFrame JLabel]
           [java.awt.event KeyListener MouseAdapter]
           [java.awt FlowLayout]))

;;;
; CLEAN
;;;
(defn clean-up-namespace[]
  (map #(ns-unmap *ns* %) (keys (ns-interns *ns*))))

;;;
;;;

(defn foo-utils [a b]
  (str "-- " a " " b))
