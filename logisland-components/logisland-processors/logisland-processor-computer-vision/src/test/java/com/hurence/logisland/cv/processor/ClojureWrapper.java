package com.hurence.logisland.cv.processor;

import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.commons.io.IOUtils;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.scijava.nativelib.NativeLoader;


import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

import static com.hurence.logisland.cv.utils.CVUtils.toBI;
import static com.hurence.logisland.cv.utils.CVUtils.toMat;


public class ClojureWrapper {

    public static final int LOOP_COUNT = 200;


    public static void main(String[] args) throws IOException {
        NativeLoader.loadLibrary(Core.NATIVE_LIBRARY_NAME);

        blurtheCat();
        greyCat();
        sepiaCat();
        redmaskCat();
    }


    private static void blurtheCat() throws IOException {

        ClassLoader classLoader = ClojureWrapper.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("img/cat/cat.jpg");
        byte[] bytes = IOUtils.toByteArray(inputStream);
        Mat mat = toMat(bytes);

        RT.loadResourceScript("scripts/opencv.clj");
        Var img = RT.var("com.hurence.logisland", "ld_blur");
        Mat clone = mat.clone();
        Mat processedMat = (Mat) img.invoke(clone);
        BufferedImage processedImage = toBI(processedMat);
        ImageIO.write(processedImage, "jpg", new File("img/processed/blured-cat.jpg"));


    }

    private static void greyCat() throws IOException {

        ClassLoader classLoader = ClojureWrapper.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("img/cat/cat.jpg");
        byte[] bytes = IOUtils.toByteArray(inputStream);
        Mat mat = toMat(bytes);

        RT.loadResourceScript("scripts/opencv.clj");
        Var img = RT.var("com.hurence.logisland", "ld_reduce_in_gray");
        Mat clone = mat.clone();
        Mat processedMat = (Mat) img.invoke(clone);
        BufferedImage processedImage = toBI(processedMat);
        ImageIO.write(processedImage, "jpg", new File("img/processed/gray-cat.jpg"));
    }

    private static void sepiaCat() throws IOException {

        ClassLoader classLoader = ClojureWrapper.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("img/cat/cat.jpg");
        byte[] bytes = IOUtils.toByteArray(inputStream);
        Mat mat = toMat(bytes);

        RT.loadResourceScript("scripts/opencv.clj");
        Var img = RT.var("com.hurence.logisland", "ld_sepia");
        Mat clone = mat.clone();
        Mat processedMat = (Mat) img.invoke(clone);
        BufferedImage processedImage = toBI(processedMat);
        ImageIO.write(processedImage, "jpg", new File("img/processed/sepia-cat.jpg"));
    }

    private static void redmaskCat() throws IOException {

        ClassLoader classLoader = ClojureWrapper.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("img/cat/cat.jpg");
        byte[] bytes = IOUtils.toByteArray(inputStream);
        Mat mat = toMat(bytes);

        RT.loadResourceScript("scripts/opencv.clj");
        Var img = RT.var("com.hurence.logisland", "ld_threshold");
        Mat clone = mat.clone();
        Mat processedMat = (Mat) img.invoke(clone);
        BufferedImage processedImage = toBI(processedMat);
        ImageIO.write(processedImage, "jpg", new File("img/processed/threshold-cat.jpg"));
    }

    private static void callLoop() throws IOException {

        ClassLoader classLoader = ClojureWrapper.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("img/cat/cat.jpg");
        byte[] bytes = IOUtils.toByteArray(inputStream);

        Mat mat = toMat(bytes);
        long start = System.currentTimeMillis();
        boolean doWarmup = true;

        // Load the Clojure script -- as a side effect this initializes the runtime.
        RT.loadResourceScript("scripts/opencv.clj");
        // Get a reference to the foo function.
        Var foo = RT.var("user", "foo");
        Var img = RT.var("com.hurence.logisland", "ld_detect_edges");
        Var recordUpdater = RT.var("user", "record_updater");


        for (int i = 0; i < 2 * LOOP_COUNT; i++) {
            try {
                //BufferedImage processedImage = (BufferedImage) img.invoke(originalImage);
                Mat clone = mat.clone();
                Mat processedMat = (Mat) img.invoke(clone);
              /*  BufferedImage processedImage = toBI(processedMat);
                ImageIO.write(processedImage, "jpg", new File("new-cat.jpg"));*/
                if (doWarmup && i > LOOP_COUNT) {
                    start = System.currentTimeMillis();
                    doWarmup = false;
                }

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        float imgBySec = 1000 * LOOP_COUNT / 2 / (System.currentTimeMillis() - start);
        System.out.println("Img processed by sec : " + imgBySec);


    }
}
