package com.hurence.logisland.cv.processor;

import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.commons.io.IOUtils;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.scijava.nativelib.NativeLoader;


import java.io.*;

import static com.hurence.logisland.utils.CVUtils.toMat;


public class ClojureWrapper {

    public static final int LOOP_COUNT = 200;




    public static void main(String[] args) throws IOException {
        NativeLoader.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        callLoop(true);

        // Call it!
      /*  Object result = foo.invoke("Hi", "there");
        System.out.println(result);


        Record record = new StandardRecord("test_record").setLongField("long_a", 12L);

        Object result2 = recordUpdater.invoke(record);
        System.out.println(result2);*/
    }

    private static void callLoop(boolean doWarmup) throws IOException {

        ClassLoader classLoader = ClojureWrapper.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("cat.jpg");
        byte[] bytes = IOUtils.toByteArray(inputStream);

        Mat mat = toMat(bytes);
        long start = System.currentTimeMillis();

        // Load the Clojure script -- as a side effect this initializes the runtime.
        RT.loadResourceScript("tiny2.clj");
        // Get a reference to the foo function.
        Var foo = RT.var("user", "foo");
        Var img = RT.var("user", "write_mat3");
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
