package com.hurence.logisland.processor.useragent;

/**
 * Created by mathieu on 22/03/17.
 * Hack in order to keep one instance alive in the JVM until Processor initialization is fixed.
 */
public class Singleton {

    private static Object o = null;

    public static void set(Object obj) {
        o = obj;
    }

    public static Object get() {
        return o;
    }

}
