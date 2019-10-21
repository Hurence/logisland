package com.hurence.logisland.processor;

import clojure.lang.RT;
import clojure.lang.Var;

import java.io.IOException;

public class ClojureWrapper {

    public static void main(String[] args) throws IOException {


        // Load the Clojure script -- as a side effect this initializes the runtime.
        RT.loadResourceScript("tiny.clj");

        // Get a reference to the foo function.
        Var foo = RT.var("user", "foo");

        // Call it!
        Object result = foo.invoke("Hi", "there");
        System.out.println(result);
    }
}
