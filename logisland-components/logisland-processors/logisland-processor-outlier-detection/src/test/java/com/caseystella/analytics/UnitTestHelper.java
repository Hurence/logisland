/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.caseystella.analytics;



import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Level;

public class UnitTestHelper {

    private static Logger logger = LoggerFactory.getLogger(UnitTestHelper.class);
    public static String findDir(String name) {
        return findDir(new File("."), name);
    }

    public static String findDir(File startDir, String name) {
        Stack<File> s = new Stack<File>();
        s.push(startDir);
        while(!s.empty()) {
            File parent = s.pop();
            if(parent.getName().equalsIgnoreCase(name)) {
                return parent.getAbsolutePath();
            }
            else {
                File[] children = parent.listFiles();
                if(children != null) {
                    for (File child : children) {
                        s.push(child);
                    }
                }
            }
        }
        return null;
    }

    public static <T> void assertSetEqual(String type, Set<T> expectedVals, Set<T> found) {
        boolean mismatch = false;
        for(T f : found) {
            if(!expectedVals.contains(f)) {
                mismatch = true;
                System.out.println("Found " + type + " that I did not expect: " + f);
            }
        }
        for(T expectedId : expectedVals) {
            if(!found.contains(expectedId)) {
                mismatch = true;
                System.out.println("Expected " + type + " that I did not index: " + expectedId);
            }
        }
        Assert.assertFalse(mismatch);
    }


}
