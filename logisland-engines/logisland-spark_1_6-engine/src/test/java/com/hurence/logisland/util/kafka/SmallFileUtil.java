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
package com.hurence.logisland.util.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Small class to extract content from small files. Should not be used with big files.
 * Created by lhubert on 15/04/16.
 */
public class SmallFileUtil {

    /**
     * Returns the content of a file
     * @param file
     * @return
     * @throws IOException
     */
    public static String getContent(File file) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line ;
        String content = "";

        // GOTO The first article
        while ((line = reader.readLine()) != null) {
            content += line;
        }
        return content;
    }

}
