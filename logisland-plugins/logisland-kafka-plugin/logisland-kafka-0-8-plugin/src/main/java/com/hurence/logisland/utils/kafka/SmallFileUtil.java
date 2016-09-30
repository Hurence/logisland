package com.hurence.logisland.utils.kafka;

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
