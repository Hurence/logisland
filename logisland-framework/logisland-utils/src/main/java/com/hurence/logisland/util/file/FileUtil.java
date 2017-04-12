/**
 * Copyright (C) 2017 Hurence
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.logisland.util.file;

import java.io.InputStream;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File utilities.
 *
 */
public class FileUtil {

    private static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    /**
     * load logs from resources as a string or return null if an error occurs
     */
    public static String loadFileContentAsString(String path, String encoding) {
        try {
            final InputStream is = FileUtil.class.getClassLoader().getResourceAsStream(path);
            assert is != null;
            byte[] encoded = IOUtils.toByteArray(is);
            is.close();
            return new String(encoded, encoding);
        } catch (Exception e) {
            logger.error(String.format("Could not load json file %s and convert to string", path), e);
            return null;
        }
    }

    /**
     * load file from resources as a bytes array or return null if an error occurs
     */
    public static byte[] loadFileContentAsBytes(String path) {
        try {
            final InputStream is = FileUtil.class.getClassLoader().getResourceAsStream(path);
            assert is != null;
            byte[] encoded = IOUtils.toByteArray(is);
            is.close();
            return encoded;
        } catch (Exception e) {
            logger.error(String.format("Could not load file %s and convert it to a bytes array", path), e);
            return null;
        }
    }

}
