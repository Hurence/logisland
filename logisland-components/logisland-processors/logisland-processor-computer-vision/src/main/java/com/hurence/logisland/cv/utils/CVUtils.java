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
package com.hurence.logisland.cv.utils;

import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class CVUtils {

    private static Logger logger = LoggerFactory.getLogger(CVUtils.class);

    public static BufferedImage toBI(Mat matrix) throws IOException {
        MatOfByte mob = new MatOfByte();
        Imgcodecs.imencode(".jpg", matrix, mob);
        return ImageIO.read(new ByteArrayInputStream(mob.toArray()));
    }


    public static Mat toMat(BufferedImage bi) {
        byte[] pixels = ((DataBufferByte) bi.getRaster().getDataBuffer()).getData();
        Mat mat = new Mat(bi.getHeight(), bi.getWidth(), CvType.CV_8UC3);
        mat.put(0, 0, pixels);
        return mat;
    }

    public static Mat toMat(InputStream is) throws IOException {
        BufferedImage originalImage = ImageIO.read(is);
        return toMat(originalImage);
    }

    public static Mat toMat(byte[] bytes) throws IOException {
        return toMat(toBI(bytes));
    }

    public static Mat toMat(Record record, String fieldName){
        if(record == null || fieldName == null || !record.hasField(fieldName)){
            throw new ProcessException("Unable to extract a matrix from record");
        }

        try {
            // make an image from bytes
            byte[] inputBytes = record.getField(fieldName).asBytes();
            BufferedImage originalImage = ImageIO.read(new ByteArrayInputStream(inputBytes));
            return CVUtils.toMat(originalImage);
        }catch (Exception e){
            logger.error(e.toString());
            return null;
        }
    }

    public static BufferedImage toBI(byte[] bytes) throws IOException {
        return ImageIO.read(new ByteArrayInputStream(bytes));
    }

    public static byte[] toBytes(Mat matrix, String imageFormat) throws IOException {
        return toBytes(toBI(matrix), imageFormat);
    }

    public static byte[] toBytes(BufferedImage bufferedImage, String imageFormat) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, imageFormat, byteArrayOutputStream);
        byteArrayOutputStream.flush();
        byte[] outputBytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        return outputBytes;
    }
}
