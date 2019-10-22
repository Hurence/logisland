package com.hurence.logisland.utils;

import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CVUtils {

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
        BufferedImage originalImage = ImageIO.read(new ByteArrayInputStream(bytes));
        return toMat(originalImage);
    }
}
