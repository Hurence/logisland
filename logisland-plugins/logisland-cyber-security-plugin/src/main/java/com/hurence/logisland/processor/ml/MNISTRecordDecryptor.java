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

package com.hurence.logisland.processor.ml;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by pducjac on 07/06/17.
 */
public class MNISTRecordDecryptor implements RecordDecryptor {

    private static Logger logger = LoggerFactory.getLogger(MNISTRecordDecryptor.class);

    private INDArray featuredData = null;
    private INDArray labels = null;
    private boolean labeledRec = false;
    private long count = 1;

    @Override
    public void decrypt(Record record) { decrypt(record,false);}

    @Override
    public void decrypt(Record record, Boolean label) {
        labeledRec = label;
        byte[] recordValue = null;
        try {
            recordValue = (byte[]) record.getField(FieldDictionary.RECORD_VALUE).getRawValue();
        } catch (Exception e) {
            logger.error("Error while retrieving the record value : record skipped");
            return;
        }

        int shift = 0;
        // read the magic number
        readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
        shift += 4;

        int c = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
        count = c;
        shift += 4;
        int row = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
        shift += 4;
        int col = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
        shift += 4;

        float[][] featureData = new float[c][0];

        for (int i = 0; i < c; i++) {
            float[] featureVec = new float[row * col];
            byte[] im = Arrays.copyOfRange(recordValue, shift, shift + row * col);
            shift += row * col;
            for (int j = 0; j < im.length; ++j) {
                float v = (float) (im[j] & 255);
                featureVec[j] = v / 255.0F;
            }
            featureData[i] = featureVec;
        }
        featuredData = Nd4j.create(featureData);

        if (label) {
            // read the magic number
            readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;
            c = (int) readInt(Arrays.copyOfRange(recordValue, shift, shift + 4));
            shift += 4;
            float[][] labelData = new float[c][0];
            for (int i = 0; i < c; i++) {
                int lab = readByte(Arrays.copyOfRange(recordValue, shift, shift + 1));
                shift += 1;
                labelData[i] = new float[10];
                labelData[i][lab] = 1.0F;
            }
            labels = Nd4j.create(labelData);
        }
    }

    @Override
    public INDArray getFeaturedData() {
        return featuredData;
    }

    @Override
    public INDArray getLabels() {
        return labels;
    }

    @Override
    public boolean isLabeledRecords() {
        return labeledRec;
    }

    @Override
    public long getCount() {
        return count;
    }

    private short readByte(byte[] buffer) {
        return ((short) (buffer[0] & 0xFF));
    }

    private long readInt(byte[] buffer) {
        return ((long) (((buffer[0] & 0xFF) << 24) | ((buffer[1] & 0xFF) << 16)
                | ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF)));
    }
}
