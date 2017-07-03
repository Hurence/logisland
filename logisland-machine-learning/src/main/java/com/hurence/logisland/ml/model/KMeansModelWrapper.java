package com.hurence.logisland.ml.model;

import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.List;

public class KMeansModelWrapper extends AbstractConfigurableComponent implements Model<double[], Integer> {

    KMeansModel kMeansModel = null;

    private KMeansModelWrapper() {
    }

    public KMeansModelWrapper(String modelFilePath) {

        try {
            FileInputStream in = new FileInputStream(modelFilePath);
            ObjectInputStream ois = new ObjectInputStream(in);
            kMeansModel = (KMeansModel) (ois.readObject());
        } catch (Exception e) {
            System.out.println("Problem serializing: " + e);
        }
    }

    @Override
    public Integer predict(double[] values) throws Exception {
        Vector inputValues = Vectors.dense(values);
        return kMeansModel.predict(inputValues);
    }

    @Override
    public Record predict(Record inputRecord) throws Exception {
        return null;
    }

    @Override
    public Collection<Record> predict(Collection<Record> inputRecord) throws Exception {
        return null;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return null;
    }
}
