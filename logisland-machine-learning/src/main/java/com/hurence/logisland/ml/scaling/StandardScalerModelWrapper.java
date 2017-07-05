package com.hurence.logisland.ml.scaling;

import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.ml.scaler.Scaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.List;

public class StandardScalerModelWrapper extends AbstractConfigurableComponent implements Scaler<double[], double[]> {

    StandardScalerModel standardScalerModel = null;

    private StandardScalerModelWrapper() {
    }

    public StandardScalerModelWrapper(String scalerModelFilePath) {

        try {
            FileInputStream in = new FileInputStream(scalerModelFilePath);
            ObjectInputStream ois = new ObjectInputStream(in);
            standardScalerModel = (StandardScalerModel) (ois.readObject());
            ois.close();
        } catch (Exception e) {
            System.out.println("Problem deserializing: " + e);
        }
    }


    @Override
    public double[] transform(double[] values) throws Exception {
        Vector inputValues = Vectors.dense(values);
        return standardScalerModel.transform(inputValues).toArray();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return null;
    }
}
