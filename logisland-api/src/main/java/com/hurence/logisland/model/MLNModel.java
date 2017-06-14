package com.hurence.logisland.model;

import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;

import java.util.Collection;
import java.util.List;

/**
 * Created by pducjac on 31/05/17.
 */
public class MLNModel  extends AbstractConfigurableComponent implements Model {
    MultiLayerNetwork model = null;

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

    public void setMLNModel(MultiLayerNetwork model) {
        this.model = model;
    }

    public MultiLayerNetwork getMLNModel() {
        return(model);
    }

    public void loadMLNModel(String path)
    {

    }
}
