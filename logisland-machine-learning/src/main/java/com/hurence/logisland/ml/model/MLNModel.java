package com.hurence.logisland.ml.model;

import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;

import java.util.Collection;
import java.util.List;

/**
 * Created by pducjac on 31/05/17.
 */
public class MLNModel  extends AbstractConfigurableComponent implements Model<Integer,Integer> {
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
    public Integer predict(Integer input) throws Exception {
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
