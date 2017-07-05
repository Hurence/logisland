package com.hurence.logisland.processor.binetflow;

import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.ml.scaler.Scaler;
import com.hurence.logisland.ml.scaling.StandardScalerModelWrapper;
import com.hurence.logisland.service.ml.ScalerModelClientService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class MockScalerModelClientService extends AbstractControllerService implements ScalerModelClientService {

    private volatile String scalerModelName = null;
    private volatile String scalerModelFilePath = null;

    private volatile Scaler scalerModel = null;

    private MockScalerModelClientService(){}

    public MockScalerModelClientService(String scalerModelName, String scalerModelFilePath){
        this.scalerModelName = scalerModelName;
        this.scalerModelFilePath = scalerModelFilePath;
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {

        // Standard scaler :
        if(scalerModelName == "standard_scaler") {
            scalerModel = new StandardScalerModelWrapper(scalerModelFilePath);
        }

    }

    @Override
    public Scaler getScalerModel() {
        return scalerModel;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();

        return Collections.unmodifiableList(props);
    }

}
