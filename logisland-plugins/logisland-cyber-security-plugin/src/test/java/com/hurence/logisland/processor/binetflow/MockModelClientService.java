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
package com.hurence.logisland.processor.binetflow;

import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.ml.model.KMeansModelWrapper;
import com.hurence.logisland.ml.model.Model;
import com.hurence.logisland.service.ml.ModelClientService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockModelClientService extends AbstractControllerService implements ModelClientService {

    private volatile String modelLibrary = null;
    private volatile String modelName = null;
    private volatile String modelFilePath = null;

    private volatile Model model = null;

    private MockModelClientService(){}

    public MockModelClientService(String modelLibrary, String modelName, String modelFilePath){
        this.modelLibrary = modelLibrary;
        this.modelName = modelName;
        this.modelFilePath = modelFilePath;
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        // KMeans :
        if(modelName == "kmeans") {
            model = new KMeansModelWrapper(modelFilePath);
        }
    }

    @Override
    public Model getModel() {
        return model;
    }

    public Model restoreModel() {
        return null;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();

        return Collections.unmodifiableList(props);
    }
}
