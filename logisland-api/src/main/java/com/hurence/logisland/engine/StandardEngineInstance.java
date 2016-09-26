package com.hurence.logisland.engine;

import com.hurence.logisland.component.AbstractConfiguredComponent;
import com.hurence.logisland.component.ConfigurableComponent;

/**
 * Created by tom on 01/07/16.
 */
public class StandardEngineInstance extends AbstractConfiguredComponent {

    private final StreamProcessingEngine engine;

    public StandardEngineInstance(StreamProcessingEngine engine, String id) {
        super((ConfigurableComponent)engine, id);
        this.engine = engine;


    }

    public StreamProcessingEngine getEngine() {
        return engine;
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {

    }
}
