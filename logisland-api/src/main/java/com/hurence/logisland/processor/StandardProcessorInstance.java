package com.hurence.logisland.processor;

import com.hurence.logisland.components.AbstractConfiguredComponent;
import com.hurence.logisland.components.ConfigurableComponent;

/**
 * Created by tom on 01/07/16.
 */
public class StandardProcessorInstance extends AbstractConfiguredComponent {

    private final AbstractEventProcessor processor;

    public StandardProcessorInstance(AbstractEventProcessor processor, String id) {
        super(processor, id);
        this.processor = processor;


    }

    public AbstractEventProcessor getProcessor() {
        return processor;
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {

    }
}
