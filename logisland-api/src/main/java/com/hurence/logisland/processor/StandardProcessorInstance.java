package com.hurence.logisland.processor;

import com.hurence.logisland.components.AbstractConfiguredComponent;

/**
 * Created by tom on 01/07/16.
 */
public class StandardProcessorInstance extends AbstractConfiguredComponent {

    private final AbstractRecordProcessor processor;

    public StandardProcessorInstance(AbstractRecordProcessor processor, String id) {
        super(processor, id);
        this.processor = processor;


    }

    public AbstractRecordProcessor getProcessor() {
        return processor;
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {

    }
}
