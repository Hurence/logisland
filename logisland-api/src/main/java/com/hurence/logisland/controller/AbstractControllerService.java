
package com.hurence.logisland.controller;


import com.hurence.logisland.component.AbstractConfigurableComponent;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.logging.ComponentLog;

public abstract class AbstractControllerService extends AbstractConfigurableComponent implements ControllerService {


    private ControllerServiceLookup serviceLookup;
    private ComponentLog logger;

    @Override
    public final void initialize(final ControllerServiceInitializationContext context) throws InitializationException {
        this.identifier = context.getIdentifier();
        serviceLookup = context.getControllerServiceLookup();
        logger = context.getLogger();
        init(context);
    }


    /**
     * @return the {@link ControllerServiceLookup} that was passed to the
     * {@link #init(ControllerServiceInitializationContext)} method
     */
    protected final ControllerServiceLookup getControllerServiceLookup() {
        return serviceLookup;
    }

    /**
     * Provides a mechanism by which subclasses can perform initialization of
     * the Controller Service before it is scheduled to be run
     *
     * @param config of initialization context
     * @throws InitializationException if unable to init
     */
    protected void init(final ControllerServiceInitializationContext config) throws InitializationException {
    }

    /**
     * @return the logger that has been provided to the component by the
     * framework in its initialize method
     */
    protected ComponentLog getLogger() {
        return logger;
    }

}
