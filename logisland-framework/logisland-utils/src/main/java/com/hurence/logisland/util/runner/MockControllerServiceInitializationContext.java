
package com.hurence.logisland.util.runner;


import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.MockComponentLogger;

import java.io.File;

public class MockControllerServiceInitializationContext extends MockControllerServiceLookup
        implements ControllerServiceInitializationContext, ControllerServiceLookup {

    private final String identifier;
    private final ComponentLog logger;

    public MockControllerServiceInitializationContext(final ControllerService controllerService, final String identifier) {
        this.identifier = identifier;
        this.logger = new MockComponentLogger();
    }



    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        return null;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return null; //this needs to be wired in.
    }

    @Override
    public File getKerberosServiceKeytab() {
        return null; //this needs to be wired in.
    }

    @Override
    public File getKerberosConfigurationFile() {
        return null; //this needs to be wired in.
    }
}
