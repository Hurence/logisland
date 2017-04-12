
package com.hurence.logisland.controller;


import com.hurence.logisland.kerberos.KerberosContext;
import com.hurence.logisland.logging.ComponentLog;

public interface ControllerServiceInitializationContext extends KerberosContext {

    /**
     * @return the identifier associated with the {@link ControllerService} with
     * which this context is associated
     */
    String getIdentifier();

    /**
     * @return the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     */
    ControllerServiceLookup getControllerServiceLookup();


    /**
     * @return a logger that can be used to log important events in a standard
     * way and generate bulletins when appropriate
     */
    ComponentLog getLogger();
}
