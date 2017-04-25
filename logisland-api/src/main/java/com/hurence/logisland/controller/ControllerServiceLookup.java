package com.hurence.logisland.controller;

import java.util.Set;

public interface ControllerServiceLookup {

    /**
     * @param serviceIdentifier of controller service
     * @return the ControllerService that is registered with the given
     * identifier
     */
    ControllerService getControllerService(String serviceIdentifier);

    /**
     * @param serviceIdentifier identifier of service to check
     * @return <code>true</code> if the Controller Service with the given
     * identifier is enabled, <code>false</code> otherwise. If the given
     * identifier is not known by this ControllerServiceLookup, returns
     * <code>false</code>
     */
    boolean isControllerServiceEnabled(String serviceIdentifier);

    /**
     * @param serviceIdentifier identifier of service to check
     * @return <code>true</code> if the Controller Service with the given
     * identifier has been enabled but is still in the transitioning state,
     * otherwise returns <code>false</code>. If the given identifier is not
     * known by this ControllerServiceLookup, returns <code>false</code>
     */
    boolean isControllerServiceEnabling(String serviceIdentifier);

    /**
     * @param service service to check
     * @return <code>true</code> if the given Controller Service is enabled,
     * <code>false</code> otherwise. If the given Controller Service is not
     * known by this ControllerServiceLookup, returns <code>false</code>
     */
    boolean isControllerServiceEnabled(ControllerService service);

    /**
     *
     * @param serviceType type of service to get identifiers for
     *
     * @return the set of all Controller Service Identifiers whose Controller
     *         Service is of the given type.
     * @throws IllegalArgumentException if the given class is not an interface
     */
    Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType) throws IllegalArgumentException;

    /**
     * @param serviceIdentifier identifier to look up
     * @return the name of the Controller service with the given identifier. If
     * no service can be found with this identifier, returns {@code null}
     */
    String getControllerServiceName(String serviceIdentifier);
}
