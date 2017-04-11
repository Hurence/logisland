
package com.hurence.logisland.controller;


import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This context is passed to ControllerServices and Reporting Tasks in order
 * to expose their configuration to them.
 */
public interface ConfigurationContext {

    /**
     * @param property to retrieve by name
     * @return the configured value for the property with the given name
     */
    PropertyValue getPropertyValue(PropertyDescriptor property);

    /**
     * @return an unmodifiable map of all configured properties for this
     * {@link ControllerService}
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * @return a String representation of the scheduling period, or <code>null</code> if
     *         the component does not have a scheduling period (e.g., for ControllerServices)
     */
    String getSchedulingPeriod();

    /**
     * Returns the amount of time, in the given {@link TimeUnit} that will
     * elapsed between the return of one execution of the
     * component's <code>onTrigger</code> method and
     * the time at which the method is invoked again. This method will return
     * null if the component does not have a scheduling period (e.g., for ControllerServices)
     *
     * @param timeUnit unit of time for scheduling
     * @return period of time or <code>null</code> if component does not have a scheduling
     *         period
     */
    Long getSchedulingPeriod(TimeUnit timeUnit);
}
