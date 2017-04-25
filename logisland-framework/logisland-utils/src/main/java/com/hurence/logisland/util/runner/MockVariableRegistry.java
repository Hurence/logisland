
package com.hurence.logisland.util.runner;



import com.hurence.logisland.registry.VariableDescriptor;
import com.hurence.logisland.registry.VariableRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MockVariableRegistry implements VariableRegistry {

    private final Map<VariableDescriptor, String> variables = new HashMap<>();

    @Override
    public Map<VariableDescriptor, String> getVariableMap() {
        return Collections.unmodifiableMap(variables);
    }

    public void setVariable(final VariableDescriptor descriptor, final String value) {
        variables.put(descriptor, value);
    }

    public String removeVariable(final VariableDescriptor descriptor) {
        return variables.remove(descriptor);
    }
}
