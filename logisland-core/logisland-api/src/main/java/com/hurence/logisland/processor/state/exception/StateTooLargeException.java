package com.hurence.logisland.processor.state.exception;

import com.hurence.logisland.processor.state.StateManager;

import java.io.IOException;

/**
 * Thrown when attempting to store state via the {@link StateManager} but the state being
 * stored is larger than is allowed by the backing storage mechanism.
 */
public class StateTooLargeException extends IOException {
    private static final long serialVersionUID = 1L;

    public StateTooLargeException(final String message) {
        super(message);
    }
}
