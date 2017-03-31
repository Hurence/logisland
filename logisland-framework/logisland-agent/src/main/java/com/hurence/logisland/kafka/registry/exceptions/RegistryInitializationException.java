package com.hurence.logisland.kafka.registry.exceptions;

/**
 * Indicates an error while initializing schema registry
 */
public class RegistryInitializationException extends RegistryException {

  public RegistryInitializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryInitializationException(String message) {
    super(message);
  }

  public RegistryInitializationException(Throwable cause) {
    super(cause);
  }

  public RegistryInitializationException() {
    super();
  }
}
