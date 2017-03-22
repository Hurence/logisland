package com.hurence.logisland.kafka.registry.exceptions;

/**
 * Indicates that some schema registry operation timed out.
 */
public class RegistryTimeoutException extends RegistryException {

  public RegistryTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryTimeoutException(String message) {
    super(message);
  }

  public RegistryTimeoutException(Throwable cause) {
    super(cause);
  }

  public RegistryTimeoutException() {
    super();
  }
}
