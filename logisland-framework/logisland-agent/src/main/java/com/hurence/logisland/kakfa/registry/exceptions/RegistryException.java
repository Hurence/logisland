package com.hurence.logisland.kakfa.registry.exceptions;

/**
 * Indicates some error while performing a schema registry operation
 */
public class RegistryException extends Exception {

  public RegistryException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryException(String message) {
    super(message);
  }

  public RegistryException(Throwable cause) {
    super(cause);
  }

  public RegistryException() {
    super();
  }
}
