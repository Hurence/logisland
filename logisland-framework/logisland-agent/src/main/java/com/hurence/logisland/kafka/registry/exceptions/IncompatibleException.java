package com.hurence.logisland.kafka.registry.exceptions;

/**
 * Indicates the schema is incompatible with the registered schema
 */
public class IncompatibleException extends RegistryException {
  public IncompatibleException(String message, Throwable cause) {
    super(message, cause);
  }

  public IncompatibleException(String message) {
    super(message);
  }

  public IncompatibleException(Throwable cause) {
    super(cause);
  }

  public IncompatibleException() {
    super();
  }
}
