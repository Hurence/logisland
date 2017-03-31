package com.hurence.logisland.kafka.registry.exceptions;

/**
 * Indicates an error while performing an operation on the underlying data store that
 * stores all schemas in the registry
 */
public class RegistryStoreException extends RegistryException {

  public RegistryStoreException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryStoreException(String message) {
    super(message);
  }

  public RegistryStoreException(Throwable cause) {
    super(cause);
  }

  public RegistryStoreException() {
    super();
  }
}
