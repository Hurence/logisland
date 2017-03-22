package com.hurence.logisland.kafka.registry.exceptions;

/**
 * Indicates that the version is not a valid version id. Allowed values are between [1,
 * 2^31-1] and the string "latest"
 */
public class InvalidVersionException extends RegistryException {
  public InvalidVersionException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidVersionException(String message) {
    super(message);
  }

  public InvalidVersionException(Throwable cause) {
    super(cause);
  }

  public InvalidVersionException() {
    super();
  }
}
