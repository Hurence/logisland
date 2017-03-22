package com.hurence.logisland.kakfa.registry.exceptions;

/**
 * Indicates an invalid schema that does not conform to the expected format of the schema
 */
public class InvalidException extends RegistryException {
  public InvalidException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidException(String message) {
    super(message);
  }

  public InvalidException(Throwable cause) {
    super(cause);
  }

  public InvalidException() {
    super();
  }
}
