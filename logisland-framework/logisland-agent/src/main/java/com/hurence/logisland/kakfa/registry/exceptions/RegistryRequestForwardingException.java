package com.hurence.logisland.kakfa.registry.exceptions;

/**
 * Indicates an error while forwarding a write request to the master node in a schema
 * registry cluster
 */
public class RegistryRequestForwardingException extends RegistryException {

  public RegistryRequestForwardingException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryRequestForwardingException(String message) {
    super(message);
  }

  public RegistryRequestForwardingException(Throwable cause) {
    super(cause);
  }

  public RegistryRequestForwardingException() {
    super();
  }
}
