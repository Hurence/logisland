package com.hurence.logisland.kakfa.registry.exceptions;

/**
 * Indicates that the node that is asked to serve the request is not the current master and
 * is not aware of the master node to forward the request to
 */
public class UnknownMasterException extends RegistryException {

  public UnknownMasterException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownMasterException(String message) {
    super(message);
  }

  public UnknownMasterException(Throwable cause) {
    super(cause);
  }

  public UnknownMasterException() {
    super();
  }
}
