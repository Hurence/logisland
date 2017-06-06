/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.kafka.registry.exceptions;

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
