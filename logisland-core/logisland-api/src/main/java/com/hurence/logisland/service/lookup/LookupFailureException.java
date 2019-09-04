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
package com.hurence.logisland.service.lookup;
/**
 * @see <a href="https://github.com/apache/nifi/blob/master/nifi-nar-bundles/nifi-standard-services/nifi-lookup-service-api/src/main/java/org/apache/nifi/lookup/LookupFailureException.java">
 *      copied from LookupFailureException nifi
 *     </a>
 */
public class LookupFailureException extends Exception {

    public LookupFailureException() {
        super();
    }

    public LookupFailureException(final String message) {
        super(message);
    }

    public LookupFailureException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public LookupFailureException(final Throwable cause) {
        super(cause);
    }
}
