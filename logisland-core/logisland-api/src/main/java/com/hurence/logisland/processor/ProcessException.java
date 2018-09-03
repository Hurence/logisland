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
package com.hurence.logisland.processor;


import com.hurence.logisland.record.StandardRecord;

import java.util.Collection;
import java.util.Collections;

/**
 * Exception indicating that a failure or early completion condition was
 * detected in an event processing.
 *
 * @author Tom Bailet
 *
 */
public class ProcessException extends java.lang.RuntimeException {

    private final Collection<StandardRecord> errorRecords;

    public Collection<StandardRecord> getErrorRecords() {
        return errorRecords;
    }

    /**
     * Create a new {@link ProcessException} based on a collection of records
     *
     * @param errorRecords the error records for this exception
     */
    public ProcessException(Collection<StandardRecord> errorRecords) {
        this.errorRecords = errorRecords;
    }

    /**
     * Create a new {@link ProcessException} based on a message string
     *
     * @param message the error message for this exception
     */
    public ProcessException(String message) {
        super(message);
        this.errorRecords = Collections.emptyList();
    }
}
