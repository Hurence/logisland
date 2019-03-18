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

public enum ProcessError {

    BAD_RECORD,
    RECORD_CONVERSION_ERROR,
    NUMBER_PARSING_ERROR,
    DATE_PARSING_ERROR,
    REGEX_MATCHING_ERROR,
    DUPLICATE_ID_ERROR,
    INDEXATION_ERROR,
    CONFIG_SETTING_ERROR,
    STRING_FORMAT_ERROR,
    INVALID_FILE_FORMAT_ERROR,
    NOT_IMPLEMENTED_ERROR,
    RUNTIME_ERROR,
    UNKNOWN_ERROR;

    private String name;

    ProcessError() {
        this.name = this.name().toLowerCase();
    }

    public String getName() {
        return name;
    }
    public String toString() {
        return name().toLowerCase();
    }
}
