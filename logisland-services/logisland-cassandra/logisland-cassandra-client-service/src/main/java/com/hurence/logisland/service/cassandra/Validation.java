/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.logisland.service.cassandra;

import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;

import java.util.Arrays;
import java.util.List;

public class Validation {

    /**
     * This validator ensures the cassandra hosts property is a valid list of hosts in a comma separated values format
     */
    public static final Validator HOSTS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input) {
            final List<String> hostsList = Arrays.asList(input.split(" ,"));
            if (hostsList.size() == 0)
            {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                    "Must have at least one cassandra host").valid(false).build();
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation(
                    "Valid hosts definition").valid(true).build();
        }
    };
}
