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
package com.hurence.logisland.service.influxdb;

import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;

import java.util.concurrent.TimeUnit;

public class Validation {

    /**
     * This validator ensures the influxdb tags property is valid
     */
    public static final Validator TAGS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input) {
            String trimmedValue = input.trim();
            if (trimmedValue.length() == 0)
            {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "Empty tags").valid(false).build();
            }
            String[] measurementEntries = trimmedValue.split(";");
            if (measurementEntries.length == 0)
            {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "No measurement entry found in tags").valid(false).build();
            }
            // Go through measurement entries
            for (String measurementEntry : measurementEntries) {
                String trimmedMeasurementEntry = measurementEntry.trim();
                String[] measurementAndTags = trimmedMeasurementEntry.split(":");
                if (measurementAndTags.length != 2)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform: " + measurementEntry).valid(false).build();
                }
                String measurement = measurementAndTags[0];
                String trimmedMeasurement = measurement.trim();
                if (trimmedMeasurement.length() == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform: " + measurementEntry).valid(false).build();
                }
                String tags = measurementAndTags[1];
                String trimmedTags = tags.trim();
                if (trimmedTags.length() == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (tags part): " + measurementEntry).valid(false).build();
                }
                String[] tagValues = trimmedTags.split(",");
                if (tagValues.length == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (tags part): " + measurementEntry).valid(false).build();
                }
                for (String tag : tagValues)
                {
                    String trimmedTag = tag.trim();
                    if (trimmedTag.length() == 0)
                    {
                        return new ValidationResult.Builder().subject(subject).input(input).explanation(
                                "Measurement entry not conform (tags part): " + measurementEntry).valid(false).build();
                    }
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation(
                    "Valid tags definition").valid(true).build();
        }
    };

    /**
     * This validator ensures the influxdb fields property is valid
     */
    public static final Validator FIELDS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input) {
            String trimmedValue = input.trim();
            if (trimmedValue.length() == 0)
            {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "Empty fields").valid(false).build();
            }
            String[] measurementEntries = trimmedValue.split(";");
            if (measurementEntries.length == 0)
            {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "No measurement entry found in fields").valid(false).build();
            }
            // Go through measurement entries
            for (String measurementEntry : measurementEntries) {
                String trimmedMeasurementEntry = measurementEntry.trim();
                String[] measurementAndFields = trimmedMeasurementEntry.split(":");
                if (measurementAndFields.length != 2)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform: " + measurementEntry).valid(false).build();
                }
                String measurement = measurementAndFields[0];
                String trimmedMeasurement = measurement.trim();
                if (trimmedMeasurement.length() == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform: " + measurementEntry).valid(false).build();
                }
                String fields = measurementAndFields[1];
                String trimmedFields = fields.trim();
                if (trimmedFields.length() == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (fields part): " + measurementEntry).valid(false).build();
                }
                String[] fieldValues = trimmedFields.split(",");
                if (fieldValues.length == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (fields part): " + measurementEntry).valid(false).build();
                }
                for (String field : fieldValues)
                {
                    String trimmedField = field.trim();
                    if (trimmedField.length() == 0)
                    {
                        return new ValidationResult.Builder().subject(subject).input(input).explanation(
                                "Measurement entry not conform (fields part): " + measurementEntry).valid(false).build();
                    }
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation(
                    "Valid fields definition").valid(true).build();
        }
    };

    /**
     * This validator ensures the influxdb timefield property is valid
     */
    public static final Validator TIME_FIELD_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input) {
            String trimmedValue = input.trim();
            if (trimmedValue.length() == 0)
            {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "Empty timefield").valid(false).build();
            }
            String[] measurementEntries = trimmedValue.split(";");
            if (measurementEntries.length == 0)
            {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "No measurement entry found in timefield").valid(false).build();
            }
            // Go through measurement entries
            for (String measurementEntry : measurementEntries) {
                String trimmedMeasurementEntry = measurementEntry.trim();
                String[] measurementAndField = trimmedMeasurementEntry.split(":");
                if (measurementAndField.length != 2)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform: " + measurementEntry).valid(false).build();
                }
                String measurement = measurementAndField[0];
                String trimmedMeasurement = measurement.trim();
                if (trimmedMeasurement.length() == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform: " + measurementEntry).valid(false).build();
                }
                String field = measurementAndField[1];
                String trimmedField = field.trim();
                if (trimmedField.length() == 0)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (timefield part): " + measurementEntry).valid(false).build();
                }
                String[] fieldAndFormat = trimmedField.split(",");
                if (fieldAndFormat.length != 2)
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (timefield part): " + measurementEntry).valid(false).build();
                }
                String timeField = fieldAndFormat[0].trim();
                if (timeField.isEmpty())
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (missing time field): " + measurementEntry).valid(false).build();
                }
                String format = fieldAndFormat[1].trim();
                if (format.isEmpty())
                {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (missing format): " + measurementEntry).valid(false).build();
                }
                // Check format
                try {
                    TimeUnit.valueOf(format);
                } catch(IllegalArgumentException e)
                {

                    return new ValidationResult.Builder().subject(subject).input(input).explanation(
                            "Measurement entry not conform (unsupported format " + format + "): " + measurementEntry).valid(false).build();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation(
                    "Valid fields definition").valid(true).build();
        }
    };
}
