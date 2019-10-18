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
package com.hurence.logisland.component;

import com.hurence.logisland.timeseries.sax.SaxConverter;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;
import net.seninp.jmotif.sax.SAXException;

public class SaxEncodingValidators {

    public static final Validator ALPHABET_SIZE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            return null;
        }

        @Override
        public ValidationResult validate(final String subject, final String value) {
            String reason = null;
            int val = 0;
            try {
                val = Integer.parseInt(value);
            } catch (final NumberFormatException e) {
                reason = "not a valid double";
            } catch (final NullPointerException e) {
                reason = "null is not a valid double";
            }
            if (reason == null) {
                try {
                    SaxConverter.normalAlphabet.getCuts(val);
                }   catch (final SAXException e) {
                    reason = "'" +val +"' is not a valid alphabet size";
                }
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };
}
