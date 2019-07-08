package com.hurence.logisland.component;

import com.hurence.logisland.timeseries.converter.sax.SaxConverter;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;
import net.seninp.jmotif.sax.SAXException;

public class SaxEncodingValidators {

    public static final Validator ALPHABET_SIZE_VALIDATOR = new Validator() {
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
