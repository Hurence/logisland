package com.hurence.logisland.hbase.validate;


import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;

public class ConfigFilesValidator implements Validator {

    @Override
    public ValidationResult validate(final String subject, final String value) {
        final String[] filenames = value.split(",");
        for (final String filename : filenames) {
            final ValidationResult result = StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject, filename.trim());
            if (!result.isValid()) {
                return result;
            }
        }

        return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
    }
}
