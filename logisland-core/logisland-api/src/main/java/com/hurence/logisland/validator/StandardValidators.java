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
package com.hurence.logisland.validator;


import com.hurence.logisland.util.FormatUtils;

import java.io.File;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class StandardValidators {


    /**
     * Validator for java class descending for a base class.
     */
    private static final class TypeValidator implements Validator {

        private final Class<?> clz;


        public TypeValidator(Class<?> clz) {
            this.clz = clz;
        }

        @Override
        public ValidationResult validate(String subject, String input) {
            String reason = null;
            try {
                Class<?> c = Class.forName(input);
                if (!clz.isAssignableFrom(c)) {
                    reason = c.getCanonicalName() + " does not inherit from " + input;
                }
            } catch (ClassNotFoundException e) {
                reason = "Could not find class " + input;
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation(reason).valid(reason == null).build();
        }
    }


    public static final Validator TYPE_VALIDATOR(Class<?> clz) {
        return new TypeValidator(clz);
    }

    public static final Validator DOUBLE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {
            String reason = null;
            try {
                final double val = Double.parseDouble(value);

            } catch (final NumberFormatException e) {
                reason = "not a valid double";
            } catch (final NullPointerException e) {
                reason = "null is not a valid double";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator FLOAT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {
            String reason = null;
            try {
                final float val = Float.parseFloat(value);

            } catch (final NumberFormatException e) {
                reason = "not a valid double";
            } catch (final NullPointerException e) {
                reason = "null is not a valid double";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator POSITIVE_INTEGER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                if (value == null) {
                    reason = "null is not a valid integer";
                } else {
                    final int intVal = Integer.parseInt(value);
                    if (intVal <= 0) {
                        reason = "not a positive value";
                    }
                }
            } catch (final NumberFormatException e) {
                reason = "not a valid integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator POSITIVE_DOUBLE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {
            String reason = null;
            try {
                if (value == null) {
                    reason = "null is not a valid double";
                } else {
                    final double doubleVal = Double.parseDouble(value);
                    if (doubleVal <= 0) {
                        reason = "not a positive value";
                    }
                }
            } catch (final NumberFormatException e) {
                reason = "not a valid double";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator POSITIVE_LONG_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                if (value == null) {
                    reason = "null is not a valid integer";
                } else {
                    final long longVal = Long.parseLong(value);

                    if (longVal <= 0) {
                        reason = "not a positive value";
                    }
                }
            } catch (final NumberFormatException e) {
                reason = "not a valid 64-bit integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator PORT_VALIDATOR = createLongValidator(1, 65535, true);

    public static final Validator NON_EMPTY_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {
            return new ValidationResult.Builder().subject(subject).input(value).valid(value != null && !value.isEmpty()).explanation(subject + " cannot be empty").build();
        }
    };

    public static final Validator BOOLEAN_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {

            final boolean valid = "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
            final String explanation = valid ? null : "Value must be 'true' or 'false'";
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid).explanation(explanation).build();
        }
    };

    public static final Validator INTEGER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                if (value == null) {
                    reason = "null is not a valid integer";
                } else {
                    Integer.parseInt(value);
                }
            } catch (final NumberFormatException e) {
                reason = "not a valid integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator LONG_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                if (value == null) {
                    reason = "null is not a valid long";
                } else {
                    Long.parseLong(value);
                }
            } catch (final NumberFormatException e) {
                reason = "not a valid Long";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator NON_NEGATIVE_INTEGER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                if (value == null) {
                    reason = "null is not a valid integer";
                } else {
                    final int intVal = Integer.parseInt(value);

                    if (intVal < 0) {
                        reason = "value is negative";
                    }
                }
            } catch (final NumberFormatException e) {
                reason = "value is not a valid integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };


    public static final Validator SEMICOLON_SEPARATED_LIST_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                value.split(";");
            } catch (final Exception e) {
                reason = "not a SEMICOLON separated list";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator COMMA_SEPARATED_LIST_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                value.split(",");
            } catch (final Exception e) {
                reason = "not a comma separated list";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator CHARACTER_SET_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                if (!Charset.isSupported(value)) {
                    reason = "Character Set is not supported by this JVM.";
                }
            } catch (final UnsupportedCharsetException uce) {
                reason = "Character Set is not supported by this JVM.";
            } catch (final IllegalArgumentException iae) {
                reason = "Character Set value cannot be null.";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator HASH_ALGORITHM_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                final MessageDigest digest = MessageDigest.getInstance(value);
            } catch (final NoSuchAlgorithmException nsae) {
                reason = "'" + value + "' is not a supported algorithme by this JVM.";
            } catch (final NullPointerException npe) {
                reason = "null is not a supported algorithm";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator LANGUAGE_TAG_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = String.format("'%s' is not a supported language tag", value);
            for (String tag : Locale.getISOLanguages()) {
                if (tag.equals(value)) reason = null;
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };


    /**
     * URL Validator that does not allow the Expression Language to be used
     */
    public static final Validator URL_VALIDATOR = createURLValidator();

    public static final Validator URI_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input) {


            try {
                new URI(input);
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Valid URI").valid(true).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Not a valid URI").valid(false).build();
            }
        }
    };


    /**
     * Time Validators
     */
    public static final Validator TIMEZONE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {
            String reason = null;
            if (!Arrays.asList(TimeZone.getAvailableIDs()).contains(value)) reason = "not a valid timezone";
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    /**
     * {@link Validator} that ensures that value has 1+ non-whitespace
     * characters
     */
    public static final Validator NON_BLANK_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value) {
            return new ValidationResult.Builder().subject(subject).input(value)
                    .valid(value != null && !value.trim().isEmpty())
                    .explanation(subject
                            + " must contain at least one character that is not white space").build();
        }
    };

    public static final Validator TIME_PERIOD_VALIDATOR = new Validator() {
        private final Pattern TIME_DURATION_PATTERN = Pattern.compile(FormatUtils.TIME_DURATION_REGEX);

        @Override
        public ValidationResult validate(final String subject, final String input) {
          /*  if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }*/

            if (input == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("Time Period cannot be null").build();
            }
            if (TIME_DURATION_PATTERN.matcher(input.toLowerCase()).matches()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } else {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Must be of format <duration> <TimeUnit> where <duration> is a "
                                + "non-negative integer and TimeUnit is a supported Time Unit, such "
                                + "as: nanos, millis, secs, mins, hrs, days")
                        .build();
            }
        }
    };

    //
    //
    // FACTORY METHODS FOR VALIDATORS
    //
    //
    public static Validator createDirectoryExistsValidator(final boolean allowExpressionLanguage, final boolean createDirectoryIfMissing) {
        return new DirectoryExistsValidator(allowExpressionLanguage, createDirectoryIfMissing);
    }

    private static Validator createURLValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input) {

                try {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Valid URL").valid(true).build();
                } catch (final Exception e) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Not a valid URL").valid(false).build();
                }
            }
        };
    }


    public static Validator createRegexMatchingValidator(final Pattern pattern) {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input) {


                final boolean matches = pattern.matcher(input).matches();
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(matches)
                        .explanation(matches ? null : "Value does not match regular expression: " + pattern.pattern())
                        .build();
            }
        };
    }


    public static Validator createLongValidator(final long minimum, final long maximum, final boolean inclusive) {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input) {


                String reason = null;
                try {
                    final long longVal = Long.parseLong(input);
                    if (longVal < minimum || (!inclusive && longVal == minimum) | longVal > maximum || (!inclusive && longVal == maximum)) {
                        reason = "Value must be between " + minimum + " and " + maximum + " (" + (inclusive ? "inclusive" : "exclusive") + ")";
                    }
                } catch (final NumberFormatException e) {
                    reason = "not a valid integer";
                }

                return new ValidationResult.Builder().subject(subject).input(input).explanation(reason).valid(reason == null).build();
            }

        };
    }


    public static class StringLengthValidator implements Validator {
        private final int minimum;
        private final int maximum;

        public StringLengthValidator(int minimum, int maximum) {
            this.minimum = minimum;
            this.maximum = maximum;
        }

        @Override
        public ValidationResult validate(final String subject, final String value) {
            if (value.length() < minimum || value.length() > maximum) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .valid(false)
                        .input(value)
                        .explanation(String.format("String length invalid [min: %d, max: %d]", minimum, maximum))
                        .build();
            } else {
                return new ValidationResult.Builder()
                        .valid(true)
                        .input(value)
                        .subject(subject)
                        .build();
            }
        }
    }


    public static final Validator FILE_EXISTS_VALIDATOR = new FileExistsValidator(true);

    public static class FileExistsValidator implements Validator {

        private final boolean allowEL;

        public FileExistsValidator(final boolean allowExpressionLanguage) {
            this.allowEL = allowExpressionLanguage;
        }

        @Override
        public ValidationResult validate(final String subject, final String value) {


            final String substituted = value;


            final File file = new File(substituted);
            final boolean valid = file.exists();
            final String explanation = valid ? null : "File " + file + " does not exist";
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid).explanation(explanation).build();
        }
    }


    public static class DirectoryExistsValidator implements Validator {

        private final boolean allowEL;
        private final boolean create;

        public DirectoryExistsValidator(final boolean allowExpressionLanguage, final boolean create) {
            this.allowEL = allowExpressionLanguage;
            this.create = create;
        }

        @Override
        public ValidationResult validate(final String subject, final String value) {


            String reason = null;
            try {
                final File file = new File(value);
                if (!file.exists()) {
                    if (!create) {
                        reason = "Directory does not exist";
                    } else if (!file.mkdirs()) {
                        reason = "Directory does not exist and could not be created";
                    }
                } else if (!file.isDirectory()) {
                    reason = "Path does not point to a directory";
                }
            } catch (final Exception e) {
                reason = "Value is not a valid directory name";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    }

    /**
     * This class validates a value against an enumeration by checking that the raw value can be transformed to an
     * item of the enumeration class.
     *
     * @param <E> the class of the enumeration to validate.
     */
    public static class EnumValidator<E extends Enum<E>> implements Validator {
        private final Class<E> enumClass;

        public EnumValidator(final Class<E> enumClass) {
            Objects.requireNonNull(enumClass);
            this.enumClass = enumClass;
        }

        @Override
        public ValidationResult validate(final String subject, final String value) {
            ValidationResult.Builder builder = new ValidationResult.Builder().subject(subject).input(value);
            try {
                Enum.valueOf(this.enumClass, value);
                builder.valid(true);
            } catch (final Exception e) {
                builder.explanation(e.getLocalizedMessage()).valid(false);
            }

            return builder.build();
        }
    }
}
