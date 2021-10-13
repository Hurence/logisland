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


import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;

import java.io.Serializable;
import java.util.*;

/**
 * An immutable object for holding information about a type of processor
 * property.
 *
 */
public final class PropertyDescriptor implements Comparable<PropertyDescriptor>, Serializable {

    public static final PropertyDescriptor NULL_DESCRIPTOR = new PropertyDescriptor.Builder().name("").build();

    /**
     * The proper name for the property. This is the primary mechanism of
     * comparing equality.
     */
    private final String name;

    /**
     * The name that should be displayed to user when referencing this property
     */
    private final String displayName;

    /**
     * And explanation of the meaning of the given property. This description is
     * meant to be displayed to a user or simply provide a mechanism of
     * documenting intent.
     */
    private final String description;
    /**
     * The default value for this property
     */
    private final String defaultValue;
    /**
     * The allowable getAllFields for this property. If empty then the allowable
     * getAllFields are not constrained
     */
    private transient final List<AllowableValue> allowableValues;
    /**
     * Determines whether the property is required for this processor
     */
    private final boolean required;
    /**
     * indicates that the value for this property should be considered sensitive
     * and protected whenever stored or represented
     */
    private final boolean sensitive;
    /**
     * indicates whether this property well-known for this processor or is
     * user-defined
     */
    private final boolean dynamic;
    /**
     * indicates whether or not this property supports the Attribute Expression
     * Language
     */
    private final boolean expressionLanguageSupported;


    private transient Class<? extends ControllerService> controllerServiceDefinition;


    /**
     * The validators that should be used whenever an attempt is made to set
     * this property value. Any allowable getAllFields specified will be checked first
     * and any validators specified will be ignored.
     */
    private transient final List<Validator> validators;

    protected PropertyDescriptor(final Builder builder) {
        this.displayName = builder.displayName == null ? builder.name : builder.displayName;
        this.name = builder.name;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.allowableValues = builder.allowableValues;
        this.required = builder.required;
        this.sensitive = builder.sensitive;
        this.dynamic = builder.dynamic;
        this.expressionLanguageSupported = builder.expressionLanguageSupported;
        this.controllerServiceDefinition = builder.controllerServiceDefinition;
        this.validators = new ArrayList<>(builder.validators);
    }

    @Override
    public int compareTo(final PropertyDescriptor o) {
        if (o == null) {
            return -1;
        }
        return getName().compareTo(o.getName());
    }

    /**
     * Validates the given input against this property descriptor's validator.
     * If this descriptor has a set of allowable getAllFields then the given value is
     * only checked against the allowable getAllFields.
     *
     * @param input the value to validate
     * @return the result of validating the input
     */
    public ValidationResult validate(final String input) {
        ValidationResult lastResult = Validator.VALID.validate(this.name, input);
        if (allowableValues != null && !allowableValues.isEmpty()) {
            final ConstrainedSetValidator csValidator = new ConstrainedSetValidator(allowableValues);
            final ValidationResult csResult = csValidator.validate(this.name, input);
            if (csResult.isValid()) {
                lastResult = csResult;
            } else {
                return csResult;
            }
        }

        for (final Validator validator : validators) {
            lastResult = validator.validate(this.name, input);
            if (!lastResult.isValid()) {
                break;
            }
        }
        return lastResult;
    }



    public Class<? extends ControllerService> getControllerServiceDefinition() {
        return controllerServiceDefinition;
    }

    public static final class Builder {

        private String displayName = null;
        private String name = null;
        private String description = "";
        private String defaultValue = null;
        private List<AllowableValue> allowableValues = null;
        private boolean required = false;
        private boolean sensitive = false;
        private boolean expressionLanguageSupported = false;
        private Class<? extends ControllerService> controllerServiceDefinition;
        private boolean dynamic = false;
        private List<Validator> validators = new ArrayList<>();

        public Builder fromPropertyDescriptor(final PropertyDescriptor specDescriptor) {
            this.name = specDescriptor.name;
            this.displayName = specDescriptor.displayName;
            this.description = specDescriptor.description;
            this.defaultValue = specDescriptor.defaultValue;
            this.allowableValues = specDescriptor.allowableValues == null ? null : new ArrayList<>(specDescriptor.allowableValues);
            this.required = specDescriptor.required;
            this.sensitive = specDescriptor.sensitive;
            this.dynamic = specDescriptor.dynamic;
            this.expressionLanguageSupported = specDescriptor.expressionLanguageSupported;
            this.controllerServiceDefinition = specDescriptor.getControllerServiceDefinition();
            this.validators = new ArrayList<>(specDescriptor.validators);
            return this;
        }

        /**
         * Sets a unique id for the property. This field is optional and if not
         * specified the PropertyDescriptor's name will be used as the
         * identifying attribute. However, by supplying an id, the
         * PropertyDescriptor's name can be changed without causing problems.
         * This is beneficial because it allows a User Interface to represent
         * the name differently.
         *
         * @param displayName of the property
         * @return the builder
         */
        public Builder displayName(final String displayName) {
            if (null != displayName) {
                this.displayName = displayName;
            }

            return this;
        }

        /**
         * Sets the property name.
         *
         * @param name of the property
         * @return the builder
         */
        public Builder name(final String name) {
            if (null != name) {
                this.name = name;
            }
            return this;
        }

        /**
         * Sets the value indicating whether or not this Property will support
         * the Attribute Expression Language.
         *
         * @param supported true if yes; false otherwise
         * @return the builder
         */
        public Builder expressionLanguageSupported(final boolean supported) {
            this.expressionLanguageSupported = supported;
            return this;
        }

        /**
         * @param description of the property
         * @return the builder
         */
        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }

        /**
         * Specifies the initial value and the default value that will be used
         * if the user does not specify a value. When {@link #build()} is
         * called, if Allowable Values have been set (see
         * {@link #allowableValues(AllowableValue...)}) and this value is not
         * one of those Allowable Values and Exception will be thrown. If the
         * Allowable Values have been set using the
         * {@link #allowableValues(AllowableValue...)} method, the default value
         * should be set to the "Value" of the {@link AllowableValue} object
         * (see {@link AllowableValue#getValue()}).
         *
         * @param value default value
         * @return the builder
         */
        public Builder defaultValue(final String value) {
            if (null != value) {
                this.defaultValue = value;
            }
            return this;
        }

        public Builder dynamic(final boolean dynamic) {
            this.dynamic = dynamic;
            return this;
        }

        /**
         * @param values contrained set of getAllFields
         * @return the builder
         */
        public Builder allowableValues(final Set<String> values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();

                for (final String value : values) {
                    this.allowableValues.add(new AllowableValue(value, value));
                }
            }
            return this;
        }

        public <E extends Enum<E>> Builder allowableValues(final E[] values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();
                for (final E value : values) {
                    allowableValues.add(new AllowableValue(value.name(), value.name()));
                }
            }
            return this;
        }

        /**
         * @param values constrained set of getAllFields
         * @return the builder
         */
        public Builder allowableValues(final String... values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();
                for (final String value : values) {
                    allowableValues.add(new AllowableValue(value, value));
                }
            }
            return this;
        }

        /**
         * Sets the Allowable Values for this Property
         *
         * @param values contrained set of getAllFields
         * @return the builder
         */
        public Builder allowableValues(final AllowableValue... values) {
            if (null != values) {
                this.allowableValues = Arrays.asList(values);
            }
            return this;
        }

        /**
         * @param required true if yes; false otherwise
         * @return the builder
         */
        public Builder required(final boolean required) {
            this.required = required;
            return this;
        }

        /**
         * @param sensitive true if sensitive; false otherwise
         * @return the builder
         */
        public Builder sensitive(final boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        /**
         * @param validator for the property
         * @return the builder
         */
        public Builder addValidator(final Validator validator) {
            if (validator != null) {
                validators.add(validator);
            }
            return this;
        }

        private boolean isValueAllowed(final String value) {
            if (allowableValues == null || value == null) {
                return true;
            }

            for (final AllowableValue allowableValue : allowableValues) {
                if (allowableValue.getValue().equals(value)) {
                    return true;
                }
            }

            return false;
        }


        /**
         * Specifies that this property provides the identifier of a Controller
         * Service that implements the given interface
         *
         * @param controllerServiceDefinition the interface that is implemented
         * by the Controller Service
         * @return the builder
         */
        public Builder identifiesControllerService(final Class<? extends ControllerService> controllerServiceDefinition) {
            if (controllerServiceDefinition != null) {
                this.controllerServiceDefinition = controllerServiceDefinition;
            }
            return this;
        }

        /**
         * @return a PropertyDescriptor as configured
         *
         * @throws IllegalStateException if allowable getAllFields are configured but
         * no default value is set, or the default value is not contained within
         * the allowable getAllFields.
         */
        public PropertyDescriptor build() {
            if (name == null) {
                throw new IllegalStateException("Must specify a name");
            }
            if (!isValueAllowed(defaultValue)) {
                throw new IllegalStateException("Default value [" + defaultValue + "] is not in the set of allowable getAllFields");
            }

            return new PropertyDescriptor(this);
        }
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public boolean isExpressionLanguageSupported() {
        return expressionLanguageSupported;
    }


    public List<Validator> getValidators() {
        return Collections.unmodifiableList(validators);
    }

    public List<AllowableValue> getAllowableValues() {
        return allowableValues == null ? null : Collections.unmodifiableList(allowableValues);
    }



    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof PropertyDescriptor)) {
            return false;
        }
        if (this == other) {
            return true;
        }

        PropertyDescriptor desc = (PropertyDescriptor) other;
        return this.name.equals(desc.name);
    }

    @Override
    public int hashCode() {
        return 287 + this.name.hashCode() * 47;
    }

    @Override
    public String toString() {
        String toReturn = "{\n" +
                "\tname : " + displayName + ",\n" +
                "\tdescription : " + description + ",\n" +
                "\tdefault : " + defaultValue + ",\n" +
                "\trequired : " + required;
        if (getAllowableValues() != null && !getAllowableValues().isEmpty()) {
            toReturn = toReturn + ",\n\tallowable values : " + getAllowableValues() + "\n}";
        } else {
            toReturn = toReturn + "\n}";
        }
        return toReturn;
    }

    private static final class ConstrainedSetValidator implements Validator {

        private static final String POSITIVE_EXPLANATION = "Given value found in allowed set";
        private static final String NEGATIVE_EXPLANATION = "Given value not found in allowed set '%1$s'";
        private static final String VALUE_DEMARCATOR = ", ";
        private final String validStrings;
        private final Collection<String> validValues;

        /**
         * Constructs a validator that will check if the given value is in the
         * given set.
         *
         * @param validValues getAllFields which are acceptible
         * @throws NullPointerException if the given validValues is null
         */
        private ConstrainedSetValidator(final Collection<AllowableValue> validValues) {
            String validVals = "";
            if (!validValues.isEmpty()) {
                final StringBuilder valuesBuilder = new StringBuilder();
                for (final AllowableValue value : validValues) {
                    valuesBuilder.append(value).append(VALUE_DEMARCATOR);
                }
                validVals = valuesBuilder.substring(0, valuesBuilder.length() - VALUE_DEMARCATOR.length());
            }
            validStrings = validVals;

            this.validValues = new ArrayList<>(validValues.size());
            for (final AllowableValue value : validValues) {
                this.validValues.add(value.getValue());
            }
        }

        @Override
        public ValidationResult validate(final String subject, final String input) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.input(input);
            builder.subject(subject);
            if (validValues.contains(input)) {
                builder.valid(true);
                builder.explanation(POSITIVE_EXPLANATION);
            } else {
                builder.valid(false);
                builder.explanation(String.format(NEGATIVE_EXPLANATION, validStrings));
            }
            return builder.build();
        }
    }
}
