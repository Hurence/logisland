package com.hurence.logisland.component;

import org.junit.Assert;
import org.junit.Test;

public class PropertyDescriptorTest {
    @Test
    public void validate01_Init_MVEL_and_Simple_EL() {


        final AllowableValue OVERWRITE_EXISTING =
                new AllowableValue("overwrite_existing,", "overwrite existing field", "if field already exist");

        final AllowableValue KEEP_OLD_FIELD =
                new AllowableValue(",keep_only_old_field", "keep only old field value", "keep only old field");

        final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
                .name("conflict.resolution.policy")
                .description("What to do when a field with the same name already exists ?")
                .required(false)
                .defaultValue(KEEP_OLD_FIELD.getValue())
                .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
                .build();

        Assert.assertTrue(CONFLICT_RESOLUTION_POLICY.validate(OVERWRITE_EXISTING.getValue()).isValid());
        Assert.assertTrue(CONFLICT_RESOLUTION_POLICY.validate(KEEP_OLD_FIELD.getValue()).isValid());
        Assert.assertFalse(CONFLICT_RESOLUTION_POLICY.validate(KEEP_OLD_FIELD.getValue() + "a").isValid());
    }
}
