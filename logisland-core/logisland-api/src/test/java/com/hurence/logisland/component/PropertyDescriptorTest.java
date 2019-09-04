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
