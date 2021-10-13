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
package com.hurence.logisland.util.runner;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockRecord extends StandardRecord {

	private static final long serialVersionUID = 7750544989597574120L;
    private static final float floatDelta = 0.000001f;
    private static final double doubleDelta = 0.000001d;
    private static Logger logger = LoggerFactory.getLogger(MockRecord.class);
	
    public MockRecord(Record toClone) {
        super(toClone);
    }

    public MockRecord assertFieldExists(final String fieldName) {
        Assert.assertTrue("Field " + fieldName + " does not exist", hasField(fieldName));
        return this;
    }

    public MockRecord assertFieldNotExists(final String fieldName) {
        Assert.assertFalse("Attribute " + fieldName + " should not exists but got " + getField(fieldName),
                hasField(fieldName));
        return this;
    }

    public MockRecord assertFieldTypeEquals(final String fieldName, final FieldType type) {
        Assert.assertEquals(type, getField(fieldName).getType());
        return this;
    }


    public MockRecord assertFieldEquals(final String fieldName, final String expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asString());
        return this;
	}
    
    public MockRecord assertFieldEquals(final String fieldName, final boolean expectedValue) {
        Assert.assertEquals(new Boolean(expectedValue), getField(fieldName).asBoolean());
        return this;
    }

    public MockRecord assertFieldEquals(final String fieldName, final int expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asInteger().intValue());
        return this;
    }

    public MockRecord assertFieldEquals(final String fieldName, final long expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asLong().longValue());
        return this;
    }

    public MockRecord assertFieldEquals(final String fieldName, final float expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asFloat(), floatDelta);
        return this;
    }

    public MockRecord assertFieldEquals(final String fieldName, final double expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asDouble(), doubleDelta);
        return this;
    }

    public MockRecord assertFieldEquals(final String fieldName, final byte[] expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).getRawValue());
        return this;
    }

    public MockRecord assertFieldEquals(final String fieldName, final Record expectedValue) {
        Record record = getField(fieldName).asRecord();
        MockRecord mockRecord = new MockRecord(record);
        mockRecord.assertContentEqualsExceptTechnicalFields(expectedValue);
        return this;
    }

    public <K,V> MockRecord assertFieldEquals(final String fieldName, final Map<K, V> expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).getRawValue());
        return this;
    }

    public <E> MockRecord assertFieldEquals(final String fieldName, final List<E> expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).getRawValue());
        return this;
    }

    public MockRecord assertNullField(final String fieldName) {
        Assert.assertNull(getField(fieldName).getRawValue());
        return this;
    }

    public MockRecord assertNotNullField(final String fieldName) {
        Assert.assertNotNull(getField(fieldName).getRawValue());
        return this;
    }

    public MockRecord assertFieldNotEquals(final String fieldName, final String expectedValue) {
        Assert.assertNotSame(expectedValue, getField(fieldName).asString());
        return this;
    }

    public MockRecord assertStringFieldStartWith(final String fieldName, final String expectedValue) {
        Assert.assertNotNull(getField(fieldName).asString());
        String msg = "'" + getField(fieldName).asString()  +"' does not start by '" + expectedValue +"'";
        Assert.assertTrue(msg, getField(fieldName).asString().startsWith(expectedValue));
        return this;
    }

    public MockRecord assertStringFieldEndWith(final String fieldName, final String expectedValue) {
        Assert.assertNotNull(getField(fieldName).asString());
        String msg = "'" + getField(fieldName).asString()  +"' does not end by '" + expectedValue +"'";
        Assert.assertTrue(msg, getField(fieldName).asString().endsWith(expectedValue));
        return this;
    }

    public MockRecord assertRecordSizeEquals(final int size) {
        Assert.assertEquals(size, size());
        return this;
    }

    public MockRecord assertAllFieldsAsserted(List<String> ignoreFields) {
		// get missing asserted fields
		Set<String> fields = new HashSet<>(getAllFieldNames());
		fields.removeAll(ignoreFields);
		//fields.removeAll(assertedFields);
		Assert.assertEquals("List of not asserted fields: " + fields, 0, fields.size());
        return this;
    }
    
    /**
     * Asserts that the content of this Record is the same as the content one
     *
     * @param record to compare content against
     */
    public MockRecord assertContentEquals(final Record record) {
        Assert.assertEquals(this, record);
        return this;
    }

    public MockRecord assertContentEqualsExceptTechnicalFields(final Record expected) {
        Assert.assertEquals(expected.size(), this.size());
        expected.getAllFields().forEach(field -> {
            if (this.isInternField(field)) return;
            logger.debug("testing field {}", field);
            Assert.assertEquals(field, this.getField(field.getName()));
        });
        this.getAllFields().forEach(field -> {
            if (this.isInternField(field)) return;
            logger.debug("testing field {}", field);
            Assert.assertEquals(field, expected.getField(field.getName()));
        });
        return this;
    }

}
