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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hurence.logisland.record.FieldType;
import org.junit.Assert;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

public class MockRecord extends StandardRecord {

	private static final long serialVersionUID = 7750544989597574120L;

	//private final Set<String> assertedFields;
	
    public MockRecord(Record toClone) {
        super(toClone);
        
      //  this.assertedFields = new HashSet<>();
    }

    public void assertFieldExists(final String fieldName) {
        Assert.assertTrue("Field " + fieldName + " does not exist", hasField(fieldName));
		//assertedFields.add(fieldName);
    }

    public void assertFieldNotExists(final String fieldName) {
        Assert.assertFalse("Attribute " + fieldName + " not exists with value " + getField(fieldName),
                hasField(fieldName));
		//assertedFields.add(fieldName);
    }

    public void assertFieldTypeEquals(final String fieldName, final FieldType type) {
        Assert.assertEquals(type, getField(fieldName).getType());
        //assertedFields.add(fieldName);
    }


    public void assertFieldEquals(final String fieldName, final String expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asString());
		//assertedFields.add(fieldName);
	}
    
    public void assertFieldEquals(final String fieldName, final boolean expectedValue) {
        Assert.assertEquals(new Boolean(expectedValue), getField(fieldName).asBoolean());
      //  assertedFields.add(fieldName);
    }

    public void assertFieldEquals(final String fieldName, final int expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asInteger().intValue());
		//assertedFields.add(fieldName);
    }

    public void assertFieldEquals(final String fieldName, final long expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asLong().longValue());
		//assertedFields.add(fieldName);
    }

    public void assertFieldEquals(final String fieldName, final float expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asFloat(), 0.000001);
		//assertedFields.add(fieldName);
    }

    public void assertFieldEquals(final String fieldName, final double expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).asDouble(), 0.000001);
		//assertedFields.add(fieldName);
    }

    public void assertFieldEquals(final String fieldName, final byte[] expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).getRawValue());
       // assertedFields.add(fieldName);
    }

    public <K,V> void assertFieldEquals(final String fieldName, final Map<K, V> expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).getRawValue());
    }

    public <E> void assertFieldEquals(final String fieldName, final List<E> expectedValue) {
        Assert.assertEquals(expectedValue, getField(fieldName).getRawValue());
    }

    public void assertNullField(final String fieldName) {
        Assert.assertNull(getField(fieldName).getRawValue());
    }

    public void assertNotNullField(final String fieldName) {
        Assert.assertNotNull(getField(fieldName).getRawValue());
    }

    public void assertFieldNotEquals(final String fieldName, final String expectedValue) {
        Assert.assertNotSame(expectedValue, getField(fieldName).asString());
		//assertedFields.add(fieldName);
    }

    public void assertStringFieldStartWith(final String fieldName, final String expectedValue) {
        Assert.assertNotNull(getField(fieldName).asString());
        String msg = "'" + getField(fieldName).asString()  +"' does not start by '" + expectedValue +"'";
        Assert.assertTrue(msg, getField(fieldName).asString().startsWith(expectedValue));
    }

    public void assertStringFieldEndWith(final String fieldName, final String expectedValue) {
        Assert.assertNotNull(getField(fieldName).asString());
        String msg = "'" + getField(fieldName).asString()  +"' does not end by '" + expectedValue +"'";
        Assert.assertTrue(msg, getField(fieldName).asString().endsWith(expectedValue));
    }


    public void assertRecordSizeEquals(final int size) {
        Assert.assertEquals(size, size());
    }

    public void assertAllFieldsAsserted(List<String> ignoreFields) {
		// get missing asserted fields
		Set<String> fields = new HashSet<>(getAllFieldNames());
		fields.removeAll(ignoreFields);
		//fields.removeAll(assertedFields);
		Assert.assertEquals("List of not asserted fields: " + fields, 0, fields.size());
    }
    
    /**
     * Asserts that the content of this Record is the same as the content of
     * the given file
     *
     * @param record to compare content against
     */
    public void assertContentEquals(final Record record) {
        Assert.assertEquals(this, record);
    }

}
