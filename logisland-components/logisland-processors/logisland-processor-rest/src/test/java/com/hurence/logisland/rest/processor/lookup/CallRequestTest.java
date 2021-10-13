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
package com.hurence.logisland.rest.processor.lookup;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.lookup.LookupFailureException;
import com.hurence.logisland.service.rest.RestClientService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import org.junit.Test;

import java.io.IOException;

import static com.hurence.logisland.rest.processor.lookup.AbstractCallRequest.REQUEST_BODY;


/**
 * note that the two test below cannot be factorized with async call request test
 * because of runner.setProperty(TAG_KEY_VALUE, ... ) statement that cannot be computed here
 */
public class CallRequestTest extends AbstractCallRequestTest {


    @Override
    Processor newProc() {
        return new CallRequest();
    }

    @Test
    public void basic_test() throws InitializationException {
        final TestRunner runner = getRunnerInitialized();

        //test queries
        StandardRecord record1 = new StandardRecord();
        record1.setField("employeeId", FieldType.INT, 1);
        StandardRecord record2 = new StandardRecord();
        record2.setField("employeeId", FieldType.INT, 2);
        runner.enqueue(record1, record2);
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(2);

        MockRecord out = runner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(2);
        out.assertFieldEquals("employeeId", 1);
        out.assertFieldTypeEquals("employeeId", FieldType.INT);
        out.assertFieldTypeEquals("response", FieldType.RECORD);
        MockRecord coordinnates = new MockRecord(out.getField("response").asRecord());
        coordinnates.assertRecordSizeEquals(1);
        coordinnates.assertFieldEquals("employeeId", 1);
        coordinnates.assertFieldTypeEquals("employeeId", FieldType.INT);
        MockRecord out2 = runner.getOutputRecords().get(1);
        out2.assertFieldEquals("employeeId", 2);
        out2.assertFieldTypeEquals("employeeId", FieldType.INT);
        out2.assertFieldTypeEquals("response", FieldType.RECORD);
        MockRecord coordinnates2 = new MockRecord(out2.getField("response").asRecord());
        coordinnates2.assertRecordSizeEquals(1);
        coordinnates2.assertFieldEquals("employeeId", 2);
        coordinnates2.assertFieldTypeEquals("employeeId", FieldType.INT);

    }

    @Test
    public void adding_body_coordinates_expression_language_3() throws InitializationException, IOException, LookupFailureException {
        final TestRunner runner = getRunnerInitialized();
        final RestClientService service = (RestClientService) runner.getControllerService(SERVICE_ID);
        runner.setProperty(REQUEST_BODY, "${http_query}");
        runner.assertValid();

        //test queries
        StandardRecord record = new StandardRecord();
        runner.enqueue(new StandardRecord(record));
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(1);

        MockRecord out = runner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(1);
        out.assertFieldTypeEquals("response", FieldType.RECORD);
        MockRecord coordinates = new MockRecord(out.getField("response").asRecord());
        coordinates.assertRecordSizeEquals(0);
    }

}
