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

import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.lookup.LookupFailureException;
import com.hurence.logisland.service.rest.MockRestClientService;
import com.hurence.logisland.service.rest.RestClientService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hurence.logisland.rest.processor.lookup.AbstractCallRequest.*;
import static com.hurence.logisland.rest.processor.lookup.AbstractHttpProcessor.HTTP_CLIENT_SERVICE;

public class AsyncCallRequestBulkPostJsonTest  {

    private static String SERVICE_ID = "bulkService";

    Processor newProc() {
        return new AsyncCallRequestBulkPostJson();
    }


    @Test
    public void basic_test() throws InitializationException {

        final TestRunner runner = getRunnerInitialized();
        final RestClientService service = (RestClientService) runner.getControllerService(SERVICE_ID);
        runner.setProperty(CONFLICT_RESOLUTION_POLICY, OVERWRITE_EXISTING.getValue());
        //runner.setProperty(KEEP_ONLY_BODY_RESPONSE, "true");

        runner.setProperty(REQUEST_BODY, "body");
        runner.setProperty(REQUEST_METHOD, "post");
        runner.setProperty(REQUEST_MIME_TYPE, "application/json");
        runner.setProperty(TAG_KEY_VALUE, "tagName=ha_video_engagement_brightcove,ha_video_engagement_brightcove_bis;itemType=pres");

        runner.assertValid();

        //test queries
        StandardRecord record1 = new StandardRecord();
        record1.setField("ItemId", FieldType.LONG, 218594);
        record1.setField("tagName", FieldType.STRING, "ha_video_engagement_brightcove");
        record1.setField("SecondsViewed", FieldType.LONG, 45);
        record1.setField("Userid", FieldType.LONG, 103951);
        record1.setField("VideoPercentViewed", FieldType.INT, 15);

        StandardRecord record2 = new StandardRecord();
        record2.setField("ItemId", FieldType.LONG, 215863);
        record2.setField("SecondsViewed", FieldType.LONG, 15);
        record2.setField("Userid", FieldType.LONG, 103951);
        record2.setField("VideoPercentViewed", FieldType.INT, 15);

        runner.enqueue(record1, record2);
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(2);
        MockRecord out = runner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(6);

        MockRecord coordinates = new MockRecord(out.getField("response").asRecord());
        coordinates.assertRecordSizeEquals(8);

    }

        private TestRunner getRunnerInitialized() throws InitializationException {
        return getRunnerInitialized(null);
    }

    private TestRunner getRunnerInitialized(String fakeBody) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(newProc());
        MockRestClientService service = new MockRestClientService(fakeBody);
        //enable service
        runner.addControllerService("restLookupService", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        //config proc
        runner.setProperty(HTTP_CLIENT_SERVICE, "restLookupService");
        runner.setProperty(FIELD_HTTP_RESPONSE, "response");
        //runner.setProperty(CONFLICT_RESOLUTION_POLICY, KEEP_OLD_FIELD.getValue());
        runner.assertValid();
        return runner;
    }
}
