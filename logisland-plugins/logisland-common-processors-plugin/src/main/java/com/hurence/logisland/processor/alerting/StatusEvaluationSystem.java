package com.hurence.logisland.processor.alerting;

import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by a.ait-bachir on 11/07/2018.
 */
public class StatusEvaluationSystem extends AbstractNashornSandboxProcessor{
    @Override
    protected void setupDynamicProperties(ProcessContext context) {

    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        // check if we need initialization
        if (datastoreClientService == null) {
            init(context);
        }
        List<Record> outputRecords = new ArrayList<>(records);


        return outputRecords;
    }
}
