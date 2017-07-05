package com.hurence.logisland.processor.binetflow;

import com.hurence.botsearch.analytics.NetworkTrace;
import com.hurence.logisland.botsearch.HttpFlow;
import com.hurence.logisland.botsearch.Trace;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.ml.model.KMeansModelWrapper;
import com.hurence.logisland.ml.model.Model;
import com.hurence.logisland.ml.scaler.Scaler;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.ml.ModelClientService;
import com.hurence.logisland.service.ml.ScalerModelClientService;

import java.util.*;

public class TagBinetflow extends AbstractProcessor {

    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), TagBinetflow.class);

    public static final PropertyDescriptor MODEL_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("ml.model.client.service")
            .description("The instance of the Controller Service to use for accessing Machine Learning model.")
            .required(true)
            .identifiesControllerService(ModelClientService.class)
            .build();

    public static final PropertyDescriptor SCALER_MODEL_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("ml.scaler.client.service")
            .description("The instance of the Controller Service to use for accessing Machine Learning scaler model.")
            .required(true)
            .identifiesControllerService(ModelClientService.class)
            .build();

    protected ModelClientService modelClientService;
    protected ScalerModelClientService scalerModelClientService;

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) {
        modelClientService = context.getPropertyValue(MODEL_CLIENT_SERVICE).asControllerService(ModelClientService.class);
        if(modelClientService == null) {
            logger.error("Model client service is not initialized!");
        }

        scalerModelClientService = context.getPropertyValue(SCALER_MODEL_CLIENT_SERVICE).asControllerService(ScalerModelClientService.class);
        if(scalerModelClientService == null) {
            logger.error("Scaler model client service is not initialized!");
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(MODEL_CLIENT_SERVICE);
        props.add(SCALER_MODEL_CLIENT_SERVICE);

        return Collections.unmodifiableList(props);
    }

    /**
     * process events
     *
     * @param context
     * @param records
     * @return
     */
    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> records) {

        List<Record> outputRecords = new ArrayList<>();

        if (records.size() != 0) {

            Map<String,List<HttpFlow>> map =new HashMap<String, List<HttpFlow>>();

            records.forEach(flowRecord -> {

                try{
                    HttpFlow flow = new HttpFlow();

                    String src_ip = flowRecord.getField("src_ip").asString();
                    String dest_ip = flowRecord.getField("dest_ip").asString();


                    flow.setDate(new java.util.Date(flowRecord.getField("record_time").asLong()));
                    flow.setipSource(src_ip);
                    flow.setIpTarget(dest_ip);
                    flow.setRequestSize(flowRecord.getField("bytes_in").asLong());
                    flow.setResponseSize(flowRecord.getField("bytes_out").asLong());

                    String key = src_ip + "_" + dest_ip;

                    List<HttpFlow> value = map.get(key);

                    if (value != null) {
                        value.add(flow);
                    } else {
                        ArrayList<HttpFlow> flows = new ArrayList<>();
                        flows.add(flow);

                        map.put(key, flows);
                    }

                } catch (Exception e)
                {}
            });

            List<NetworkTrace> networkTraces = new ArrayList<>();

            for (Map.Entry<String, List<HttpFlow>> entry : map.entrySet())
            {
                List<HttpFlow> flows = entry.getValue();
                if(flows.size() > 5)
                {
                    Trace trace = new Trace();

                    flows.forEach(trace::add);

                    // compute trace frequencies and stats
                    trace.compute();

                    NetworkTrace networkTrace = new NetworkTrace(
                            flows.get(0).getipSource(),
                            flows.get(0).getIpTarget(),
                            (float) trace.getAvgUploadedBytes(),
                            (float) trace.getAvgDownloadedBytes(),
                            (float) trace.getAvgTimeBetweenTwoFLows(),
                            (float) trace.getMostSignificantFrequency(),
                            trace.getFlows().size(),
                            "",
                            0);

                    networkTraces.add(networkTrace);
                }
            }

            // load ScalerModel parameters :

            Model<double[],Integer> kmeansModel = modelClientService.getModel();

            List<double[]> clusterCenters = null;
            try {
                clusterCenters = ((KMeansModelWrapper) kmeansModel).getClusterCenters();
            } catch (Exception e) {

            }

            Scaler<double[],double[]> standardScalerModel = scalerModelClientService.getScalerModel();

            double[] networkTraceElements = new double[4];

            Iterator<NetworkTrace> tracesIterator = networkTraces.iterator();
            while (tracesIterator.hasNext()) {

                NetworkTrace networkTrace = tracesIterator.next();
                networkTraceElements[0] = networkTrace.avgUploadedBytes();
                networkTraceElements[1] = networkTrace.avgDownloadedBytes();
                networkTraceElements[2] = networkTrace.avgTimeBetweenTwoFLows();
                networkTraceElements[3] = networkTrace.mostSignificantFrequency();

                StandardRecord outputRecord = new StandardRecord();
                outputRecord.setField(new Field(FieldDictionary.RECORD_TYPE, FieldType.STRING, "tagged_trace"));
                outputRecord.setField(new Field("avgUploadedBytes", FieldType.DOUBLE, networkTraceElements[0]));
                outputRecord.setField(new Field("avgDownloadedBytes", FieldType.DOUBLE, networkTraceElements[1]));
                outputRecord.setField(new Field("avgTimeBetweenTwoFLows", FieldType.DOUBLE, networkTraceElements[2]));
                outputRecord.setField(new Field("mostSignificantFrequency", FieldType.DOUBLE, networkTraceElements[3]));

                try {
                    double[] scaledNetworkTraceElements = standardScalerModel.transform(networkTraceElements);
                    int clusterID = kmeansModel.predict(scaledNetworkTraceElements);

                    outputRecord.setField(new Field("scaled_avgUploadedBytes", FieldType.DOUBLE, scaledNetworkTraceElements[0]));
                    outputRecord.setField(new Field("scaled_avgDownloadedBytes", FieldType.DOUBLE, scaledNetworkTraceElements[1]));
                    outputRecord.setField(new Field("scaled_avgTimeBetweenTwoFLows", FieldType.DOUBLE, scaledNetworkTraceElements[2]));
                    outputRecord.setField(new Field("scaled_mostSignificantFrequency", FieldType.DOUBLE, scaledNetworkTraceElements[3]));

                    outputRecord.setField(new Field("clusterID", FieldType.INT, clusterID));

                    double[] centerCoordinates = clusterCenters.get(clusterID);

                    outputRecord.setField(new Field("center_avgUploadedBytes", FieldType.DOUBLE, centerCoordinates[0]));
                    outputRecord.setField(new Field("center_avgDownloadedBytes", FieldType.DOUBLE, centerCoordinates[1]));
                    outputRecord.setField(new Field("center_avgTimeBetweenTwoFLows", FieldType.DOUBLE, centerCoordinates[2]));
                    outputRecord.setField(new Field("center_mostSignificantFrequency", FieldType.DOUBLE, centerCoordinates[3]));

                    outputRecords.add(outputRecord);
                } catch (Exception e) {

                }
            }
        }

        return outputRecords;
    }

}
