package com.hurence.logisland.processor.binetflow;

import com.hurence.botsearch.analytics.NetworkTrace;
import com.hurence.logisland.botsearch.HttpFlow;
import com.hurence.logisland.botsearch.Trace;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.ml.model.Model;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.ml.ModelClientService;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;

public class TagBinetflow extends AbstractProcessor {

    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), TagBinetflow.class);

    public static final PropertyDescriptor MODEL_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("ml.model.client.service")
            .description("The instance of the Controller Service to use for accessing Machine Learning model.")
            .required(true)
            .identifiesControllerService(ModelClientService.class)
            .build();

    protected ModelClientService modelClientService;

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
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(MODEL_CLIENT_SERVICE);

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

        if (records.size() != 0) {

            //Map<String,List<List<Long>>> map =new HashMap<String, List<List<Long>>>();
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

                    String key = src_ip + dest_ip;

                    //List<List<Long>> value = map.get(key);
                    List<HttpFlow> value = map.get(key);

                    if (value != null) {
                        //value.get(0).add(bytes_in);
                        //value.get(1).add(bytes_out);
                        value.add(flow);
                    } else {
                        //List<Long> bytes_in_0 = new ArrayList<>();
                        //List<Long> bytes_out_1 = new ArrayList<>();
                        //List<List<Long>> matrice = new ArrayList<>();
                        //matrice.add(0, bytes_in_0);
                        //matrice.add(1, bytes_out_1);
                        //map.put(key, matrice);

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
                System.out.println(entry.getKey() + "/" + entry.getValue());

                List<HttpFlow> flows = entry.getValue();
                if(flows.size() > 5)
                {
                    Trace trace = new Trace();

                    flows.forEach(trace::add);

                    // compute trace frequencies and stats
                    trace.compute();

                    NetworkTrace networkTrace = new NetworkTrace(
                            trace.getIpSource(),
                            trace.getIpTarget(),
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

            /*
            double meanUploadedBytes = 1.0;
            double meanDownloadedBytes = 1.0;
            double meanTimeBetweenTwoFLows = 1.0;
            double meanMostSignificantFrequency = 1.0;

            double stdUploadedBytes = 1.0;
            double stdDownloadedBytes = 1.0;
            double stdTimeBetweenTwoFLows = 1.0;
            double stdMostSignificantFrequency = 1.0;
            */

            String scalerModelFile = "" ;
            StandardScalerModel scalerModel = null;

            Model<double[],Integer> kmeansModel = modelClientService.getModel();

            try {
                FileInputStream in = new FileInputStream(scalerModelFile);
                ObjectInputStream ois = new ObjectInputStream(in);
                scalerModel = (StandardScalerModel) ois.readObject();
                ois.close();
            } catch (Exception e) {
                System.out.println("Problem serializing: " + e);
            }

            double[] traceValues = new double[4];

            Iterator<NetworkTrace> tracesIterator = networkTraces.iterator();
            while (tracesIterator.hasNext()) {

                NetworkTrace trace = tracesIterator.next();
                traceValues[0] = trace.avgUploadedBytes();
                traceValues[1] = trace.avgDownloadedBytes();
                traceValues[2] = trace.avgTimeBetweenTwoFLows();
                traceValues[3] = trace.mostSignificantFrequency();

                double[] scaledTraceValues = scalerModel.transform(Vectors.dense(traceValues)).toArray();

                try {
                    int clusterID = kmeansModel.predict(scaledTraceValues);
                } catch (Exception e) {

                }
            }
        }

        return records;
    }


}
