package com.caseystella.analytics.extractor;

import com.caseystella.analytics.DataPoint;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPointExtractor implements Extractor {
    DataPointExtractorConfig config = null;
    public DataPointExtractor() {

    }

    public DataPointExtractor(DataPointExtractorConfig config) {
        this.config =  config;
    }

    public DataPointExtractor withConfig(DataPointExtractorConfig config) {
        this.config = config;
        return this;
    }

    @Override
    public Iterable<DataPoint> extract(byte[] key, byte[] value, boolean failOnMalformed) {

        Map<String, Object> unionMap = new HashMap<>();
        {
            Map<String, Object> keyMap = config.getKeyConverter().convert(key, config.getKeyConverterConfig());
            Map<String, Object> valueMap = config.getValueConverter().convert(value, config.getValueConverterConfig());
            if (keyMap != null) {
                unionMap.putAll(keyMap);
            }
            if (valueMap != null) {
                unionMap.putAll(valueMap);
            }
        }
        List<DataPoint> ret = new ArrayList<>();
        if(unionMap.size() > 0) {
            for (DataPointExtractorConfig.Measurement measurement : config.getMeasurements()) {
                try {
                    DataPoint dp = new DataPoint();
                    if (measurement.getSource() != null) {
                        dp.setSource(measurement.getSource());
                    } else {
                        List<String> sources = new ArrayList<>();
                        for (String sourceField : measurement.getSourceFields()) {
                            sources.add(unionMap.get(sourceField).toString());
                        }
                        dp.setSource(Joiner.on(".").join(sources));
                    }
                    Object tsObj = unionMap.get(measurement.getTimestampField());
                    if (tsObj == null) {
                        throw new RuntimeException("Unable to find " + measurement.getTimestampField() + " in " + unionMap);
                    }
                    dp.setTimestamp(measurement.getTimestampConverter().convert(tsObj, measurement.getTimestampConverterConfig()));

                    Object measurementObj = unionMap.get(measurement.getMeasurementField());
                    if (measurementObj == null) {
                        throw new RuntimeException("Unable to find " + measurement.getMeasurementField() + " in " + unionMap);
                    }
                    dp.setValue(measurement.getMeasurementConverter().convert(measurementObj, measurement.getMeasurementConverterConfig()));

                    Map<String, String> metadata = new HashMap<>();
                    if (measurement.getMetadataFields() != null && measurement.getMetadataFields().size() > 0) {
                        for (String field : measurement.getMetadataFields()) {
                            metadata.put( field
                                        , unionMap.get(field)
                                                  .toString()
                                                  .replace(' ', '_')
                                                  .replace("&", "and")
                                                  .replace(",", "")
                                                  .replace("(", "")
                                                  .replace(")", "")
                                                  .replace("[", "")
                                                  .replace("]", "")
                                                  .replace("{", "")
                                                  .replace("}", "")
                                                  .replace("?", "")
                                                  .replace("\'", "")
                                                  .replace("\"", "")
                                                  .replace("/", "_")
                                        );
                            if(metadata.get(field).length() == 0) {
                                metadata.remove(field);
                            }
                        }
                    } else {
                        for (Map.Entry<String, Object> kv : unionMap.entrySet()) {
                            if (!kv.getKey().equals(measurement.getMeasurementField()) && !kv.getKey().equals(measurement.getTimestampField())) {
                                metadata.put( kv.getKey()
                                            , kv.getValue()
                                                .toString()
                                                .replace(' ', '_')
                                                .replace("&", "and")
                                                .replace(",", "")
                                                .replace("(", "")
                                                .replace(")", "")
                                                .replace("[", "")
                                                .replace("]", "")
                                                .replace("{", "")
                                                .replace("}", "")
                                                .replace("?", "")
                                                .replace("\'", "")
                                                .replace("\"", "")
                                                .replace("/", "_")
                                            );
                                if(metadata.get(kv.getKey()).length() == 0) {
                                    metadata.remove(kv.getKey());
                                }
                            }
                        }
                    }
                    dp.setMetadata(metadata);
                    ret.add(dp);
                }
                catch(RuntimeException re) {
                    if(failOnMalformed) {
                        throw re;
                    }
                }
            }
        }
        return ret;
    }

}
