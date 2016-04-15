package com.caseystella.analytics.extractor;

import com.caseystella.analytics.converters.*;
import com.caseystella.analytics.converters.primitive.PrimitiveConverter;
import com.caseystella.analytics.util.JSONUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataPointExtractorConfig implements Serializable {

    public static class Measurement implements Serializable {
        private String source;
        private List<String> sourceFields;
        private String timestampField;
        private TimestampConverter timestampConverter = new PrimitiveConverter.PrimitiveTimestampConverter();
        private Map<String, Object> timestampConverterConfig;
        private String measurementField;
        private MeasurementConverter measurementConverter = new PrimitiveConverter.PrimitiveMeasurementConverter();
        private Map<String, Object> measurementConverterConfig;
        private List<String> metadataFields = new ArrayList<>();

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public List<String> getSourceFields() {
            return sourceFields;
        }

        public void setSourceFields(List<String> sourceField) {
            this.sourceFields = sourceField;
        }

        public String getTimestampField() {
            return timestampField;
        }

        public void setTimestampField(String timestampField) {
            this.timestampField = timestampField;
        }

        public TimestampConverter getTimestampConverter() {
            return timestampConverter;
        }

        public void setTimestampConverter(String timestampConverter) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
            this.timestampConverter = Converters.getTimestampConverter(timestampConverter);
        }

        public Map<String, Object> getTimestampConverterConfig() {
            return timestampConverterConfig;
        }

        public void setTimestampConverterConfig(Map<String, Object> timestampConverterConfig) {
            this.timestampConverterConfig = timestampConverterConfig;
        }

        public String getMeasurementField() {
            return measurementField;
        }

        public void setMeasurementField(String measurementField) {
            this.measurementField = measurementField;
        }

        public MeasurementConverter getMeasurementConverter() {
            return measurementConverter;
        }

        public void setMeasurementConverter(String measurementConverter) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
            this.measurementConverter = Converters.getMeasurementConverter(measurementConverter);
        }

        public Map<String, Object> getMeasurementConverterConfig() {
            return measurementConverterConfig;
        }

        public void setMeasurementConverterConfig(Map<String, Object> measurementConverterConfig) {
            this.measurementConverterConfig = measurementConverterConfig;
        }

        public List<String> getMetadataFields() {
            return metadataFields;
        }

        public void setMetadataFields(List<String> metadataFields) {
            this.metadataFields = metadataFields;
        }
    }

    private MappingConverter keyConverter = new NOOP();
    private Map<String, Object> keyConverterConfig;
    private MappingConverter valueConverter;
    private Map<String, Object> valueConverterConfig;
    private List<Measurement> measurements;

    public MappingConverter getKeyConverter() {
        return keyConverter;
    }

    public void setKeyConverter(String keyConverter) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        this.keyConverter = Converters.getMappingConverter(keyConverter);
    }

    public Map<String, Object> getKeyConverterConfig() {
        return keyConverterConfig;
    }

    public void setKeyConverterConfig(Map<String, Object> keyConverterConfig) {
        this.keyConverterConfig = keyConverterConfig;
    }

    public MappingConverter getValueConverter() {
        return valueConverter;
    }

    public void setValueConverter(String valueConverter) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        this.valueConverter = Converters.getMappingConverter(valueConverter);
    }

    public Map<String, Object> getValueConverterConfig() {
        return valueConverterConfig;
    }

    public void setValueConverterConfig(Map<String, Object> valueConverterConfig) {
        this.valueConverterConfig = valueConverterConfig;
    }

    public List<Measurement> getMeasurements() {
        return measurements;
    }

    public void setMeasurements(List<Measurement> measurements) {
        this.measurements = measurements;
    }

    public static DataPointExtractorConfig load(String configStr) throws IOException {
        return JSONUtil.INSTANCE.load(configStr, DataPointExtractorConfig.class);
    }
}
