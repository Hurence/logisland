package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * This class is a basic WebItem that wraps an inner record.
 */
public class RecordItem {
    /**
     * The record actually computed by this processor and returned at the end of the processing.
     */
    protected final Record record;


    /**
     * Creates a new instance of this class with the associated parameter.
     *
     * @param record the wrapped record.
     */
    public RecordItem(final Record record) {
        this.record = record;
    }

    /**
     * Returns the value of the specified field name. {@code null} is returned if the field does not exists or
     * if the value of the field is {@code null}.
     *
     * @param fieldname the name of the field to retrieve.
     * @return the value of the specified field name.
     */
    public Object getValue(final String fieldname) {
        final Field field = this.record.getField(fieldname);
        return field == null ? null : field.asString();
    }

    public String getStringValue(final String fieldname) {
        return (String) this.getValue(fieldname);
    }

    /**
     * Returns a ZonedDateTime corresponding to the provided epoch parameter with the system default timezone.
     *
     * @param epoch the time to convert.
     * @return a ZonedDateTime corresponding to the provided epoch parameter with the system default timezone.
     */
    ZonedDateTime fromEpoch(final long epoch) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(epoch)), ZoneId.systemDefault());
    }

    /**
     * Returns a concatenation of the form ${utmSource}:${utmMedium}:${utmCampaign}:${utmTerm}:${utmContent} with
     * the provided parameters.
     *
     * @param utmSource   the utm source
     * @param utmMedium   the medium source
     * @param utmCampaign the campaign source
     * @param utmTerm     the utm term
     * @param utmContent  the utm content
     * @return a concatenation of the form ${utmSource}:${utmMedium}:${utmCampaign}:${utmTerm}:${utmContent}
     * with the provided parameters.
     */
    protected String concatFieldsOfTraffic(final String utmSource,
                                         final String utmMedium,
                                         final String utmCampaign,
                                         final String utmTerm,
                                         final String utmContent) {
        return new StringBuilder().append(utmSource == null ? "" : utmSource).append(':')
                .append(utmMedium == null ? "" : utmMedium).append(':')
                .append(utmCampaign == null ? "" : utmCampaign).append(':')
                .append(utmTerm == null ? "" : utmTerm).append(':')
                .append(utmContent == null ? "" : utmContent).toString();
    }
}
