package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;

import java.util.*;

public class Utils {

    /**
     * Return {@code true} if the provided field has a non-null value;  {@code false} otherwise.
     *
     * @param field the field to check.
     *
     * @return {@code true} if the provided field has a non-null value;  {@code false} otherwise.
     */
    public static boolean isFieldAssigned(final Field field)
    {
        return field!=null && field.getRawValue()!=null;
    }

    /**
     * Returns the conversion of a record to a map where all {@code null} values were removed.
     *
     * @param record the record to convert.
     *
     * @return the conversion of a record to a map where all {@code null} values were removed.
     */
    public static Map<String, Object> toMap(final Record record)
    {
        return toMap(record, false);
    }

    /**
     *
     * Returns the conversion of a record to a map where all {@code null} values were removed.
     *
     * @param record the record to convert.
     * @param excludeDictionnaryFields if {@code true} special dictionnary fields are ignored; included otherwise.
     *
     * @return the conversion of a record to a map where all {@code null} values were removed.
     */
    public static Map<String, Object> toMap(final Record record,
                                            final boolean excludeDictionnaryFields)
    {
        try
        {
            final Map<String, Object> result = new HashMap<>();

            record.getFieldsEntrySet()
                    .stream()
                    .filter(entry -> {
                        if (!excludeDictionnaryFields) return true;
                        if (FieldDictionary.contains(entry.getKey())) return false;
                        return true;
                    })
                    .forEach(entry -> {
                        Object value = entry.getValue().getRawValue();
                        if (value != null) {
                            switch(entry.getValue().getType())
                            {
                                case RECORD:
                                    value = toMap((Record)value, true);
                                    break;
                                case ARRAY:
                                    Collection collection;
                                    if ( value instanceof Collection )
                                    {
                                        collection = (Collection) value;
                                    }
                                    else
                                    {
                                        collection = Arrays.asList(value);
                                    }
                                    final List list = new ArrayList(collection.size());
                                    for(final Object item: collection)
                                    {
                                        if ( item instanceof Record )
                                        {
                                            list.add(toMap((Record)item, true));
                                        }
                                        else
                                        {
                                            list.add(item);
                                        }
                                    }
                                    value = list;
                                    break;
                                default:
                            }
                            result.put(entry.getKey(), value);
                        }
                    });
            return result;
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }


}
