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
package com.hurence.logisland.timeseries;


/**
 * The minimal required schema of Chronix.
 *
 * @author f.lautenschlager
 */
public final class Schema {

    /**
     * The id of an document
     */
    public static final String ID = "id";
    /**
     * The the data field
     */
    public static final String DATA = "data";
    /**
     * The start as long milliseconds since 1970
     */
    public static final String START = "start";

    /**
     * The end as long milliseconds since 1970
     */
    public static final String END = "end";

    /**
     * The type of the serialized data field
     */
    public static final String TYPE = "type";

    /**
     * Each time series has a name
     */
    public static final String NAME = "name";

    /**
     * Private constructor
     */
    private Schema() {

    }

    /**
     * Checks if a given field is user-defined or not.
     *
     * @param field - the field name
     * @return true if the field is not one of the four required fields, otherwise false.
     */
    @SuppressWarnings("all")
    public static boolean isUserDefined(String field) {
        return !(ID.equals(field) || DATA.equals(field) ||
                START.equals(field) || END.equals(field) ||
                TYPE.equals(field) || NAME.equals(field));
    }

}
