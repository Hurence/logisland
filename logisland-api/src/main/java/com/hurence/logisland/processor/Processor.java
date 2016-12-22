/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.processor;

import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.util.Collection;


public interface Processor extends ConfigurableComponent {

    /**
     * Setup statefull parameters
     *
     * @param context
     */
    void init(final ProcessContext context);

    /**
     * Process the incoming collection of records to
     * generate a new collection of records
     *
     * @param context the current process context
     * @param records the collection of records to handle
     * @return a collection of computed records
     * @throws ProcessException if something went wrong
     */
    Collection<Record> process(ProcessContext context, Collection<Record> records);

}
