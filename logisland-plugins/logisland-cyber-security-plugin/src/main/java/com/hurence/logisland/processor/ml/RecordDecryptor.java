/**
 * Copyright (C) 2017 Hurence
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.logisland.processor.ml;

import com.hurence.logisland.record.Record;
import org.nd4j.linalg.api.ndarray.INDArray;

/**
 * Created by pducjac on 07/06/17.
 */
public interface RecordDecryptor {

    void decrypt(Record record);
    void decrypt(Record record, Boolean label);

    INDArray getFeaturedData();
    INDArray getLabels();
    boolean isLabeledRecords();
    long getCount();
}
