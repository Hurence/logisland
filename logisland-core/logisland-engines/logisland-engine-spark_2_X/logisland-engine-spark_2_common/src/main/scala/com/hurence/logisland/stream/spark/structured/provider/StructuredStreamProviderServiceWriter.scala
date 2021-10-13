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
/**
  * Copyright (C) 2016 Hurence (support@hurence.com)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.hurence.logisland.stream.spark.structured.provider

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.record._
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming._


trait StructuredStreamProviderServiceWriter extends ControllerService {

  /**
    * create a streaming DataFrame that represents data received
    *
    * @return DataFrame currently loaded
    */
  def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]): DataStreamWriter[_]
}

object StructuredStreamProviderServiceWriter {
  val APPEND_MODE = "append"
  val COMPLETE_MODE = "complete"
  val UPDATE_MODE = "update"

  val OUTPUT_MODE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("output.mode")
    .description("output mode for the streaming sink. By default will use output mode by default of the sink (see sink doc)")
    .defaultValue(UPDATE_MODE)
    .allowableValues(APPEND_MODE, COMPLETE_MODE, UPDATE_MODE)
    .required(false)
    .build
}
