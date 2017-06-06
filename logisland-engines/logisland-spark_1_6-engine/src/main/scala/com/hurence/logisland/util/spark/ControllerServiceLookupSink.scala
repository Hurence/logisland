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
package com.hurence.logisland.util.spark

import java.util
import java.util.Objects._

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.config.ControllerServiceConfiguration
import com.hurence.logisland.controller._


class ControllerServiceLookupSink(createControllerServiceLookup: () => ControllerServiceLookup) extends Serializable {

    lazy val controllerServiceLookup = createControllerServiceLookup()


    def getControllerServiceLookup(): ControllerServiceLookup = controllerServiceLookup

    def getControllerService(serviceIdentifier: String): ControllerService =
        controllerServiceLookup.getControllerService(serviceIdentifier)

    def addControllerService(serviceIdentifier: String, controllerService: ControllerService, properties: Map[PropertyDescriptor, String]) {
        requireNonNull(controllerService)
    }


}

object ControllerServiceLookupSink {
    def apply(configs: util.Collection[ControllerServiceConfiguration]): ControllerServiceLookupSink = {
        val f = () => {
           new StandardControllerServiceLookup(configs)


        }
        new ControllerServiceLookupSink(f)
    }
}
