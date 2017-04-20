package com.hurence.logisland.util.spark

import java.util
import java.util.Objects._

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.config.ControllerServiceConfiguration
import com.hurence.logisland.controller._


class ControllerServiceLookupSink(createControllerServiceLookup: () => ControllerServiceLookup) extends Serializable {

    lazy val controllerServiceLookup = createControllerServiceLookup()


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
