package com.hurence.logisland.util.spark

import java.util.Objects._

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.controller.{ControllerService, ControllerServiceLookup, StandardControllerServiceLookup}


class ControllerServiceLookupSink(createControllerServiceLookup: () => ControllerServiceLookup) extends Serializable {

    lazy val controllerServiceLookup = createControllerServiceLookup()


    def getControllerService(serviceIdentifier: String): ControllerService =
        controllerServiceLookup.getControllerService(serviceIdentifier)

    def addControllerService(serviceIdentifier: String, controllerService: ControllerService, properties: Map[PropertyDescriptor, String]) {
        requireNonNull(controllerService)
    }


}

object ControllerServiceLookupSink {
    def apply(): ControllerServiceLookupSink = {
        val f = () => {
           new StandardControllerServiceLookup()


        }
        new ControllerServiceLookupSink(f)
    }
}
