package com.hurence.logisland.engine.spark


import java.util

import com.hurence.logisland.component.ComponentType
import com.hurence.logisland.config.{ComponentFactory, EngineConfiguration}
import com.hurence.logisland.engine.{EngineContext, StandardEngineContext}
import org.junit.Assert._
import org.scalatest.{FlatSpec, Matchers}


class SparkStreamprocessingEngineTest extends FlatSpec with Matchers {


    /**
      * create
      *
      *
      */
    def createEngineConfiguration(): EngineConfiguration = {
        val conf = new util.HashMap[String, String]
        conf.put(SparkStreamProcessingEngine.SPARK_APP_NAME.getName, "testApp")
        conf.put(SparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION.getName, "2000")
        conf.put(SparkStreamProcessingEngine.SPARK_MASTER.getName, "local[4]")
        conf.put(SparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT.getName, "5000")

        val engineConfiguration = new EngineConfiguration()
        engineConfiguration.setComponent(classOf[SparkStreamProcessingEngine].getName)
        engineConfiguration.setType(ComponentType.ENGINE.toString)
        engineConfiguration.setConfiguration(conf)
        engineConfiguration
    }


    "An engine context" should "be created" in {
        val engineConfiguration = createEngineConfiguration()

        val instance = ComponentFactory.getEngineInstance(engineConfiguration)
        instance.isPresent should be(true)
        instance.get().isValid should be(true)
        val engine = instance.get.getEngine
        val engineContext = new StandardEngineContext(instance.get)

        System.setProperty("hadoop.home.dir", "/")
        engine.start(engineContext)

    }


    it should "handle dates as well" in {
    }

}
