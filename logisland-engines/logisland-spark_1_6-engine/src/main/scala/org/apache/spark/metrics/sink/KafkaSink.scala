package org.apache.spark.metrics.sink

import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties}

import com.codahale.metrics.MetricRegistry
import com.hurence.logisland.util.kafka.KafkaReporter
import org.slf4j.{Logger, LoggerFactory}

/**
  * A Kafka metric sink for Apache Spark


Configure your spark metrics.properties file

Edit /path/to/spark/conf/metrics.properites to look like this:

master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource

  *.sink.kafka.class=org.apache.spark.metrics.sink.KafkaSink
  *.sink.kafka.broker=127.0.0.1:9092
  *.sink.kafka.topic=test
  *.sink.kafka.period=10
  *.sink.kafka.unit=seconds

# histquantiles and timerquantiles have following defaults:
#*.sink.kafka.histquantiles=0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0
#*.sink.kafka.timerquantiles=0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0

# These carry configure settings to the KafkaProducer
# *.sink.kafka.prodconf_xxx, where xxx can be anything, just has to
# be unique per setting:
  *.sink.kafka.prodconf_a=retries=0
  *.sink.kafka.prodconf_b=acks=all
  *.sink.kafka.prodconf_c=request.timeout.ms=5
  *.sink.kafka.prodconf_d=max.block.ms=5


  */
class KafkaSink(val properties: Properties, val registry: MetricRegistry,
                securityMgr: SecurityManager) extends org.apache.spark.metrics.sink.Sink {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    private def popt(prop: String): Option[String] =
        Option(properties.getProperty(prop))

    // These are non-negotiable
    val broker = popt("broker").get
    val topic = popt("topic").get

    lazy val reporter = new KafkaReporter(registry, broker, topic, properties)

    def start(): Unit = {
        logger.info(s"Starting Kafka metric reporter at $broker, topic $topic")
        val period = popt("period").getOrElse("10").toLong
        val tstr = popt("unit").getOrElse("seconds").toUpperCase(Locale.ROOT)
        val tunit = TimeUnit.valueOf(tstr)
        reporter.start(period, tunit)
    }

    def stop(): Unit = {
        logger.info(s"Stopping Kafka metric reporter at $broker, topic $topic")
        reporter.stop()
    }

    def report(): Unit = {
        logger.info(s"Reporting metrics to Kafka reporter at $broker, topic $topic")
        reporter.report()
    }
}