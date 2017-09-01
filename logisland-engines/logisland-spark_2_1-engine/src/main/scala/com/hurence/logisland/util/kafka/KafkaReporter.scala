package com.hurence.logisland.util.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.{Failure, Success, Try}

class KafkaReporter(
                       registry: MetricRegistry,
                       kafkaEndpoint: String,
                       kafkaTopic: String,
                       properties: Properties)
    extends ScheduledReporter(
        registry,
        "kafka-reporter",
        MetricFilter.ALL,
        TimeUnit.SECONDS,
        TimeUnit.MILLISECONDS) {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    var producer: Option[KafkaProducer[String, String]] = None

    // Any user properties set in the metrics config file
    // prodconf_foo=this.setting.key=value
    // prodconf_bar=this.setting.key2=value2
    private def setUserProperties(props: Properties) {
        for {
            entry <- properties.entrySet().asScala
            if entry.getKey().asInstanceOf[String].startsWith("prodconf_")
        } {
            val kv = entry.getValue().asInstanceOf[String].split('=')
            if (kv.length != 2) {
                logger.error(s"Ignoring bad prodconf_* setting: ${entry.getValue()}")
            } else {
                props.put(kv(0), kv(1))
            }
        }
    }

    override def start(period: Long, unit: TimeUnit): Unit = {
        super.start(period, unit)
        val status = for {
            kp <- Try {
                logger.info(s"Opening Kafka endpoint $kafkaEndpoint")
                val props = new Properties()

                // Set these, but may be overridden in setUserProperties
                props.put("client.id", s"KafkaReporter-$kafkaEndpoint-$kafkaTopic".replace(':', '-'))

                // load any KafkaProducer conf settings passed in from metrics config
                setUserProperties(props)

                // Anything here takes precedence over user settings
                props.put("bootstrap.servers", kafkaEndpoint)
                props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer")
                props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer")

                logger.info(s"Kafka producer properties:\n$props")

                new KafkaProducer[String, String](props)
            }
        } yield {
            kp
        }
        status match {
            case Success(kp) => {
                logger.info(s"Kafka producer connected to $kafkaEndpoint")
                producer = Some(kp)
            }
            case Failure(err) => {
                logger.error(s"Failure opening Kafka endpoint $kafkaEndpoint:\n$err")
            }
        }
    }

    override def stop(): Unit = {
        logger.info(s"Stopping Kafka reporter at $kafkaEndpoint")
        super.stop()
    }

    def report(
                  gauges: java.util.SortedMap[String, Gauge[_]],
                  counters: java.util.SortedMap[String, Counter],
                  histograms: java.util.SortedMap[String, Histogram],
                  meters: java.util.SortedMap[String, Meter],
                  timers: java.util.SortedMap[String, Timer]): Unit = {

        if (producer.isEmpty) {
            logger.error(s"Failed Kafka client for $kafkaEndpoint: metric output ignored")
        } else {
            // dump metric output to the kafka topic
            producer match {
                case Some(prod) =>
                    for {entry <- gauges.entrySet().asScala} {
                        gaugeJSON(entry.getValue).foreach { jv => prod.send(metricRec(entry.getKey, jv)) }
                    }
                    for {entry <- counters.entrySet().asScala} {
                        counterJSON(entry.getValue).foreach { jv => prod.send(metricRec(entry.getKey, jv)) }
                    }
                    for {entry <- histograms.entrySet().asScala} {
                        histJSON(entry.getValue).foreach { jv => prod.send(metricRec(entry.getKey, jv)) }
                    }
                    for {entry <- meters.entrySet().asScala} {
                        meterJSON(entry.getValue).foreach { jv => prod.send(metricRec(entry.getKey, jv)) }
                    }
                    for {entry <- timers.entrySet().asScala} {
                        timerJSON(entry.getValue).foreach { jv => prod.send(metricRec(entry.getKey, jv)) }
                    }
            }
        }
    }

    private def metricRec(key: String, value: String) =
        new ProducerRecord[String, String](kafkaTopic, key, value)

    private def gaugeJSON(gauge: Gauge[_]): Option[String] = {
        val tpe = "type" -> "gauge"
        gauge.getValue match {
            case v: Int => Some(compact(render(tpe ~ ("value" -> v))))
            case v: Long => Some(compact(render(tpe ~ ("value" -> v))))
            case v: Float => Some(compact(render(tpe ~ ("value" -> v))))
            case v: Double => Some(compact(render(tpe ~ ("value" -> v))))
            case v => {
                logger.error(s"Ignoring unexpected Gauge value: $v")
                None
            }
        }
    }

    private def counterJSON(counter: Counter): Option[String] = {
        val tpe = "type" -> "counter"
        Some(compact(render(tpe ~ ("value" -> counter.getCount)))
    }

    private def histJSON(hist: Histogram): Option[String] = {
        for {
            hsub <- samplingAST(hist, "histquantiles")
            nsub <- Some("n" -> hist.getCount)
        } yield {
            compact(render(("type" -> "histogram") ~ ("value" -> (nsub ~ hsub))))
        }
    }

    private def meterJSON(meter: Meter): Option[String] = {
        for {

            msub <- meteredAST(meter)
            nsub <- Some("n" -> meter.getCount)
        } yield {
            compact(render(("type" -> "meter") ~ ("value" -> (nsub ~ msub))))
        }
    }

    private def timerJSON(timer: Timer): Option[String] = {
        for {
            hsub <- samplingAST(timer, "timerquantiles")
            msub <- meteredAST(timer)
            nsub <- Some(("n" -> timer.getCount()))
        } yield {
            compact(render(("type" -> "timer") ~ ("value" -> (nsub ~ hsub ~ msub))))
        }
    }

    private def samplingAST(hist: Sampling, qsetting: String): Option[JObject] = {
        val snapshot = hist.getSnapshot
        Try {
            val hqs = Option(properties.getProperty(qsetting)).getOrElse(
                "0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0")
            val q = hqs.split(",").map(_.toDouble).toVector
            val x = q.map { z => snapshot.getValue(z) }
            (q, x)
        } match {
            case Failure(_) => {
                val hqs = properties.getProperty(qsetting)
                logger.error(s"Bad quantile setting: $hqs\nIgnoring histogram metric output")
                None
            }
            case Success((q, x)) => {
                val hsub =
                    ("q" -> q) ~
                        ("x" -> x) ~
                        ("min" -> snapshot.getMin) ~
                        ("max" -> snapshot.getMax) ~
                        ("mean" -> snapshot.getMean) ~
                        ("stdv" -> snapshot.getStdDev)
                Some(hsub)
            }
        }
    }

    private def meteredAST(meter: Metered): Option[JObject] = {
        val msub =
            ("rate1" -> meter.getOneMinuteRate()) ~
                ("rate5" -> meter.getFiveMinuteRate()) ~
                ("rate15" -> meter.getFifteenMinuteRate()) ~
                ("rateMean" -> meter.getMeanRate())
        Some(msub)
    }
}