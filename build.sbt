import com.typesafe.sbt.SbtNativePackager.NativePackagerHelper._
import com.typesafe.sbt.SbtNativePackager.NativePackagerKeys._
import com.typesafe.sbt.SbtNativePackager._
import sbt.Keys._

name := "logisland-core"

version := "0.9.3"

scalaVersion := "2.10.6"

organization := "com.hurence.logisland"

publishMavenStyle := true

crossPaths := false

autoScalaLibrary := false

// Native packager

packageArchetype.java_application

packageDescription := "Hurence Log-Island"

mappings in Universal += file("README.md") -> "README.md"

mappings in Universal += file("CHANGES.md") -> "CHANGES.md"

mappings in Universal ++= directory("conf")

mappings in Universal ++= directory("bin")

mappings in Universal ++= directory("docs/_site")

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-target:jvm-1.7")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

unmanagedClasspath in Runtime <+= (baseDirectory) map { bd => Attributed.blank(bd / "conf") }

unmanagedClasspath in Runtime <+= (baseDirectory) map { bd => Attributed.blank(bd / "lib") }

val SPARK_VERSION = "1.4.1"

val KAFKA_VERSION = "0.8.2.2"

val ELASTICSEARCH_VERSION = "1.7.1"

libraryDependencies ++= Seq(
    // Logging
    "log4j" % "log4j" % "1.2.17",
    "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2",

    // Elasticsearch
    "org.elasticsearch" % "elasticsearch" % ELASTICSEARCH_VERSION,

    // Spark dependencies
    "org.apache.spark" % "spark-core_2.10" % SPARK_VERSION % "provided",
    "org.apache.spark" % "spark-streaming_2.10" % SPARK_VERSION % "provided",
    "org.apache.spark" % "spark-sql_2.10" % SPARK_VERSION % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % SPARK_VERSION % "provided",
    "org.apache.spark" % "spark-hive_2.10" % SPARK_VERSION % "provided",
    "org.apache.spark" % "spark-streaming-kafka-assembly_2.10" % SPARK_VERSION,

    // Kafka stuff
    "org.apache.kafka" % "kafka_2.10" % KAFKA_VERSION,
    "org.apache.kafka" % "kafka_2.10" %KAFKA_VERSION % Test classifier "test",

    // misc
    "org.apache.commons" % "commons-lang3" % "3.3.2",
    "org.apache.commons" % "commons-math3" % "3.2",
    "junit" % "junit" % "4.12",
    "org.scalanlp" % "breeze-viz_2.10" % "0.11.2",
    "joda-time" % "joda-time" % "2.8.1",
    "com.googlecode.json-simple" % "json-simple" % "1.1.1",
    "com.esotericsoftware" % "kryo" % "3.0.3",
    "asm" % "asm" % "3.3.1",
    "io.dropwizard.metrics" % "metrics-core" % "3.1.0",
    "com.maxmind.geoip2" % "geoip2" % "2.6.0",

    // test
    "com.novocode" % "junit-interface" % "0.11" % Test,
    "org.scalactic" % "scalactic_2.10" % "2.2.6",
    "org.scalatest" % "scalatest_2.10" % "2.2.6" % Test
)

resolvers ++= Seq(
    "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
    "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases",
    "Maven Repository" at "http://mvnrepository.com/artifact"
)

fork in Test := true

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

parallelExecution in Test := false

publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
    else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomExtra := (
    <url>http://jsuereth.com/scala-arm</url>
        <licenses>
            <license>
                <name>The Apache License, Version 2.0</name>
                <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
            </license>
        </licenses>

        <issueManagement>
            <system>Github Issues</system>
            <url>https://github.com/Hurence/log-island/issues</url>
        </issueManagement>

        <scm>
            <url>git@github.com:Hurence/log-island.git</url>
            <connection>scm:git@github.com:Hurence/log-island.git</connection>
        </scm>

        <developers>
            <developer>
                <id>hurence</id>
                <name>Hurence</name>
                <url>http://hurence.com</url>
            </developer>
        </developers>

    )