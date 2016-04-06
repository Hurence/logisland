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

mainClass in Compile := Some("com.hurence.logisland.app.AppLauncher")

// Native packager

packageArchetype.java_application

packageDescription := "Hurence Log-Island"

mappings in Universal += file("README.md") -> "README.md"

mappings in Universal += file("CHANGES.md") -> "CHANGES.md"

mappings in Universal ++= directory("conf")

mappings in Universal ++= directory("data/var")

mappings in Universal ++= directory("bin")

mappings in Universal ++= directory("docs/_site")

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

unmanagedClasspath in Runtime <+= (baseDirectory) map { bd => Attributed.blank(bd / "conf") }

unmanagedClasspath in Runtime <+= (baseDirectory) map { bd => Attributed.blank(bd / "lib") }

libraryDependencies ++= Seq(
    "log4j" % "log4j" % "1.2.17",
    "org.elasticsearch" % "elasticsearch" % "1.7.1",
    "org.scalactic" % "scalactic_2.10" % "2.2.6",
    "org.scalatest" % "scalatest_2.10" % "2.2.6" % Test,
    "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided",
    "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "provided",
    "org.apache.spark" % "spark-sql_2.10" % "1.6.0" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.6.0" % "provided",
    "org.apache.spark" % "spark-hive_2.10" % "1.6.0" % "provided",
    "org.apache.spark" % "spark-streaming-kafka-assembly_2.10" % "1.6.0",
    "org.apache.commons" % "commons-lang3" % "3.3.2",
    "org.apache.commons" % "commons-math3" % "3.2",
    "junit" % "junit" % "4.12",
    "org.scalanlp" % "breeze-viz_2.10" % "0.11.2",
    "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2",
    "com.googlecode.json-simple" % "json-simple" % "1.1.1",
    "com.esotericsoftware" % "kryo" % "3.0.3",
    "org.apache.kafka" % "kafka_2.10" % "0.8.2.2",
    "asm" % "asm" % "3.3.1",
    "org.apache.kafka" % "kafka_2.10" % "0.8.2.2" % Test classifier "test",
    "com.novocode" % "junit-interface" % "0.11" % Test,
    "io.dropwizard.metrics" % "metrics-core" % "3.1.0",
    "com.maxmind.geoip2" % "geoip2" % "2.6.0"
)

resolvers ++= Seq(
    "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
    "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases",
    "Maven Repository" at "http://mvnrepository.com/artifact"
)

fork in Test := true

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

parallelExecution in Test := false
