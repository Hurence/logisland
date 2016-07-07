package com.hurence.logisland.parser.base

import com.hurence.logisland.event.EventField

/**
  * Created by gregoire on 13/04/16.
  */
abstract class BaseLogParserTest extends BaseUnitTest{
    protected def testAnEventField(apacheEventField: EventField, name: String, eventFieldType: String, value: Object) = {
        apacheEventField.getName shouldBe name
        apacheEventField.getType shouldBe eventFieldType
        eventFieldType match {
            case "string" => apacheEventField.getValue.asInstanceOf[String] shouldBe value.asInstanceOf[String]
            case "long" => apacheEventField.getValue.asInstanceOf[Long] shouldBe value.asInstanceOf[Long]
            case "int" => apacheEventField.getValue.asInstanceOf[Int] shouldBe value.asInstanceOf[Int]
            case "double" => apacheEventField.getValue.asInstanceOf[Double] shouldBe value.asInstanceOf[Double]
            case "float" => apacheEventField.getValue.asInstanceOf[Float] shouldBe value.asInstanceOf[Float]
            case "boolean" => apacheEventField.getValue.asInstanceOf[Boolean] shouldBe value.asInstanceOf[Boolean]
            case x => throw new Error(s"this type '$x' is not recognised yet in 'testAnEventField' function")
        }

    }
}
