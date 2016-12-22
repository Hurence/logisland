# coding: utf-8
from AbstractProcessor import AbstractProcessor
from com.hurence.logisland.record import StandardRecord

#
# Simple python processor to test ability to run python code and process some
# records
#
class BasicProcessor(AbstractProcessor):

    def init(self, context):
        print "Inside init of BasicProcessor code"
  
    def process(self, context, records):
        print "Inside process of BasicProcessor python code"

        # Copy the records and add python_field field in it
        outputRecords = []
        for record in records:
            copyRecord = StandardRecord(record)

            # Check that one can read values coming from java
            javaFieldValue = copyRecord.getField("java_field").getRawValue()
            expectedValue = "java_field_value"
            assert (javaFieldValue == expectedValue) , "Expected " + expectedValue + " but got " + javaFieldValue

            copyRecord.setStringField('python_field', 'python_field_value')
            outputRecords.append(copyRecord)
        return outputRecords