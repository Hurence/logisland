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
        print "Inside process multi records of BasicProcessor python code"

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

# same-name method with different parameters in python seems impossible or at 
# least tricky. See http://stackoverflow.com/questions/22377338/how-to-write-same-name-methods-with-different-parameters-in-python
# So for the moment juste implementing multi records version. Otherwise the latest
# defined seems to be called. In clear, if you uncomment this second process method,
# this one will be called instead of multi records version. 
# So for the moment, only using multi records version (mono call is multi version with one item).
# One could also decide call two distincts method processRecord and processRecords
#
#    def process(self, context, record):
#        print "Inside process mono record of MyProcessor python code"
