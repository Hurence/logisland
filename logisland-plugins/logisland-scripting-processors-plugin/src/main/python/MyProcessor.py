from AbstractProcessor import AbstractProcessor

class MyProcessor(AbstractProcessor):

    def init(self, context):
        print "Inside init of MyProcessor code"
  
    def process(self, context, records):
        print "Inside process multi records of MyProcessor code"

    def process(self, context, record):
        print "Inside process mono record of MyProcessor code"
