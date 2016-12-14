from AbstractProcessor import AbstractProcessor

class MyProcessor(AbstractProcessor):

    def init(self, context):
        print "Inside init of MyProcessor code"
        print context
  
    def process(self, context, records):
        print "Inside process multi records of MyProcessor python code"
        print context
        print records

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
