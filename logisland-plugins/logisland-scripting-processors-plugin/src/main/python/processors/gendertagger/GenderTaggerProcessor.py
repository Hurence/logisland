# coding: utf-8
from AbstractProcessor import AbstractProcessor
from com.hurence.logisland.record import StandardRecord

# NLTK dependencies are by default searched in the dependencies directory
# (conventional name) at the same location of the python processor script
# This can be changed using the python_processor.python_processor_dependencies_path
# config property of the java python processor poiting to this current script
from nltk.corpus import names
import nltk.data
import random

# Path to the nltk data corpora. We only need male.txt and female.txt files
# but they must respect nltk_data file layouting
nltk_data_path = "src/main/python/processors/gendertagger/nltk_data"

# Features are the 2 last characters of the first name
def gender_features(word):
    return {'suffix1': word[-2] , 'suffix2': word[-1]}

#
# This gender tagger processor uses a trained classifier to tag a record event
# with the passed record author with a tag specifying the gender of the author
# 
# The python_processor.python_processor_script_path config property of the
# java python processor must point to a pyhton module file. This module must
# at least contain the definition of a python class with the same name as the
# one of the module and this class must inherits from the logisland provided
# python class: AbstractProcessor
#
class GenderTaggerProcessor(AbstractProcessor):
    
    # Features are the 2 last characters of the first name
    #def gender_features(self, word):
    #    return {'suffix1': word[-2] , 'suffix2': word[-1]}

    def initClassifier(self):
        
        # Set the nltk_data path so that we load the logisland mebedded corpus
        nltk.data.path.insert(0, nltk_data_path)
        
        # Loading gender data
        print "Loading gender data..."
        labeled_names = ([(name, 'male') for name in names.words('male.txt')] +
                 [(name, 'female') for name in names.words('female.txt')])
        random.shuffle(labeled_names)
        print "Loaded " + str(len(labeled_names)) + " samples"
        
        # Train classifier  with data
        print "Training gender classifier..."
        featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
        self.classifier = nltk.classify.NaiveBayesClassifier.train(featuresets)
        print "Gender classifier trained"

    def init(self, context):
        print "Inside init of GenderTaggerProcessor python code"
        self.initClassifier()
        
    def onPropertyModified(self, descriptor, oldValue, newValue):
        print "Inside onPropertyModified of GenderTaggerProcessor python code"
   
    #
    # Get the record's author firstname and tag the record with the author's
    # gender
    #
    def process(self, context, records):
        print "Inside process of GenderTaggerProcessor python code"

        outputRecords = []
        for record in records:
            copyRecord = StandardRecord(record)

            # Get record author first name
            authorFirstname = copyRecord.getField('author_firstname').getRawValue()

            # Guess author gender
            gender = self.classifier.classify(gender_features(authorFirstname))

            # Tag record with author's gender
            print "Tagging record from  author <" + authorFirstname + "> with guessed gender: <" + gender + ">"
            copyRecord.setStringField('author_gender', gender)
            
            outputRecords.append(copyRecord)
         
        return outputRecords