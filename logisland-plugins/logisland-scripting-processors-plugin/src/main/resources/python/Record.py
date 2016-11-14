#!/usr/bin/env python
""" generated source for module Record """
# 
#  * Copyright (C) 2016 Hurence
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *         http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  

import java.util.Collection

import java.util.Date

import java.util.Map

import java.util.Set

class Record:
    """ generated source for interface Record """
    __metaclass__ = ABCMeta
    @abstractmethod
    def getTime(self):
        """ generated source for method getTime """

    @abstractmethod
    def setTime(self, recordTime):
        """ generated source for method setTime """

    @abstractmethod
    def setFields(self, fields):
        """ generated source for method setFields """

    @abstractmethod
    def addFields(self, fields):
        """ generated source for method addFields """

    @abstractmethod
    def setType(self, type_):
        """ generated source for method setType """

    @abstractmethod
    def getType(self):
        """ generated source for method getType """

    @abstractmethod
    def setId(self, id):
        """ generated source for method setId """

    @abstractmethod
    def getId(self):
        """ generated source for method getId """

    @abstractmethod
    def hasField(self, fieldName):
        """ generated source for method hasField """

    @abstractmethod
    @overloaded
    def setField(self, field):
        """ generated source for method setField """

    @abstractmethod
    @setField.register(object, str, FieldType, object)
    def setField_0(self, fieldName, fieldType, value):
        """ generated source for method setField_0 """

    @abstractmethod
    def setStringField(self, fieldName, value):
        """ generated source for method setStringField """

    @abstractmethod
    def removeField(self, fieldName):
        """ generated source for method removeField """

    @abstractmethod
    def getField(self, fieldName):
        """ generated source for method getField """

    @abstractmethod
    def setStringFields(self, entrySets):
        """ generated source for method setStringFields """

    @abstractmethod
    def getAllFieldsSorted(self):
        """ generated source for method getAllFieldsSorted """

    @abstractmethod
    def getAllFields(self):
        """ generated source for method getAllFields """

    @abstractmethod
    def getAllFieldNames(self):
        """ generated source for method getAllFieldNames """

    @abstractmethod
    def getFieldsEntrySet(self):
        """ generated source for method getFieldsEntrySet """

    @abstractmethod
    def isEmpty(self):
        """ generated source for method isEmpty """

    @abstractmethod
    def isValid(self):
        """ generated source for method isValid """

    @abstractmethod
    def size(self):
        """ generated source for method size """

    @abstractmethod
    def sizeInBytes(self):
        """ generated source for method sizeInBytes """

