#!/usr/bin/env python
""" generated source for module PropertyValue """
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

# 
#  * <p>
#  * A PropertyValue provides a mechanism whereby the currently configured value
#  * of a processor property can be obtained in different forms.
#  * </p>
#  
class PropertyValue:
    """ generated source for interface PropertyValue """
    __metaclass__ = ABCMeta
    # 
    #      * @return the raw property value as a string
    #      
    @abstractmethod
    def getRawValue(self):
        """ generated source for method getRawValue """

    # 
    #      * @return an String representation of the property value, or
    #      * <code>null</code> if not set
    #      
    @abstractmethod
    def asString(self):
        """ generated source for method asString """

    # 
    #      * @return an integer representation of the property value, or
    #      * <code>null</code> if not set
    #      * @throws NumberFormatException if not able to parse
    #      
    @abstractmethod
    def asInteger(self):
        """ generated source for method asInteger """

    # 
    #      * @return a Long representation of the property value, or <code>null</code>
    #      * if not set
    #      * @throws NumberFormatException if not able to parse
    #      
    @abstractmethod
    def asLong(self):
        """ generated source for method asLong """

    # 
    #      * @return a Boolean representation of the property value, or
    #      * <code>null</code> if not set
    #      
    @abstractmethod
    def asBoolean(self):
        """ generated source for method asBoolean """

    # 
    #      * @return a Float representation of the property value, or
    #      * <code>null</code> if not set
    #      * @throws NumberFormatException if not able to parse
    #      
    @abstractmethod
    def asFloat(self):
        """ generated source for method asFloat """

    # 
    #      * @return a Double representation of the property value, of
    #      * <code>null</code> if not set
    #      * @throws NumberFormatException if not able to parse
    #      
    @abstractmethod
    def asDouble(self):
        """ generated source for method asDouble """

    # 
    #      * @param timeUnit specifies the TimeUnit to convert the time duration into
    #      * @return a Long value representing the value of the configured time period
    #      * in terms of the specified TimeUnit; if the property is not set, returns
    #      * <code>null</code>
    #      
    #  public Long asTimePeriod(TimeUnit timeUnit);
    # 
    #      * @return <code>true</code> if the user has configured a value, or if the
    #      * {@link PropertyDescriptor} for the associated property has a default
    #      * value, <code>false</code> otherwise
    #      
    @abstractmethod
    def isSet(self):
        """ generated source for method isSet """

