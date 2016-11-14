#!/usr/bin/env python
""" generated source for module ConfigurableComponent """
# 
#  * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
# package: com.hurence.logisland.component
import java.io.Serializable

import java.util.Collection

import java.util.List

class ConfigurableComponent(Serializable):
    """ generated source for interface ConfigurableComponent """
    __metaclass__ = ABCMeta
    # 
    #      * Validates a set of properties, returning ValidationResults for any
    #      * invalid properties. All defined properties will be validated. If they are
    #      * not included in the in the purposed configuration, the default value will
    #      * be used.
    #      *
    #      * @param context of validation
    #      * @return Collection of validation result objects for any invalid findings
    #      * only. If the collection is empty then the component is valid. Guaranteed
    #      * non-null
    #      
    @abstractmethod
    def validate(self, context):
        """ generated source for method validate """

    # 
    #      * @param name to lookup the descriptor
    #      * @return the PropertyDescriptor with the given name, if it exists;
    #      * otherwise, returns <code>null</code>
    #      
    @abstractmethod
    def getPropertyDescriptor(self, name):
        """ generated source for method getPropertyDescriptor """

    # 
    #      * Hook method allowing subclasses to eagerly react to a configuration
    #      * change for the given property descriptor. This method will be invoked
    #      * regardless of property validity. As an alternative to using this method,
    #      * a component may simply getField the latest value whenever it needs it and if
    #      * necessary lazily evaluate it. Any throwable that escapes this method will
    #      * simply be ignored.
    #      *
    #      *
    #      * @param descriptor the descriptor for the property being modified
    #      * @param oldValue the value that was previously set, or null if no value
    #      *            was previously set for this property
    #      * @param newValue the new property value or if null indicates the property
    #      *            was removed
    #      
    @abstractmethod
    def onPropertyModified(self, descriptor, oldValue, newValue):
        """ generated source for method onPropertyModified """

    # 
    #      * Returns a {@link List} of all {@link PropertyDescriptor}s that this
    #      * component supports.
    #      *
    #      * @return PropertyDescriptor objects this component currently supports
    #      
    @abstractmethod
    def getPropertyDescriptors(self):
        """ generated source for method getPropertyDescriptors """

