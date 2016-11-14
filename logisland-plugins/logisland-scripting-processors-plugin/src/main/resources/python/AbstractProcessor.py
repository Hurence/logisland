#!/usr/bin/env python
""" generated source for module AbstractProcessor """
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
# package: com.hurence.logisland.processor
import com.hurence.logisland.component.AbstractConfigurableComponent

import com.hurence.logisland.component.PropertyDescriptor

import com.hurence.logisland.record.Record

import com.hurence.logisland.record.StandardRecord

import com.hurence.logisland.util.validator.StandardValidators

import org.slf4j.Logger

import org.slf4j.LoggerFactory

import java.util.Collection

import java.util.Collections

class AbstractProcessor(AbstractConfigurableComponent, Processor):
    """ generated source for class AbstractProcessor """
    INCLUDE_INPUT_RECORDS = PropertyDescriptor.Builder().name("include.input.records").description("if set to true all the input records are copied to output").required(False).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("true").build()

    def onPropertyModified(self, descriptor, oldValue, newValue):
        """ generated source for method onPropertyModified """
        self.logger.info("property {} value changed from {} to {}", descriptor.__name__, oldValue, newValue)

    def init(self, context):
        """ generated source for method init """
        self.logger.info("init")

    def process(self, context, record):
        """ generated source for method process """
        return self.process(context, Collections.singleton(record))

AbstractProcessor.logger = LoggerFactory.getLogger(AbstractProcessor.__class__)

