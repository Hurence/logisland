#!/usr/bin/env python

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

class AbstractProcessor:
    """ generated source for class AbstractProcessor """
    #INCLUDE_INPUT_RECORDS = PropertyDescriptor.Builder().name("include.input.records").description("if set to true all the input records are copied to output").required(False).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("true").build()

    def onPropertyModified(self, descriptor, oldValue, newValue):
        """ generated source for method onPropertyModified """

    def init(self, context):
        """ generated source for method init """

    def process(self, context, record):
        """ generated source for method process """

