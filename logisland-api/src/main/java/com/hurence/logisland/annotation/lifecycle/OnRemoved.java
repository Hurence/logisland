/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.annotation.lifecycle;

import com.hurence.logisland.processor.ProcessContext;

import java.lang.annotation.*;

/**
 * <p>
 * Marker annotation a {@link com.hurence.logisland.processor.Processor Processor},
 * implementation
 * can use to indicate a method should be called whenever the component is
 * removed from the flow. This method will be called once for the entire life of
 * a component instance. If the method throw any Throwable, that Throwable will
 * be caught and logged but will not prevent subsequent methods with this
 * annotation or removal of the component from the flow.
 * </p>
 *
 * <p>
 * Methods with this annotation are permitted to take no arguments or to take a
 * single argument. If the component is a Processor, then the argument must be
 * of type {@link ProcessContext}.
 * </p>
 *
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface OnRemoved {
}
