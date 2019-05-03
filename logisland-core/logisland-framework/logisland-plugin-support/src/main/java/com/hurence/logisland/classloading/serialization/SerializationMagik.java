/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.classloading.serialization;

import java.io.ObjectStreamException;

/**
 * Java calls those hooks when serializing an object. See {@link java.io.Serializable}.
 * Adding those methods to a public interface make possible proxying those methods with our CGLib handler.
 *
 * @author amarziali
 */
public interface SerializationMagik {

    Object writeReplace() throws ObjectStreamException;
}