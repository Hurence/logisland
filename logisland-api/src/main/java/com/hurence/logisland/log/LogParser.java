/*
 * Copyright 2016 Hurence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.log;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.processor.ProcessContext;

import java.io.Serializable;
import java.util.Collection;


/**
 * Strategy interface for providing the log data. <br>
 *
 * Implementations are expected to be stateful.
 *
 * Implementations need <b>not</b> be thread-safe and clients of a {@link LogParser}
 * need to be aware that this is the case.<br>
 *
 * Note that if we want to maintain
 *
 *
 * @author Tom Bailet
 */
public interface LogParser extends Serializable{

    Collection<Event> parse(ProcessContext context, String key, String value) throws LogParserException;

}



