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
package com.hurence.logisland.processor.webanalytics.modele;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * This class represents one or more sessions resulting of the processing of web events.
 */
public class Sessions {

    private static final Logger logger = LoggerFactory.getLogger(Sessions.class);

    private final String divolteSessionId;
    // The resulting sessions from the processed web events.
    // MAKE SURE LAST SESSION IS AT LAST POSITION!!!
    private final List<WebSession> calculatedSessions;

    public Sessions(String divolteSessionId,
                    List<WebSession> calculatedSessions) {
        this.divolteSessionId = divolteSessionId;
        this.calculatedSessions = calculatedSessions;
    }

    /**
     * Returns the session identifier of this session.
     *
     * @return the session identifier of this session.
     */
    public String getDivolteSessionId() {
        return this.divolteSessionId;
    }


    /**
     * Returns the processed web sessions.
     *
     * @return the processed web sessions.
     */
    public Collection<WebSession> getCalculatedSessions() {
        return calculatedSessions;
    }

    /**
     * Returns the last sessionId (#?) of this session container.
     *
     * @return the last sessionId (#?) of this session container.
     */
    public String getLastSessionId() {
        String result = this.divolteSessionId;

        if (!this.calculatedSessions.isEmpty()) {
            result = this.calculatedSessions.get(this.calculatedSessions.size() - 1).getSessionId();
        } else {
            logger.error("Invalid state: session container for '" + this.divolteSessionId + "' is empty. " +
                    "At least one session is expected");
        }
        return result;
    }
}
