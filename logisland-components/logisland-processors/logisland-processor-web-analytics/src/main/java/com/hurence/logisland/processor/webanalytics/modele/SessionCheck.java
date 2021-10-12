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

/**
 * This interface defines rules to test whether an event can be applied to a session or not. In case it can not
 * be applied then a new session must be created.
 */
public interface SessionCheck {
    /**
     * Returns {@code true} is the event is applicable to the session incrementally, {@code false} otherwise.
     * If {@code false} is returned then a new session must be created from the provided event and the provided
     * session close.
     *
     * @param session the session to apply the event onto.
     * @param event   the event to apply to the session.
     * @return {@code true} is the event is applicable to the session, {@code false} otherwise.
     */
    SessionCheckResult isValid(WebSession session, Event event);
}
