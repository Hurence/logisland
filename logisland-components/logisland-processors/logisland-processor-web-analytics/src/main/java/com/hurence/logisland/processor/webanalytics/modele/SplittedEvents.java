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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SplittedEvents {
    final private Collection<Events> eventsInNominalMode;
    final private Collection<Events> eventsfromPast;

    public SplittedEvents(Collection<Events> eventsInNominalMode, Collection<Events> eventsFromPast) {
        this.eventsInNominalMode = eventsInNominalMode;
        this.eventsfromPast = eventsFromPast;
    }

    public Collection<Events> getEventsInNominalMode() {
        return eventsInNominalMode;
    }

    public Collection<Events> getEventsfromPast() {
        return eventsfromPast;
    }

    public Stream<Events> getAllEvents() {
        return Stream.concat(
                getEventsfromPast().stream(),
                getEventsInNominalMode().stream()
        );
    }

    public Stream<Events> getAllEventsThatContainsEventsFromPast() {
        return getAllEvents().filter(events -> {
            String dilvotSession = events.getOriginalSessionId();
            return  dilvotSession != null && containEventsFromPast(dilvotSession);
        });
    }

    public boolean isThereEventsFromPast() {
        return !getEventsfromPast().isEmpty();
    }

    public Set<String> getDivolteSessionsWithEventsFromPast() {
        return getEventsfromPast().stream()
                .map(Events::getOriginalSessionId)
                .collect(Collectors.toSet());
    }

    private boolean containEventsFromPast(String dilvotSession) {
        return this.eventsfromPast.stream()
                .anyMatch(events ->  { return dilvotSession.equals(events.getOriginalSessionId()) && !events.isEmpty(); });
    }
}
