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
        return  !getEventsfromPast().isEmpty();
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
