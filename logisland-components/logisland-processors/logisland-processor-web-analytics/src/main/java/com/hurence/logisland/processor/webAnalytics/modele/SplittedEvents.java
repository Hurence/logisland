package com.hurence.logisland.processor.webAnalytics.modele;

import java.util.Collection;
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
        return Stream.concat(
                getEventsfromPast().stream(),
                getEventsInNominalMode().stream()
        ).filter(events -> {
            String dilvotSession = events.getOriginalSessionId();
            return  dilvotSession != null && containEventsFromPast(dilvotSession);
        });
    }

    private boolean containEventsFromPast(String dilvotSession) {
        return this.eventsfromPast.stream()
                .anyMatch(events ->  { return dilvotSession.equals(events.getOriginalSessionId()) && !events.isEmpty(); });
    }
}
