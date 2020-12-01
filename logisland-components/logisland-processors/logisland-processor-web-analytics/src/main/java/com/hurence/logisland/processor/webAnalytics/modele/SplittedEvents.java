package com.hurence.logisland.processor.webAnalytics.modele;

import java.util.Collection;

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
}
