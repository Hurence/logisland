package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.processor.webAnalytics.IncrementalWebSession;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents one or more sessions resulting of the processing of web events.
 */
public class SessionsCalculator {
    private final IncrementalWebSession processor;
    //sessionId in input of logisland
    private final String originalSessionId;
    //last web session for the sessionId before any processing
    private final WebSession lastSessionBeforeProcessing;

    // The resulting sessions from the processed web events.
    // MAKE SURE LAST SESSION IS AT LAST POSITION!!!
    private final List<WebSession> processedSessions = new ArrayList<>();
    private long eventCount;


    /**
     *
     * @param processor
     * @param originalSessionId
     * @param lastSessionBeforeProcessing
     */
    public SessionsCalculator(IncrementalWebSession processor,
                              final String originalSessionId,
                              WebSession lastSessionBeforeProcessing) {
        this.processor = processor;
        this.originalSessionId = originalSessionId;
        this.lastSessionBeforeProcessing = lastSessionBeforeProcessing;
    }

    /**
     * Returns the session identifier of this session.
     *
     * @return the session identifier of this session.
     */
    public String getOriginalSessionId() {
        return this.originalSessionId;
    }

    /**
     * Processes the provided events against the first session (if any).
     *
     * @param events the events to process.
     * @return this object for convenience.
     */
    public SessionsCalculator processEvents(final Events events) {
        processor.debug("Applying %d events to session '%s'", events.size(), events.getSessionId());

        if (this.lastSessionBeforeProcessing != null) {
            // One or more sessions were already stored in datastore.
            final Iterator<Event> eventIterator = events.iterator();

            Event event = null;
            boolean outsideTimeWindow = false;
            // Skip all events that have their timestamp in the range of the [first, last] timestamps of the
            // web session. This happens in case the kafka topic was re-read from earliest than the last
            // processed messages.
            while (eventIterator.hasNext()) {
                event = eventIterator.next();
                outsideTimeWindow = !lastSessionBeforeProcessing.containsTimestamp(event.getTimestamp());
                if (outsideTimeWindow) {
                    break;
                }
            }

            if (outsideTimeWindow) {
                // Event iterator points to first event outside of session's time window.
                // Recreates a list from the first event outside of the time window included.
                final Events nextEvents = new Events(events.tailSet(event));

                final String sessionIdOfCurrentSession = this.lastSessionBeforeProcessing.getSessionId();
                nextEvents.forEach(toRename -> toRename.rename(sessionIdOfCurrentSession));

                // Resume from first session.
                this.processEvents(lastSessionBeforeProcessing, nextEvents);
            }
        } else {
            // No web session yet exists for this session identifier. Create a new one.
            this.processEvents(null, events);
        }

        return this;
    }

    /**
     * Processes the provided events against the provided session (can be {@code null}).
     *
     * @param session the session to update. If {@code null} is provided a new session is created.
     * @param events  the events to process.
     * @return this object for convenience.
     */
    private void processEvents(WebSession session,
                               final Events events) {
        if (events.isEmpty()) {
            // No event. Paranoid.
            return;
        }

        final Iterator<Event> iterator = events.iterator();
        processor.debug("Processing event sessionId=" + events.getSessionId() + " eventCount=" + eventCount);

        if (session == null) {
            // No web-session yet in datastore.
            Event event = iterator.next();
            eventCount++;
            session = new WebSession(event, processor);
            session.add(event);
        }

        this.processedSessions.add(session);

        while (iterator.hasNext()) {
            final Event event = iterator.next();
            eventCount++;

            final SessionCheckResult isSessionValid = isEventApplicable(session, event);

            if (isSessionValid.isValid()) {
                // No invalid check found.
                session.add(event);
            } else {
                // Invalid check found:
                // 1. keep current web-session untouched (and save it)
                // 2. create a new web-session from the current web-event and rename/increase session-id.
                final String[] oldSessionId = event.getSessionId().split(IncrementalWebSession.EXTRA_SESSION_DELIMITER);
                final int index = (oldSessionId.length == 1) ? 2 // only one web session so far => create 2nd one
                        : Integer.valueOf(oldSessionId[1]) + 1; // +1 on web session
                final String newSessionId = oldSessionId[0] + IncrementalWebSession.EXTRA_SESSION_DELIMITER + index;
                final Collection<Event> renamedEvents = events.tailSet(event);
                // Rewrite all remaining web-events with new session identifier.
                renamedEvents.forEach(toRename -> toRename.rename(newSessionId));
                // Mark event that triggered the new sessions with the reason.
                event.record.setField(processor._NEW_SESSION_REASON_FIELD, FieldType.STRING, isSessionValid.reason());

                final Events nextEvents = new Events(renamedEvents);

                this.processEvents(null/*force new web-session*/, nextEvents);
                break;
            }
        }
    }

    public static String extractOrignalSessionsId(String sessionId) {
        final String[] splittedSessionId = sessionId.split(IncrementalWebSession.EXTRA_SESSION_DELIMITER);
        return splittedSessionId[0];
    }


    /**
     * Returns {@code true} if the specified web-event checked against the provided web-session is valid;
     * {@code false} otherwise.
     * In case the returned value is {@code false} then a new session must be created.
     *
     * @param webSession the web-session to check the web-event against.
     * @param webEvent   the web-event to validate.
     * @return {@code true} if the specified web-event checked against the provided web-session is valid;
     * {@code false} otherwise.
     */
    private SessionCheckResult isEventApplicable(final WebSession webSession,
                                                 final Event webEvent) {
        SessionCheckResult result = IncrementalWebSession.VALID;
        for (final SessionCheck check : processor.checker) {
            result = check.isValid(webSession, webEvent);
            if (!result.isValid()) {
                break;
            }
        }

        return result;
    }


    /**
     * Returns the processed web sessions.
     *
     * @return the processed web sessions.
     */
    public Collection<WebSession> getSessions() {
        return processedSessions;
    }

    /**
     * Returns the last sessionId (#?) of this session container.
     *
     * @return the last sessionId (#?) of this session container.
     */
    public String getLastSessionId() {
        String result = this.originalSessionId;

        if (!this.processedSessions.isEmpty()) {
            result = this.processedSessions.get(this.processedSessions.size() - 1).getSessionId();
        } else {
            processor.error("Invalid state: session container for '" + this.originalSessionId + "' is empty. " +
                    "At least one session is expected");
        }

        return result;
    }
}
