package com.hurence.logisland.processor.webAnalytics.modele;

import com.hurence.logisland.processor.webAnalytics.IncrementalWebSession;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents one or more sessions resulting of the processing of web events.
 */
public class Sessions {
    private final IncrementalWebSession processor;
    private final String sessionId;

    // Last processed web session retrieved from datastore.
    private final WebSession lastSession;

    // The resulting sessions from the processed web events.
    // MAKE SURE LAST SESSION IS AT LAST POSITION!!!
    private final List<WebSession> processedSessions = new ArrayList<>();
    private long eventCount;


    /**
     * Create a new instance of this class.
     * The provided web sessions are sorted chronologically and only the last one is used as starting point to
     * process all web events.
     *
     * @param storedSessions the sessions stored in the datastore.
     */
    public Sessions(IncrementalWebSession processor, final String sessionId,
                    final Collection<WebSession> storedSessions) {
        this.processor = processor;
        this.sessionId = sessionId;

        this.lastSession = storedSessions == null ? null : Collections.max(storedSessions);

        if (processor._DEBUG) {
            processor.debug("storedSessions=" +
                    (storedSessions == null ? null :
                            storedSessions.stream()
                                    .map(webSession -> webSession.record.getId())
                                    .collect(Collectors.joining(" "))) +
                    ", last=" + (lastSession == null ? null : lastSession.record.getId()));
        }
    }

    /**
     * Returns the session identifier of this session.
     *
     * @return the session identifier of this session.
     */
    public String getSessionId() {
        return this.sessionId;
    }

    /**
     * Processes the provided events against the first session (if any).
     *
     * @param events the events to process.
     * @return this object for convenience.
     */
    public Sessions processEvents(final Events events) {
        processor.debug("Applying %d events to session '%s'", events.size(), events.getSessionId());

        if (this.lastSession != null) {
            // One or more sessions were already stored in datastore.
            final Iterator<WebEvent> eventIterator = events.iterator();

            WebEvent event = null;
            boolean outsideTimeWindow = false;
            // Skip all events that have their timestamp in the range of the [first, last] timestamps of the
            // web session. This happens in case the kafka topic was re-read from earliest than the last
            // processed messages.
            while (eventIterator.hasNext()) {
                event = eventIterator.next();
                outsideTimeWindow = !lastSession.containsTimestamp(event.getTimestamp());
                if (outsideTimeWindow) {
                    break;
                }
            }

            if (outsideTimeWindow) {
                // Event iterator points to first event outside of session's time window.
                // Recreates a list from the first event outside of the time window included.
                final Events nextEvents = new Events(events.tailSet(event));

                final String sessionIdOfCurrentSession = this.lastSession.getSessionId();
                nextEvents.forEach(toRename -> toRename.rename(sessionIdOfCurrentSession));

                // Resume from first session.
                this.processEvents(lastSession, nextEvents);
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

        final Iterator<WebEvent> iterator = events.iterator();
        processor.debug("Processing event sessionId=" + events.getSessionId() + " eventCount=" + eventCount);

        if (session == null) {
            // No web-session yet in datastore.
            WebEvent event = iterator.next();
            eventCount++;
            session = new WebSession(event, processor);
            session.add(event);
        }

        this.processedSessions.add(session);

        while (iterator.hasNext()) {
            final WebEvent event = iterator.next();
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
                final Collection<WebEvent> renamedEvents = events.tailSet(event);
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
                                                 final WebEvent webEvent) {
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
     * Returns the processed sessions as records.
     *
     * @return the processed sessions as records.
     */
    public Collection<Record> getSessions() {
        return processedSessions.stream()
                .map(item -> item.record)
                .collect(Collectors.toSet());
    }

    /**
     * Returns the last sessionId (#?) of this session container.
     *
     * @return the last sessionId (#?) of this session container.
     */
    public String getLastSessionId() {
        String result = this.sessionId;

        if (!this.processedSessions.isEmpty()) {
            result = this.processedSessions.get(this.processedSessions.size() - 1).getSessionId();
        } else {
            processor.error("Invalid state: session container for '" + this.sessionId + "' is empty. " +
                    "At least one session is expected");
        }

        return result;
    }
}
