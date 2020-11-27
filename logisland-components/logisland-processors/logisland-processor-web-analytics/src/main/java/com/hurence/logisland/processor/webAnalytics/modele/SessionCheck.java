package com.hurence.logisland.processor.webAnalytics.modele;

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
    SessionCheckResult isValid(WebSession session, WebEvent event);
}
