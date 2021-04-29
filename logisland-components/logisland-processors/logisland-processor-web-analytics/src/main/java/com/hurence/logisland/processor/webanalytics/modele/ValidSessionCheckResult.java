package com.hurence.logisland.processor.webanalytics.modele;

import java.io.Serializable;

public class ValidSessionCheckResult implements SessionCheckResult, Serializable
{

    /** Holder */
    private static class SingletonHolder {
        /** Instance unique non préinitialisée */
        private final static ValidSessionCheckResult instance = new ValidSessionCheckResult();
    }

    /** Point d'accès pour l'instance unique du singleton */
    public static ValidSessionCheckResult getInstance() {
        return SingletonHolder.instance;
    }

    /** Constructeur privé */
    private ValidSessionCheckResult() {}

    @Override
    public boolean isValid() { return true; }

    @Override
    public String reason() { return null; }

    /** Sécurité anti-désérialisation */
    private Object readResolve() {
        return getInstance();
    }
}