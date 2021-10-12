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