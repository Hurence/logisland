package com.hurence.logisland.processor.webAnalytics.modele;

public class InvalidSessionCheckResult implements SessionCheckResult {

        private final String reason;

        public InvalidSessionCheckResult(final String reason) { this.reason = reason; }

        @Override
        public boolean isValid() { return false; }

        @Override
        public String reason() { return this.reason; }

}
