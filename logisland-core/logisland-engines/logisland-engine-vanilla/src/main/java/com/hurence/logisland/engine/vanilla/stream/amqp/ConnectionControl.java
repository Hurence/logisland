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
package com.hurence.logisland.engine.vanilla.stream.amqp;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

class ConnectionControl {

    @FunctionalInterface
    public interface ReconnectHandler {
        void reconnect(Vertx vertx) throws Exception;
    }

    private static final Logger logger = LoggerFactory.getLogger(ConnectionControl.class);


    private long currentDelay = 0;

    private final long maxDelay;
    private final long startDelay;
    private final double backoff;
    private volatile boolean running = true;

    public ConnectionControl(long maxDelay, long startDelay, double backoff) {
        this.maxDelay = maxDelay;
        this.startDelay = startDelay;
        this.backoff = backoff;
    }

    public void connected() {
        currentDelay = 0;
    }


    public boolean shouldReconnect() {
        return running;
    }

    public boolean isRunning() {
        return running;
    }

    private long nextDelay() {
        long ret = 0;
        if (currentDelay == 0) {
            ret = startDelay;
        } else {
            ret = Math.min(Math.round(currentDelay * backoff), maxDelay);
        }
        return ret;

    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    void scheduleReconnect(ReconnectHandler reconnectHandler) {
        currentDelay = nextDelay();
        final Handler timerHandler = ignored -> {
            try {
                reconnectHandler.reconnect(Vertx.currentContext().owner());
            } catch (Exception e) {
                logger.warn("Reconnection failed: {}", e.getMessage());
                scheduleReconnect(reconnectHandler);
            }
        };

        if (currentDelay <= 0) {
            Vertx.currentContext().runOnContext(timerHandler);
        } else {
            logger.info("Scheduling connect attempt in  {} ms at {}", currentDelay,
                    Instant.now().plusMillis(currentDelay));
            Vertx.currentContext().owner().setTimer(currentDelay, timerHandler);
        }
    }
}