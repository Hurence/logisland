/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.connect.opcda;

import com.hurence.opc.*;
import com.hurence.opc.util.AutoReconnectOpcOperations;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A 'smart' version of {@link AutoReconnectOpcOperations}.
 * It tracks a stale flag becoming true if the connection has been interrupted and recreated.
 * The stale flag can be reset upon call of method {@link SmartOpcOperations#resetStale()}.
 *
 * @author amarziali
 */
public class SmartOpcOperations<S extends ConnectionProfile<S>, T extends SessionProfile<T>, U extends OpcSession>
        implements OpcOperations<S, T, U> {

    private final AtomicBoolean stale = new AtomicBoolean();
    private final OpcOperations<S, T, U> delegate;

    /**
     * Construct an instance.
     *
     * @param delegate the deletegate {@link OpcOperations}.
     */
    public SmartOpcOperations(OpcOperations<S, T, U> delegate) {
        this.delegate = AutoReconnectOpcOperations.create(delegate);
    }

    @Override
    public void connect(S connectionProfile) {
        stale.set(true);
        delegate.connect(connectionProfile);
        awaitConnected();
    }

    @Override
    public void disconnect() {
        delegate.disconnect();
    }

    /**
     * Reset the connection stale flag and return previous state.
     *
     * @return the stale flag.
     */
    public synchronized boolean resetStale() {
        awaitConnected();
        return stale.getAndSet(false);
    }

    @Override
    public boolean isChannelSecured() {
        return false;
    }

    @Override
    public ConnectionState getConnectionState() {
        return delegate.getConnectionState();
    }

    @Override
    public Collection<OpcTagInfo> browseTags() {
        return delegate.browseTags();
    }

    @Override
    public U createSession(T t) {
        return delegate.createSession(t);
    }

    @Override
    public void releaseSession(U u) {
        delegate.releaseSession(u);
    }

    @Override
    public boolean awaitConnected() {
        return delegate.awaitConnected();
    }

    @Override
    public boolean awaitDisconnected() {
        return delegate.awaitDisconnected();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @Override
    public String toString() {
        return "SmartOpcOperations{" +
                "stale=" + stale +
                "} " + super.toString();
    }
}

