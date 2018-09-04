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
package com.hurence.logisland.util;

public class Tuple<A, B> {

    final A key;
    final B value;

    public Tuple(A key, B value) {
        this.key = key;
        this.value = value;
    }

    public A getKey() {
        return key;
    }

    public B getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!(other instanceof Tuple)) {
            return false;
        }

        final Tuple<?, ?> tuple = (Tuple<?, ?>) other;
        if (key == null) {
            if (tuple.key != null) {
                return false;
            }
        } else {
            if (!key.equals(tuple.key)) {
                return false;
            }
        }

        if (value == null) {
            if (tuple.value != null) {
                return false;
            }
        } else {
            if (!value.equals(tuple.value)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return 581 + (this.key == null ? 0 : this.key.hashCode()) + (this.value == null ? 0 : this.value.hashCode());
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
