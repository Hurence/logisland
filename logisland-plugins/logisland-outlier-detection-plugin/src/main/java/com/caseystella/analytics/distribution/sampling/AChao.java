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
package com.caseystella.analytics.distribution.sampling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * See http://arxiv.org/pdf/1012.0256.pdf
 */
class AChao<T> {
    private final List<T> reservoir;
    double runningCount;
    private final int reservoirCapacity;
    private final Random random;

    public AChao(int capacity) {
        this(capacity, new Random());
    }

    public AChao(int capacity, Random random) {
        reservoir = new ArrayList<>();
        reservoirCapacity = capacity;
        this.random = random;
    }

    public final List<T> getReservoir() {
        return reservoir;
    }

    public void insert(T ele, double weight) {
        runningCount += weight;

        if (reservoir.size() < reservoirCapacity) {
            reservoir.add(ele);
        } else if (random.nextDouble() < weight / runningCount) {
            reservoir.remove(random.nextInt(reservoirCapacity));
            reservoir.add(ele);
        }
    }
}
