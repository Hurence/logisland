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
package com.hurence.logisland.kafka.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public abstract class RegistryKey implements Comparable<RegistryKey> {

    @Min(0)
    protected int magicByte;
    @NotEmpty
    protected RegistryKeyType keyType;

    public RegistryKey(@JsonProperty("keytype") RegistryKeyType keyType) {
        this.keyType = keyType;
    }

    @JsonProperty("magic")
    public int getMagicByte() {
        return this.magicByte;
    }

    @JsonProperty("magic")
    public void setMagicByte(int magicByte) {
        this.magicByte = magicByte;
    }

    @JsonProperty("keytype")
    public RegistryKeyType getKeyType() {
        return this.keyType;
    }

    @JsonProperty("keytype")
    public void setKeyType(RegistryKeyType keyType) {
        this.keyType = keyType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegistryKey that = (RegistryKey) o;

        if (this.magicByte != that.magicByte) {
            return false;
        }
        if (!this.keyType.equals(that.keyType)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 31 * this.magicByte;
        result = 31 * result + this.keyType.hashCode();
        return result;
    }

    @Override
    public int compareTo(RegistryKey otherKey) {
        return this.keyType.compareTo(otherKey.keyType);
    }
}
