/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.event;

import java.io.Serializable;
import java.util.*;

/**
 * Encapsulation of an Event a map of Fields
 *
 * @author Tom Bailet
 */
public class Event implements Serializable {

    private Map<String, EventField> fields = new HashMap<>();
    private Date creationDate = new Date();
    private String type = "none";
    private String id = "none";

    public Event() {
    }

    public Event(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Event{" +
                "fields=" + fields +
                ", creationDate=" + creationDate +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        if (fields != null ? !fields.equals(event.fields) : event.fields != null) return false;
      //  if (creationDate != null ? !creationDate.equals(event.creationDate) : event.creationDate != null) return false;
        if (type != null ? !type.equals(event.type) : event.type != null) return false;
        return id != null ? id.equals(event.id) : event.id == null;

    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @param name
     * @param value
     */
    public void put(String name, EventField value) {
        fields.put(name, value);
    }

    public void put(String name, String type, Object value) {
        put(name, new EventField(name, type, value));
    }

    public void putAll(Map<String, Object> entrySets) {
        Objects.requireNonNull(entrySets, "Argument can not be null");
        for(Map.Entry<String, Object> entry : entrySets.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            this.put(key, "object", value);
        }
    }

    public EventField remove(String name) {
        return fields.remove(name);
    }

    public EventField get(String name) {
        return fields.get(name);
    }

    public Collection<EventField> values() {
        return fields.values();
    }

    public Set<String> keySet() {
        return fields.keySet();
    }

    public Set<Map.Entry<String, EventField>> entrySet() {
        return fields.entrySet();
    }
}
