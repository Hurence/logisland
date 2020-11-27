package com.hurence.logisland.processor.webAnalytics.modele;

import java.util.*;

/**
 * This class represents a collection of events and is provided for convenience.
 */
public class Events
        implements SortedSet<WebEvent> {
    private final SortedSet<WebEvent> set;

    public Events(Collection<WebEvent> events) {
        this.set = new TreeSet<>(events);
    }

    public String getSessionId() {
        return this.first().getSessionId();
    }

    @Override
    public Comparator<? super WebEvent> comparator() {
        return this.set.comparator();
    }

    @Override
    public SortedSet<WebEvent> subSet(WebEvent fromElement, WebEvent toElement) {
        return this.set.subSet(fromElement, toElement);
    }

    @Override
    public SortedSet<WebEvent> headSet(WebEvent toElement) {
        return this.set.headSet(toElement);
    }

    @Override
    public SortedSet<WebEvent> tailSet(WebEvent fromElement) {
        return this.set.tailSet(fromElement);
    }

    @Override
    public WebEvent first() {
        return this.set.first();
    }

    @Override
    public WebEvent last() {
        return this.set.last();
    }

    @Override
    public int size() {
        return this.set.size();
    }

    @Override
    public boolean isEmpty() {
        return this.set.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.set.contains(o);
    }

    @Override
    public Iterator<WebEvent> iterator() {
        return this.set.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.set.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.set.toArray(a);
    }

    @Override
    public boolean add(WebEvent t) {
        return this.set.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return this.set.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.set.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends WebEvent> c) {
        return this.set.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.set.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.set.removeAll(c);
    }

    @Override
    public void clear() {
        this.set.clear();
    }
}
