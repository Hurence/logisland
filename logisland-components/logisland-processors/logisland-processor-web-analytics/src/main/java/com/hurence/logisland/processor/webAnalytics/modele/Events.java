package com.hurence.logisland.processor.webAnalytics.modele;

import java.util.*;

/**
 * This class represents a collection of events and is provided for convenience.
 */
public class Events
        implements SortedSet<Event> {
    private final SortedSet<Event> set;

    public Events(Collection<Event> events) {
        this.set = new TreeSet<>(events);
    }

    @Override
    public Comparator<? super Event> comparator() {
        return this.set.comparator();
    }

    @Override
    public SortedSet<Event> subSet(Event fromElement, Event toElement) {
        return this.set.subSet(fromElement, toElement);
    }

    @Override
    public SortedSet<Event> headSet(Event toElement) {
        return this.set.headSet(toElement);
    }

    @Override
    public SortedSet<Event> tailSet(Event fromElement) {
        return this.set.tailSet(fromElement);
    }

    @Override
    public Event first() {
        return this.set.first();
    }

    @Override
    public Event last() {
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
    public Iterator<Event> iterator() {
        return this.set.iterator();
    }

    public Collection<Event> getAll() {
        return this.set;
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
    public boolean add(Event t) {
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
    public boolean addAll(Collection<? extends Event> c) {
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


    public String getOriginalSessionId() {
        Event firstEvent = this.first();
        return firstEvent.getOriginalSessionId() != null ? firstEvent.getOriginalSessionId() : firstEvent.getSessionId() ;
    }

}
