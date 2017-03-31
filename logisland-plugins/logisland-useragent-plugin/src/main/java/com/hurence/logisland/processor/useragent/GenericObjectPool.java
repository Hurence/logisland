package com.hurence.logisland.processor.useragent;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is a generic pool to store objects.
 * The instantiation is lazy, only when requested to ensure that objects are created only is required
 *
 * TODO : Make it a blocking pool to ensure null is never returned
 * TODO : should be enhanced with TTL to destroy object after some time
 */
public class GenericObjectPool<T> {

    private int max;
    private ObjectFactory<T> factory;
    private ConcurrentLinkedQueue<T> available = new ConcurrentLinkedQueue();
    private ConcurrentLinkedQueue<T> inUse = new ConcurrentLinkedQueue();

    private Object sync = new Object();



    public GenericObjectPool(ObjectFactory<T> factory, int max) {
        this.max = max;
        this.factory = factory;
    }


    public T get() {

        synchronized(sync) {

            T obj = available.poll();
            if (obj == null) {
                if (inUse.size() < max) {
                    obj = factory.getInstance();
                }

                if (obj != null) {
                    inUse.add(obj);
                } else {
                    throw new RuntimeException("Reached max pool size of " + max);
                }
            }

            return obj;
        }
    }


    public void release(T obj) {
        synchronized(sync) {
            boolean removed = inUse.remove(obj);
            if (!removed) {
                throw new RuntimeException("Failed to remove object");
            } else {
                available.add(obj);
            }
        }
    }

     public int size() {
         synchronized(sync) {
             return available.size() + inUse.size();
         }
     }

     public int maxSize() {
         return max;
     }
}
