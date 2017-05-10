package com.hurence.logisland.processor.useragent;

/**
 * Interface to allow the GenericObjectPool to create new objects in the pool.
 *
 */
interface ObjectFactory<T> {

    public T getInstance();

    public void destroy(T obj);

}
