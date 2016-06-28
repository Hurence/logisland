package com.caseystella.analytics.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Map;

public enum JSONUtil {
    INSTANCE;
    static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
        /**
         * Returns the current thread's "initial value" for this
         * thread-local variable.  This method will be invoked the first
         * time a thread accesses the variable with the {@link #get}
         * method, unless the thread previously invoked the {@link #set}
         * method, in which case the <tt>initialValue</tt> method will not
         * be invoked for the thread.  Normally, this method is invoked at
         * most once per thread, but it may be invoked again in case of
         * subsequent invocations of {@link #remove} followed by {@link #get}.
         * <p/>
         * <p>This implementation simply returns <tt>null</tt>; if the
         * programmer desires thread-local variables to have an initial
         * value other than <tt>null</tt>, <tt>ThreadLocal</tt> must be
         * subclassed, and this method overridden.  Typically, an
         * anonymous inner class will be used.
         *
         * @return the initial value for this thread-local
         */
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper();
        }
    };

    public <T> T load(File f, Class<T> clazz) throws IOException {
        return load(new FileInputStream(f), clazz);
    }

    public <T> T load(InputStream is, Class<T> clazz) throws IOException {
        T ret = MAPPER.get().readValue(is, clazz);
        return ret;
    }
    public <T> T load(String s, Charset c, Class<T> clazz) throws IOException {
        return load( new ByteArrayInputStream(s.getBytes(c)), clazz);
    }
    public <T> T load(String s, Class<T> clazz) throws IOException {
        return load( s, Charset.defaultCharset(), clazz);
    }
    public <T> T load(InputStream is, TypeReference<T> reference) throws IOException {
        T ret = MAPPER.get().readValue(is, reference);
        return ret;
    }
    public <T> T load(String s, Charset c, TypeReference<T> reference) throws IOException {
        return load( new ByteArrayInputStream(s.getBytes(c)), reference);
    }
    public <T> T load(String s, TypeReference<T> reference) throws IOException {
        return load( s, Charset.defaultCharset(), reference);
    }

    public  String toJSON(Object bean ) throws JsonProcessingException {
        return MAPPER.get().writerWithDefaultPrettyPrinter().writeValueAsString(bean);
    }

}
