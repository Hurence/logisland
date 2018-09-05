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

package com.hurence.logisland.classloading;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * A cglib autoproxy creator.
 *
 * @author amarziali
 */
public class PluginProxy {

    private static class CglibProxyHandler implements MethodInterceptor {
        private final Object delegate;

        public CglibProxyHandler(Object delegate) {
            this.delegate = delegate;
        }

        public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
            Method delegateMethod = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());
            return delegateMethod.invoke(delegate, args);
        }
    }

    private static Object createProxy(Object object, Class superClass, Class[] interfaces, ClassLoader cl) {
        CglibProxyHandler handler = new CglibProxyHandler(object);

        Enhancer enhancer = new Enhancer();

        if (superClass != null) {
            enhancer.setSuperclass(superClass);
        }

        enhancer.setCallback(handler);

        if (interfaces != null) {
            List<Class> il = new ArrayList<Class>();

            for (Class i : interfaces) {
                if (i.isInterface()) {
                    il.add(i);
                }
            }

            enhancer.setInterfaces(il.toArray(new Class[il.size()]));
        }

        enhancer.setClassLoader(cl == null ? Thread.currentThread().getContextClassLoader() : cl);

        return enhancer.create();
    }

    /**
     * Creates a proxy. Useful tu kind of 'tunnel' object from a classloader to another one.
     * Please beware linkage errors.
     *
     * @param object the object to proxy.
     * @return the proxied object.
     */
    public static Object create(Object object) {

        Class superClass = null;

        // Check class
        try {
            Class.forName(object.getClass().getSuperclass().getName());
            superClass = object.getClass().getSuperclass();
        } catch (ClassNotFoundException e) {
        }

        Class[] interfaces = object.getClass().getInterfaces();

        List<Class<?>> il = new ArrayList<>();

        // Check available interfaces
        for (Class i : interfaces) {
            try {
                Class.forName(i.getClass().getName());
                il.add(i);
            } catch (ClassNotFoundException e) {
            }
        }


        if (superClass == null && il.size() == 0) {
           return object;
        }

        return createProxy(object, superClass, il.toArray(new Class[il.size()]), null);
    }


}
