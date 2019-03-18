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
package com.hurence.logisland.classloading;

import com.hurence.logisland.classloading.serialization.AutoProxiedSerializablePlugin;
import com.hurence.logisland.classloading.serialization.SerializationMagik;
import com.hurence.logisland.component.AbstractConfigurableComponent;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.InvocationHandler;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;

/**
 * A cglib autoproxy creator.
 *
 * @author amarziali
 */
public class PluginProxy {

    /**
     * A method handler for our proxies.
     *
     * @author amarziali
     */
    private static class CglibProxyHandler implements InvocationHandler, Serializable {

        private transient final Object delegate;

        public CglibProxyHandler(Object delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object o, Method method, Object[] args) throws Throwable {
            if (delegate instanceof Serializable) {
                if ("writeReplace".equals(method.getName())) {
                    return new AutoProxiedSerializablePlugin(delegate.getClass().getCanonicalName(), SerializationUtils.serialize((Serializable) delegate));
                }
            }
            if ("resolveDelegate".equals(method.getName())) {
                return delegate;
            }
            Method delegateMethod = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());

            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            ClassLoader toSet = delegate != null ? delegate.getClass().getClassLoader() : Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(toSet);
                return delegateMethod.invoke(delegate, args);
            } finally {
                if (toSet.equals(Thread.currentThread().getContextClassLoader())) {
                    Thread.currentThread().setContextClassLoader(cl);
                }
            }
        }
    }

    private static Object createProxy(Object object, Class superClass, Class[] interfaces, ClassLoader cl) {
        CglibProxyHandler handler = new CglibProxyHandler(object);

        Enhancer enhancer = new Enhancer();


        if (superClass != null && !AbstractConfigurableComponent.class.isAssignableFrom(superClass)) {
            enhancer.setSuperclass(superClass);
        }

        enhancer.setCallback(handler);

        Set<Class<?>> il = new LinkedHashSet<>();
        il.add(SerializationMagik.class);
        il.add(DelegateAware.class);


        if (interfaces != null) {

            for (Class<?> i : interfaces) {
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
    public static <T> T create(T object) {

        Class<?> superClass = null;

        // Check class
        Class<?> currentCls = object.getClass();
        while (superClass == null) {
            try {
                Class.forName(currentCls.getSuperclass().getName());
                superClass = currentCls.getSuperclass();
            } catch (ClassNotFoundException e) {
                currentCls = currentCls.getSuperclass();
            }
        }

        Class[] interfaces = getAllInterfaces(object);

        List<Class<?>> il = new ArrayList<>();

        // Check available interfaces
        for (Class<?> i : interfaces) {
            try {
                Class.forName(i.getClass().getName());
                il.add(i);
            } catch (ClassNotFoundException e) {
            }
        }


        if (superClass == null && il.size() == 0) {
            return object;
        }

        return (T) createProxy(object, superClass, il.toArray(new Class[il.size()]), null);
    }


    public static <T> T unwrap(Object object) {
        if (object instanceof DelegateAware) {
            return (T) ((DelegateAware) object).resolveDelegate();
        }
        return (T) object;
    }

    public static <T> T rewrap(Object object) {
        Object o = PluginProxy.unwrap(object);
        if (o != null) {
            return (T) PluginProxy.create(o);
        }
        return null;
    }

    /**
     * Return all interfaces that the given instance implements as an array,
     * including ones implemented by superclasses.
     *
     * @param instance the instance to analyze for interfaces
     * @return all interfaces that the given instance implements as an array
     */
    public static Class<?>[] getAllInterfaces(Object instance) {
        return getAllInterfacesForClass(instance.getClass());
    }

    /**
     * Return all interfaces that the given class implements as an array,
     * including ones implemented by superclasses.
     * <p>If the class itself is an interface, it gets returned as sole interface.
     *
     * @param clazz the class to analyze for interfaces
     * @return all interfaces that the given object implements as an array
     */
    public static Class<?>[] getAllInterfacesForClass(Class<?> clazz) {
        return getAllInterfacesForClass(clazz, null);
    }

    /**
     * Return all interfaces that the given class implements as an array,
     * including ones implemented by superclasses.
     * <p>If the class itself is an interface, it gets returned as sole interface.
     *
     * @param clazz       the class to analyze for interfaces
     * @param classLoader the ClassLoader that the interfaces need to be visible in
     *                    (may be {@code null} when accepting all declared interfaces)
     * @return all interfaces that the given object implements as an array
     */
    public static Class<?>[] getAllInterfacesForClass(Class<?> clazz, ClassLoader classLoader) {
        return toClassArray(getAllInterfacesForClassAsSet(clazz, classLoader));
    }

    /**
     * Return all interfaces that the given class implements as a Set,
     * including ones implemented by superclasses.
     * <p>If the class itself is an interface, it gets returned as sole interface.
     *
     * @param clazz       the class to analyze for interfaces
     * @param classLoader the ClassLoader that the interfaces need to be visible in
     *                    (may be {@code null} when accepting all declared interfaces)
     * @return all interfaces that the given object implements as a Set
     */
    public static Set<Class<?>> getAllInterfacesForClassAsSet(Class<?> clazz, ClassLoader classLoader) {
        if (clazz.isInterface() && isVisible(clazz, classLoader)) {
            return Collections.<Class<?>>singleton(clazz);
        }
        Set<Class<?>> interfaces = new LinkedHashSet<Class<?>>();
        Class<?> current = clazz;
        while (current != null) {
            Class<?>[] ifcs = current.getInterfaces();
            for (Class<?> ifc : ifcs) {
                interfaces.addAll(getAllInterfacesForClassAsSet(ifc, classLoader));
            }
            current = current.getSuperclass();
        }
        return interfaces;
    }

    /**
     * Check whether the given class is visible in the given ClassLoader.
     *
     * @param clazz       the class to check (typically an interface)
     * @param classLoader the ClassLoader to check against (may be {@code null},
     *                    in which case this method will always return {@code true})
     */
    public static boolean isVisible(Class<?> clazz, ClassLoader classLoader) {
        if (classLoader == null) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }
        try {
            Class<?> actualClass = classLoader.loadClass(clazz.getName());
            //return (clazz == actualClass);
            return true;
            // Else: different interface class found...
        } catch (ClassNotFoundException ex) {
            // No interface class found...
            return false;
        }
    }

    /**
     * Copy the given {@code Collection} into a {@code Class} array.
     * <p>The {@code Collection} must contain {@code Class} elements only.
     *
     * @param collection the {@code Collection} to copy
     * @return the {@code Class} array
     * @since 3.1
     */
    public static Class<?>[] toClassArray(Collection<Class<?>> collection) {
        if (collection == null) {
            return null;
        }
        return collection.toArray(new Class<?>[collection.size()]);
    }


}
