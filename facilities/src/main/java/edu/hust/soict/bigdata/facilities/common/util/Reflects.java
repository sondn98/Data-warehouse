package edu.hust.soict.bigdata.facilities.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;

/**
 * Các helper method làm việc với Reflections
 *
 *
 */
@SuppressWarnings("unchecked")
public class Reflects {

    private static final ClassLoader mainCl = Reflects.class.getClassLoader();
    private static final Logger logger = LoggerFactory.getLogger(Reflects.class);

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getClassInstance(T object) {
        return (Class<T>) object.getClass();
    }

    public static <T> T newInstance(Class<T> c) {
        try {
            return c.newInstance();
        } catch (IllegalAccessException | InstantiationException | NullPointerException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newInstance(String className) {
        try {
            return (T) mainCl.loadClass(className).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newInstance(String className, Class<?>[] parameterTypes, Object... parameters) {
        try {
            return (T) mainCl.loadClass(className).getConstructor(parameterTypes).newInstance(parameters);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Class[] getClass(List<String> clazzNames) {
        List<Class> classes = new LinkedList<>();
        for(String name : clazzNames){
            Class c = null;
            try {
                c = mainCl.loadClass(name);
            } catch (ClassNotFoundException e) {
                logger.error("Ignoring class " + name + " due to ClassNotFoundException. See full stacktrace below", e);
            }
            if(c != null)
                classes.add(c);
        }

        return classes.toArray(new Class[0]);
    }

    public static <T> T newInstance(Class<T> clazz, Class<?>[] parameterTypes, Object... parameters) {
        try {
            return clazz.getConstructor(parameterTypes).newInstance(parameters);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
