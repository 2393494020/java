package org.who.util;

import java.lang.reflect.Constructor;

public class ReflectionUtils {
    private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

    public static <T> T newInstance(Class<T> theClass) {
        T result;
        try {
            Constructor<T> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
            meth.setAccessible(true);
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}
