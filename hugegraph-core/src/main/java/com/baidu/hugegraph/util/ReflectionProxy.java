package com.baidu.hugegraph.util;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.NotSupportException;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReflectionProxy {
    static Class reflectionClazz;
    static Method registerFieldsToFilterMethod;
    static Method registerMethodsToFilterMethod;
    private static final Logger LOG = Log.logger(ReflectionProxy.class);


    static {
        try {
            reflectionClazz = Class.forName("jdk.internal.reflect.Reflection");
        } catch (ClassNotFoundException e) {
            try {
                reflectionClazz = Class.forName("sun.reflect.Reflection");
            } catch (ClassNotFoundException ex) {
                LOG.error("Can't find Reflection class", ex);
            }
        }

        if (reflectionClazz != null) {
            try {
                registerFieldsToFilterMethod =
                reflectionClazz.getMethod("registerFieldsToFilter",
                                          Class.class, String[].class);
            } catch (NoSuchMethodException e) {
                LOG.error("Can't find registerFieldsToFilter method", e);
            }

            try {
                registerMethodsToFilterMethod =
                reflectionClazz.getMethod("registerMethodsToFilter",
                                          Class.class, String[].class);
            } catch (NoSuchMethodException e) {
                LOG.error("Can't find registerMethodsToFilter method", e);
            }
        }
    }

    public static void registerFieldsToFilter(Class<?> containingClass,
                                              String ... fieldNames) {
        if (registerFieldsToFilterMethod == null) {
            throw new NotSupportException(
                      "No support this method 'registerFieldsToFilter'");
        }

        try {
            registerFieldsToFilterMethod.setAccessible(true);
            registerFieldsToFilterMethod.invoke(
            reflectionClazz, containingClass, fieldNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException(
                      "invoke 'registerFieldsToFilter' failed", e);
        }
    }

    public static void registerMethodsToFilter(Class<?> containingClass,
                                               String ... methodNames) {
        if (registerMethodsToFilterMethod == null) {
            throw new NotSupportException(
                      "No support this method 'registerMethodsToFilterMethod'");
        }

        try {
            registerMethodsToFilterMethod.setAccessible(true);
            registerMethodsToFilterMethod.invoke(
            reflectionClazz, containingClass, methodNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException(
                      "invoke 'registerMethodsToFilter' failed", e);
        }
    }

    public static void main(String[] argvs) {
        ReflectionProxy.registerFieldsToFilter(java.lang.Thread.class, "name", "priority", "threadQ", "eetop", "single_step", "daemon", "stillborn", "target", "group", "contextClassLoader", "inheritedAccessControlContext", "threadInitNumber", "threadLocals", "inheritableThreadLocals", "stackSize", "nativeParkEventPointer", "tid", "threadSeqNumber", "threadStatus", "parkBlocker", "blocker", "blockerLock", "EMPTY_STACK_TRACE", "SUBCLASS_IMPLEMENTATION_PERMISSION", "uncaughtExceptionHandler", "defaultUncaughtExceptionHandler", "threadLocalRandomSeed", "threadLocalRandomProbe", "threadLocalRandomSecondarySeed");
        ReflectionProxy.registerMethodsToFilter(java.lang.Thread.class, "exit", "dispatchUncaughtException", "clone", "isInterrupted", "registerNatives", "init", "init", "nextThreadNum", "nextThreadID", "blockedOn", "start0", "isCCLOverridden", "auditSubclass", "dumpThreads", "getThreads", "processQueue", "setPriority0", "stop0", "suspend0", "resume0", "interrupt0", "setNativeName");
        ReflectionProxy.registerFieldsToFilter(java.lang.ThreadLocal.class, "threadLocalHashCode", "nextHashCode", "HASH_INCREMENT");
        ReflectionProxy.registerMethodsToFilter(java.lang.ThreadLocal.class, "access$400", "createInheritedMap", "nextHashCode", "initialValue", "setInitialValue", "getMap", "createMap", "childValue");
        ReflectionProxy.registerMethodsToFilter(java.lang.InheritableThreadLocal.class, "getMap", "createMap", "childValue");
    }
}
