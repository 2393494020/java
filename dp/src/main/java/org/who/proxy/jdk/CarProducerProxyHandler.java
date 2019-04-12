package org.who.proxy.jdk;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

@Slf4j
public class CarProducerProxyHandler implements InvocationHandler {
    // 被代理对象
    private Object producer;

    public CarProducerProxyHandler(Object producer) {
        this.producer = producer;
    }

    /**
     *
     * @param proxy 代理对象
     * @param method
     * @param args
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        log.info("before---");
        Object result = method.invoke(producer, args);
        log.info("after---");
        return result;
    }
}
