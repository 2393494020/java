package org.who.proxy;


import com.google.common.reflect.Reflection;
import org.junit.Test;
import org.who.proxy.jdk.CarProducer;
import org.who.proxy.jdk.CarProducerProxyHandler;
import org.who.proxy.jdk.Producer;

import java.lang.reflect.Proxy;

public class ProxyTest {
    Producer wrap1(Producer carProducer) {
        return (Producer) Proxy.newProxyInstance(Producer.class.getClassLoader(), new Class[]{Producer.class}, new CarProducerProxyHandler(carProducer));
    }

    Producer wrap2(Producer carProducer) {
        return Reflection.newProxy(Producer.class, new CarProducerProxyHandler(carProducer));
    }

    @Test
    public void testDynamicProxy() {
        Producer carProducer = new CarProducer();
        Producer producer = wrap2(carProducer);
        producer.produce();
    }
    // http://www.importnew.com/23301.html
    // https://my.oschina.net/liughDevelop/blog/1457097
    // http://ifeve.com/part-1-thread-pools/#more-29761
    // http://www.importnew.com/27772.html
    // https://www.jianshu.com/p/e2917b0b9614
    // https://rejoy.iteye.com/blog/1627405
}
