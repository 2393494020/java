package org.who.proxy;


import com.google.common.reflect.Reflection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.who.proxy.jdk.CarProducer;
import org.who.proxy.jdk.CarProducerProxyHandler;
import org.who.proxy.jdk.Producer;
import org.who.rpc.GreetingService;
import org.who.rpc.GreetingServiceImpl;
import org.who.rpc.Server;
import org.who.rpc.ServiceCenter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;

@Slf4j
public class ProxyTest {
    Producer wrap1(Producer carProducer) {
        return (Producer) Proxy.newProxyInstance(Producer.class.getClassLoader(), new Class[]{Producer.class}, new CarProducerProxyHandler(carProducer));
    }

    Producer wrap2(Producer carProducer) {
        return Reflection.newProxy(Producer.class, new CarProducerProxyHandler(carProducer));
    }

    // @Test
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

    class RpcClient<T> {
        T wrapRpcClient(final Class<?> interfaceType, final InetSocketAddress address) {
            return (T) Reflection.newProxy(interfaceType, new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    Socket socket = null;
                    ObjectOutputStream output = null;
                    ObjectInputStream input = null;

                    try {
                        // 2.创建Socket客户端，根据指定地址连接远程服务提供者
                        socket = new Socket();
                        socket.connect(address);

                        // 3.将远程服务调用所需的接口类、方法名、参数列表等编码后发送给服务提供者
                        output = new ObjectOutputStream(socket.getOutputStream());
                        output.writeUTF(interfaceType.getName());
                        output.writeUTF(method.getName());
                        output.writeObject(method.getParameterTypes());
                        output.writeObject(args);

                        // 4.同步阻塞等待服务器返回应答，获取应答后返回
                        input = new ObjectInputStream(socket.getInputStream());
                        return input.readObject();
                    } finally {
                        if (socket != null) socket.close();
                        if (output != null) output.close();
                        if (input != null) input.close();
                    }
                }
            });
        }
    }

    @Test
    public void testRpc() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Server serviceServer = new ServiceCenter(8088);
                    serviceServer.register(GreetingService.class, GreetingServiceImpl.class);
                    serviceServer.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        RpcClient<GreetingService> client = new RpcClient<>();
        GreetingService greetingService = client.wrapRpcClient(GreetingService.class, new InetSocketAddress("localhost", 8088));
        log.info(greetingService.greeting("juno"));
    }
}
