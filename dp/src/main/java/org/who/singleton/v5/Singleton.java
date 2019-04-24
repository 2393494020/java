package org.who.singleton.v5;

/**
 * 使用枚举除了线程安全和防止反射强行调用构造器之外，还提供了自动序列化机制，防止反序列化的时候创建新的对象。因此，Effective Java 推荐尽可能地使用枚举来实现单例。
 */
public class Singleton {
    private Singleton() {
    }

    public static Singleton getInstance() {
        return SingletonInner.INSTANCE.getSingleton();
    }

    private enum SingletonInner {
        INSTANCE;

        private Singleton singleton;

        // JVM 保证只调用一次
        SingletonInner() {
            singleton = new Singleton();
        }

        public Singleton getSingleton() {
            return singleton;
        }
    }
}
