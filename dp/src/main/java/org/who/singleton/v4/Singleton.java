package org.who.singleton.v4;

/**
 * v1 ~ v4 的共同缺点
 * 都需要额外的工作(Serializable、transient、readResolve())来实现序列化，否则每次反序列化一个序列化的对象实例时都会创建一个新的实例。
 * 可能会有人使用反射强行调用我们的私有构造器（如果要避免这种情况，可以修改构造器，让它在创建第二个实例的时候抛异常）。
 * 本例缺点：外部无法传参
 */
public class Singleton {
    /**
     * 静态内部类属于被动引用
     * 外部类加载时并不需要立即加载内部类，内部类不被加载则不去初始化INSTANCE，故而不占内存
     */
    private static class Holder {
        private static Singleton instance = new Singleton();
    }

    private Singleton() {
        if (Holder.instance != null) throw new IllegalStateException();
    }

    /**
     * 当 getInstance() 方法被调用时，Holder 才在 Singleton 的运行时常量池里，把符号引用替换为直接引用，这时静态对象 instance 也真正被创建
     * 虚拟机会保证一个类的<clinit>()方法在多线程环境中被正确地加锁、同步，如果多个线程同时去初始化一个类，那么只会有一个线程去执行这个类的<clinit>()方法，其他线程都需要阻塞等待，直到活动线程执行<clinit>()方法完毕
     * @return
     */
    public static Singleton getInstance() {
        return Holder.instance;
    }
}
