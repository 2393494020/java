package org.who.singleton.v3;

/**
 * 双重锁懒汉模式
 * DCL (Double Check Lock)
 */
public class Singleton {
    // volatile 作用：
    // 1. 保证其可见
    // 2. 禁止jvm对其进行指令重排序优化 jdk >= 1.5
    private volatile static Singleton instance;

    private Singleton() {
    }

    public static Singleton getInstance() {
        if (instance == null) {
            synchronized (Singleton.class) {
                if (instance == null) {
                    // 非原子操作
                    // 1.在堆内存开辟内存空间。
                    // 2.在堆内存中实例化Singleton里面的各个参数。
                    // 3.把对象指向堆内存空间。
                    // 由于jvm存在乱序执行功能，所以可能在2还没执行时就先执行了3，如果此时再被切换到线程B上，由于执行了3，instance 已经非空了，会被直接拿出来用，这样的话，就会出现异常
                    instance = new Singleton();
                }
            }
        }

        return instance;
    }
}
