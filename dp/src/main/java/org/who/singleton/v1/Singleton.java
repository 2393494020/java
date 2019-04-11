package org.who.singleton.v1;

/**
 * 饿汉模式
 * 空间换时间，不存在线程安全问题
 */
public class Singleton {
    private static Singleton instance = new Singleton();

    private Singleton() {
    }

    public static Singleton getInstance() {
        return instance;
    }
}
