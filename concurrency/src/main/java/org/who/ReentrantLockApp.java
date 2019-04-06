package org.who;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantLockApp implements Runnable {
    private Lock lock = new ReentrantLock(true); // 公平锁

    @Override
    public void run() {
        for (int i = 1; i <= 10; i++) {
            try {
                lock.lock();
                // lock.lockInterruptibly(); // 可以被其他线程打断
                // boolean flag = lock.tryLock(); // 可尝试申请锁
                TimeUnit.SECONDS.sleep(1);
                System.out.println(Thread.currentThread().getName());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) {
        ReentrantLockApp app = new ReentrantLockApp();
        Thread t1 = new Thread(app);
        Thread t2 = new Thread(app);

        t1.start();
        t2.start();
    }
}
