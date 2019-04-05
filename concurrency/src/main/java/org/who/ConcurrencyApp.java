package org.who;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * synchronized jvm auto freed when exception
 * ReentrantLock manual freed
 */
public class ConcurrencyApp {
    private static Logger logger = LoggerFactory.getLogger(ConcurrencyApp.class);
    private int count = 0;
    private AtomicInteger atomCount = new AtomicInteger(0);
    private /*volatile*/ boolean add = true;
    private Lock lock = new ReentrantLock(true);

    private void increase() {
        count++;
    }

    private void increase0() {
        for (int i = 1; i <= 10000; i++) {
            count++;
        }
    }

    private void increase1() {
        for (int i = 1; i <= 10000; i++) {
            synchronized (this) {
                count++;
            }
        }
    }

    private void increase2() {
        try {
            lock.lock();
            for (int i = 1; i <= 10000; i++) {
                count++;

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private void increaseAtom() {
        for (int i = 1; i <= 10000; i++) {
            atomCount.incrementAndGet();
        }
    }

    public static void main(String[] args) {
        final ConcurrencyApp app = new ConcurrencyApp();
        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            threads.add(new Thread(app::increase0));
        }
        threads.forEach(Thread::start);
        // 等待线程结束
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        /*Thread t1 = new Thread(() -> {
            while (app.add)
                app.increase();
        });

        Thread t2 = new Thread(() -> {
            try {
                Thread.sleep(100);
                app.add = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        logger.info("" + app.count);
    }
}
