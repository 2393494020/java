package org.who;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyApp {
    private static Logger logger = LoggerFactory.getLogger(ConcurrencyApp.class);
    private AtomicInteger atomCount = new AtomicInteger(0);
    private int count = 0;
    private volatile boolean add = true;
    // private ReentrantLock lock = new ReentrantLock(true);

    private int getCount() {
        return count;
    }

    private void increase() {
        count++;
        logger.info(Thread.currentThread().getName() + ":" + count);
//        for (int i = 1; i <= 10; i++) {
//            count++;
//            try {
//                Thread.sleep(5);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            logger.info(Thread.currentThread().getName() + ":" + count);
//        }
    }

    private void increaseAtom() {
        for (int i = 1; i <= 10; i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info((Thread.currentThread().getName() + ":" + atomCount.incrementAndGet()));
        }
    }

    public static void main(String[] args) {
        final ConcurrencyApp app = new ConcurrencyApp();
//        Thread[] threads = new Thread[100];
//        for (int i = 0; i < threads.length; i++) {
//            threads[i] = new Thread(app::increase);
//            threads[i].start();
//        }

        new Thread(() -> {
            while (app.add)
                app.increase();
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(50);
                app.add = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        try {
            Thread.sleep(45);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.info(Thread.currentThread().getName() + ":" + app.getCount());
    }
}
