package org.who;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyApp {
    private static Logger logger = LoggerFactory.getLogger(ConcurrencyApp.class);
    private AtomicInteger atomCount = new AtomicInteger(0);
    private volatile int count = 0;
    private volatile boolean add = true;
    // private ReentrantLock lock = new ReentrantLock(true);

    private int getCount() {
        return count;
    }

    private void increase() {
//        count++;
//        logger.info(Thread.currentThread().getName() + ":" + count);
        for (int i = 1; i <= 10000; i++) {
            count++;
//            try {
//                Thread.sleep(5);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            // logger.info(Thread.currentThread().getName() + ":" + count);
        }
    }

    private void increaseAtom() {
        for (int i = 1; i <= 10000; i++) {
            atomCount.incrementAndGet();
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            logger.info((Thread.currentThread().getName() + ":" + atomCount.incrementAndGet()));
        }
    }

    public static void main(String[] args) {
        final ConcurrencyApp app = new ConcurrencyApp();
        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            threads.add(new Thread(app::increaseAtom));
        }
        threads.forEach(Thread::start);

//        new Thread(() -> {
//            while (app.add)
//                app.increase();
//        }).start();
//
//        new Thread(() -> {
//            try {
//                Thread.sleep(50);
//                app.add = false;
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }).start();
//
//        try {
//            Thread.sleep(45);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        logger.info(Thread.currentThread().getName() + ":" + app.atomCount.get());
    }
}
