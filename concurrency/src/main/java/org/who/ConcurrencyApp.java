package org.who;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyApp {
    private Logger logger = LoggerFactory.getLogger(getClass());
    // private AtomicInteger count = new AtomicInteger(0);
    private int count;

    private /*synchronized*/ void increace() {
        for (int i = 1; i <= 10; i++) {
            count++;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info(Thread.currentThread().getName() + ":" + count);
        }
    }

    public static void main(String[] args) {
        ConcurrencyApp app = new ConcurrencyApp();
        Thread threads[] = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    app.increace();
                }
            });
            threads[i].start();
        }
    }
}
