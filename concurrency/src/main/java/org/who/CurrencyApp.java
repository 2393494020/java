package org.who;

import java.util.concurrent.atomic.AtomicInteger;

public class CurrencyApp {
    private AtomicInteger count = new AtomicInteger(0);

    private /*synchronized*/ void increace() {
        for (int i = 1; i <= 10; i++) {
            // count.incrementAndGet();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + ":" + count.incrementAndGet());
        }
    }

    public static void main(String[] args) {
        CurrencyApp app = new CurrencyApp();
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
