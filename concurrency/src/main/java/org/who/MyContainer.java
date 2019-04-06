package org.who;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MyContainer<T> {
    private static Logger logger = LoggerFactory.getLogger(MyContainer.class);
    private final List<T> container = new ArrayList<>();
    private static final int MAX_SIZE = 3;
    private int count = 0;

    synchronized void put(T ele) {
        while (count == MAX_SIZE) {
            try {
                logger.info(ele.toString() + " want join us, but container full");
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        container.add(ele);
        ++count;
        logger.info("put " + ele.toString() + ",count " + count);
        this.notifyAll();
    }

    synchronized T get() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        T ele = null;
        while (count == 0) {
            try {
                logger.info(Thread.currentThread().getName() + " want take one ele, but container empty");
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ele = container.remove(0);
        count--;
        logger.info(Thread.currentThread().getName() + " take " + ele.toString() + ",count " + count);
        this.notifyAll();
        return ele;
    }

    synchronized int getCount() {
        return count;
    }

    public static void main(String[] args) {
        MyContainer container = new MyContainer();
        List<Thread> producerList = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    String msg = Thread.currentThread().getName();
                    container.put(msg);
                }
            }, "producer-thread-" + i);
            producerList.add(thread);
        }

        producerList.forEach(t -> t.start());

        for (int j = 1; j <= 9; j++) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            new Thread(new Runnable() {
                @Override
                public void run() {
                    Object ele = container.get();
                }
            }, "consumer-thread-" + j).start();
        }
    }
}
