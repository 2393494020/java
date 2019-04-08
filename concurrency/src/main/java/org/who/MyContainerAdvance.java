package org.who;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyContainerAdvance<T> {
    private static Logger logger = LoggerFactory.getLogger(MyContainerAdvance.class);
    private final List<T> container = new ArrayList<>();
    private static final int MAX_SIZE = 3;
    private int count = 0;
    private Lock lock = new ReentrantLock(false);
    private Condition producerCondition = lock.newCondition();
    private Condition consumerCondition = lock.newCondition();

    void put(T ele) {
        try {
            lock.lock();
            while (count == MAX_SIZE) {
                logger.info(ele.toString() + " want join us, but container full");
                producerCondition.await();
            }
            container.add(ele);
            ++count;
            logger.info("put " + ele.toString() + ",count " + count);
            consumerCondition.signalAll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    T get() {
        T ele = null;
        try {
            lock.lock();

            Thread.sleep(200);

            while (count == 0) {
                logger.info(Thread.currentThread().getName() + " want take one ele, but container empty");
                consumerCondition.await();
            }
            ele = container.remove(0);
            count--;
            logger.info(Thread.currentThread().getName() + " take " + ele.toString() + ",count " + count);
            producerCondition.signalAll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return ele;
    }

    synchronized int getCount() {
        return count;
    }

    public static void main(String[] args) {
        MyContainerAdvance<String> container = new MyContainerAdvance();
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

        for (int j = 1; j <= 8; j++) {
            try {
                Thread.sleep(500);
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
