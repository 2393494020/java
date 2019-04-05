package org.who;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Communication {
    private volatile List<Integer> list = new ArrayList();

    public static void main0(String[] args) {
        Object lock = new Object();
        Communication communication = new Communication();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (lock) {
                    System.out.println("t2启动");
                    if (communication.list.size() < 5) {
                        try {
                            lock.wait();
                            // wait 释放锁
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("t2 over");
                    lock.notify();
                }
            }
        });
        t2.start();

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (lock) {
                    for (int i = 1; i <= 10; i++) {
                        communication.list.add(i);
                        System.out.println("t1-" + communication.list.size());
                        if (communication.list.size() == 5) {
                            // notify 不会释放锁
                            lock.notify();
                            try {
                                lock.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        t1.start();
    }

    public static void main(String[] args) {
        Communication communication = new Communication();
        CountDownLatch latch = new CountDownLatch(1);

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("t2 启动");
                if (communication.list.size() < 5) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                System.out.println("t2 over");
            }
        });
        t2.start();

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 1;i <= 10 ;i++) {
                    communication.list.add(i);
                    System.out.println("t1-" + i);
                    if (communication.list.size()==5){
                        latch.countDown();
                    }
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        t1.start();
    }
}
