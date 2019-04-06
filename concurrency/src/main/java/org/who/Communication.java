package org.who;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Communication {
    private volatile List<Integer> list = new ArrayList();

    public static void main(String[] args) {
        Object lock = new Object();
        Communication communication = new Communication();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (lock) {
                    System.out.println("t2启动");
                    if (communication.list.size() < 5) {
                        try {
                            // wait 会释放锁
                            lock.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("t2 over");
                    // 唤醒线程2
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
                                // 释放锁,让线程1继续执行,等待线程1执行结束然后唤醒我
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

    public static void main1(String[] args) {
        Communication communication = new Communication();
        CountDownLatch latch = new CountDownLatch(1); // 加一把门栓

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
                        // 门栓减为0,通知t2继续运行,自己也继续运行
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
