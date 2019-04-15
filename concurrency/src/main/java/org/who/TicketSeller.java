package org.who;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TicketSeller {
    static Queue<String> ticketQueue = new ConcurrentLinkedQueue<>();

    static {
        for (int i = 1; i <= 10000; i++) {
            // ticketQueue.offer(""); 队列满了不会报错
            ticketQueue.add("票 - " + i);
        }
    }

    public static void main(String[] args) {
        for (int i = 1; i <= 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        String ticket = ticketQueue.poll();
                        if (ticket == null)
                            break;
                        else
                            System.out.println(Thread.currentThread().getName() + " - 卖出 - " + ticket);
                    }
                }
            }).start();
        }
    }
}
