package org.who;

public class DeadLock {
    private static void method1() {
        synchronized (String.class) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("i need Integer.class");

            synchronized (Integer.class) {

            }
        }
    }

    private static void method2() {
        synchronized (Integer.class) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("i need String.class");

            synchronized (String.class) {

            }
        }
    }

    public static void main(String[] args) {
        Thread thread1 = new Thread(DeadLock::method1);
        Thread thread2 = new Thread(DeadLock::method2);

        thread1.start();
        thread2.start();
    }
}
