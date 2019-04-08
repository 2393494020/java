package org.who;

public class ThreadLocalApp {
    static class User {
        private String username = "张三";

        public User() {
        }

        public User(String username) {
            this.username = username;
        }
    }

    private ThreadLocal<User> local = new ThreadLocal();

    public static void main(String[] args) {
        ThreadLocalApp app = new ThreadLocalApp();
        app.local.set(new User());

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                app.local.set(new User("李四"));
                System.out.println(Thread.currentThread().getName() + " " + app.local.get().username);
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                app.local.set(new User("王五"));
                System.out.println(Thread.currentThread().getName() + " " + app.local.get().username);
            }
        });

        thread1.start();

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(app.local.get().username);
    }
}
