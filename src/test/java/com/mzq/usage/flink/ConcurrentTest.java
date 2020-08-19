package com.mzq.usage.flink;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.Repeat;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@RunWith(SpringRunner.class)
public class ConcurrentTest {

    private static class Product {
        private final AtomicInteger money;

        public Product(int money) {
            this.money = new AtomicInteger(money);
        }

        public int getMoney() {
            return money.get();
        }

        public void setMoney(int money) {
            this.money.set(money);
        }

        public void addMoney() {
            money.addAndGet(1);
        }
    }

    @Configuration
    public static class Config {

    }


    @Test
    @Repeat(30)
    public void test1() {
        Product product = new Product(0);
        int times = 1;
        while (times++ <= 100) {
            int money = product.getMoney();
            money += 1;
            product.setMoney(money);
        }
        System.out.println(product.getMoney());
    }

    @Test
    @Repeat(30)
    public void test2() {
        Product product = new Product(0);
        CountDownLatch latch = new CountDownLatch(100);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        int times = 1;

        while (times++ <= 100) {
            executorService.execute(() -> {
                int money = product.getMoney();
                money += 1;
                product.setMoney(money);

                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(product.getMoney());

    }

    @Test
    @Repeat(30)
    public void test3() {
        Product product = new Product(0);
        CountDownLatch latch = new CountDownLatch(100);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        int times = 1;

        Object mutex = new Object();
        while (times++ <= 100) {
            executorService.execute(() -> {
                synchronized (mutex) {
                    int money = product.getMoney();
                    money += 1;
                    product.setMoney(money);
                }
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(product.getMoney());

    }


    @Test
    @Repeat(30)
    public void test4() {
        Product product = new Product(0);
        CountDownLatch latch = new CountDownLatch(100);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        int times = 1;

        while (times++ <= 100) {
            executorService.execute(() -> {
                product.addMoney();
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(product.getMoney());
    }

    @Test
    @Repeat(30)
    public void test5() {
        Product product = new Product(0);
        CountDownLatch latch = new CountDownLatch(100);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        int times = 1;

        ReentrantLock lock = new ReentrantLock();
        while (times++ <= 100) {
            executorService.execute(() -> {
                lock.lock();
                int money = product.getMoney();
                money += 1;
                product.setMoney(money);
                lock.unlock();

                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(product.getMoney());
    }

    @Test
    @Repeat(30)
    public void test6() {
        Product product = new Product(0);
        CountDownLatch latch = new CountDownLatch(100);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        int times = 1;

        while (times++ <= 100) {
            executorService.execute(() -> {
                product.addMoney();
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(product.getMoney());
    }

    @Test
    public void test7() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Object mutext1 = new Object();
        Object mutext2 = new Object();

        executorService.execute(() -> {
            synchronized (mutext1) {
                try {
                    System.out.println("i get lock 1");
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (mutext2) {
                    System.out.println("I'll never arrive this 1");
                }
            }
        });

        executorService.execute(() -> {
            synchronized (mutext2) {
                try {
                    System.out.println("i get lock 2");
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (mutext1) {
                    System.out.println("I'll never arrive this 2");
                }
            }
        });

        try {
            Thread.sleep(TimeUnit.MINUTES.toMillis(5));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
