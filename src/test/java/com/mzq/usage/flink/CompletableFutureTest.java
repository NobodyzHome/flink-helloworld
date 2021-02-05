package com.mzq.usage.flink;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

public class CompletableFutureTest {

    @Test
    public void testFuture(){
        // Future的核心思想是：主线程想让子线程去做些事情，并且需要获取子线程做完事情的结果，那么子线程就给主线程开了张空头支票Future
        // 主线程拿着这张空头支票，在需要获取子线程结果时，就调用这个Future的get方法获取子线程的执行结果。如果此时子线程已经执行完了，就可以顺序拿到子线程的处理结果。但如果子线程没有执行完，则主线程会被阻塞住，等待子线程执行完成。
        // 而与此同时，子线程在处理完毕后，就把这个空头支票Future设置为已处理完成，此后主线程调用这个Future的get方法，就可以获取子线程的执行结果了

    }

    @Test
    public void testCompleteBeforeAssign() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        // 我们让这个Future变成完成状态，且Future完成后的结果内容是hello
        // 通常是在另外一个线程中，异步执行完操作后，把主线程的CompletableFuture对象设置为已完成状态
        completableFuture.complete("hello");

        // 如果在给这个CompletableFuture安排后续处理前，这个Future已经完成了，那么后续安排的任务会立即执行，只不过是同步执行还是异步执行的问题
        // 在这里我们使用的都是非异步执行的方法，因此会在当前线程中执行后续任务
        // 第一个Future执行完是根据外部调用它的complete方法，第二个Future执行完是根据第一个Future的结果再加调用完成str->str + "world"函数
        CompletableFuture<Void> future = completableFuture.thenApply(str -> {
            return str + "world";
        }).thenApply(str -> {
            return "test:" + str;
        }).thenAccept(str -> {
            System.out.println(str);
        });
        // 执行到这里时，由于是同步方法，因此上面的三个任务已经执行完了
        future.thenRun(() -> System.out.println("execute end"));
        System.out.println("end");
    }

    @Test
    public void testCompleteBeforeAssignAsync() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        completableFuture.complete("hello");

        completableFuture.thenApplyAsync(str -> {
            return str + "world";
        }).thenApplyAsync(str -> {
            return "test:" + str;
        }).thenAcceptAsync(str->{
            System.out.println(str);
        });

    }

    @Test
    public void testCompleteBeforeAssignMix() {

    }
}
