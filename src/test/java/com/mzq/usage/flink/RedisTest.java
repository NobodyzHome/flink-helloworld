package com.mzq.usage.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mzq.usage.flink.domain.BdWaybillOrder;
import com.mzq.usage.flink.util.GenerateDomainUtils;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
public class RedisTest {

    /**
     * 单机模式下访问redis的uri格式
     * Syntax: redis://[password@]host[:port][/databaseNumber]
     * Syntax: redis://[username:password@]host[:port][/databaseNumber]
     * <p>
     * 集群模式下访问redis的uri格式
     * Syntax: redis://[password@]host[:port]
     * Syntax: redis://[username:password@]host[:port]
     * <p>
     * 可以看到，主要区别就是单机模式可以给定database来指定访问的db，而集群模式不可以。
     * 因为redis的设计中，针对集群模式，只能使用数据库0，以下是redis的说明：
     * When using Redis Cluster, the SELECT command cannot be used, since Redis Cluster only supports database zero.
     * In the case of a Redis Cluster, having multiple databases would be useless and an unnecessary source of complexity.
     * Commands operating atomically on a single database would not be possible with the Redis Cluster design and goals.
     * <p>
     * 单机模式下redis使用多个数据库的目的：
     * 这是为了解决，在开发和测试时，不同的应用程序向同一个redis写入时，有可能写相同的key，造成key冲突的问题。通过使用不同的database，不同的应用程序往同一个redis实例的不同的database里写，就避免了key冲突的问题。
     * 但database的使用仅限于单机模式，也就是仅仅应该只是用于开发和测试阶段。
     * Selectable Redis databases are a form of namespacing: all databases are still persisted in the same RDB / AOF file.
     * However different databases can have keys with the same name, and commands like FLUSHDB, SWAPDB or RANDOMKEY work on specific databases.
     * <p>
     * redis官方建议，在实际生产中，应该是一个应用程序使用一个单独的redis库，而不是多个不相干的应用使用同一个redis库。也就是说，不同的应用程序使用同一个
     * redis实例的不同database，在实际生产应用中，是不建议的。
     * In practical terms, Redis databases should be used to separate different keys belonging to the same application (if needed)
     * , and not to use a single Redis instance for multiple unrelated applications.
     */
    @Test
    public void testSyncApi() {
        RedisClient redisClient = RedisClient.create("redis://localhost:6379/3");
        // 创建一个redis连接
        StatefulRedisConnection<String, String> connect = redisClient.connect();
        // 代表要发送同步命令。实际上Lettuce是一个非阻塞的异步处理客户端，它发送的每一个命令都是异步的。它提供的所谓的同步接口的实现原理是异步发送一个命令，然后阻塞用户线程，直接接到服务器的响应并且解析完成响应内容，解除用户线程的阻塞并将响应结果返回给用户线程
        // Lettuce is a non-blocking and asynchronous client. It provides a synchronous API to achieve a blocking behavior on a per-Thread basis to create await (synchronize) a command response
        RedisCommands<String, String> sync = connect.sync();
        String set = sync.get("JD0000007782");
        System.out.println(set);
        // Close the connection when you’re done. This happens usually at the very end of your application. Connections are designed to be long-lived.
        connect.close();
        // Shut down the client instance to free threads and resources. This happens usually at the very end of your application.
        redisClient.shutdown();
    }

    public static class BdWaybillCodec implements RedisCodec<String, BdWaybillOrder> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        static {
            OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @Override
        public String decodeKey(ByteBuffer bytes) {
            byte[] target = new byte[bytes.remaining()];
            bytes.get(target);
            return new String(target);
        }

        @Override
        public BdWaybillOrder decodeValue(ByteBuffer bytes) {
            try {
                byte[] target = new byte[bytes.remaining()];
                bytes.get(target);
                return OBJECT_MAPPER.readValue(target, BdWaybillOrder.class);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public ByteBuffer encodeKey(String key) {
            return ByteBuffer.wrap(key.getBytes());
        }

        @Override
        public ByteBuffer encodeValue(BdWaybillOrder value) {
            try {
                return ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsBytes(value));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Test
    public void testCodec() {
        RedisClient redisClient = RedisClient.create("redis://localhost:6379/3");
        // BdWaybillCodec给RedisCodec类的范型K,V的赋值是String、BdWaybillOrder，代表它能处理String类型的key以及BdWaybillOrder类型的value，
        // 这里说的处理是把程序里的类型对象序列化成byte[]发送给redis服务器，以及把redis服务器返回的byte[]反序列化成程序里的类型对象
        // 也代表了，要发送的命令中，key和value的类型
        StatefulRedisConnection<String, BdWaybillOrder> connection = redisClient.connect(new BdWaybillCodec());
        RedisCommands<String, BdWaybillOrder> syncCommands = connection.sync();
        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(10);
        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            syncCommands.set(bdWaybillOrder.getWaybillCode(), bdWaybillOrder);
        }
        List<String> keys = syncCommands.keys("*");
        for (String key : keys) {
            BdWaybillOrder bdWaybillOrder = syncCommands.get(key);
            System.out.println(bdWaybillOrder);
            syncCommands.expire(key, 30);
        }
        connection.close();
    }

    @Test
    public void testAsyncApi() {
        RedisClient redisClient = RedisClient.create("redis://localhost:6379");
        StatefulRedisConnection<String, BdWaybillOrder> connection = redisClient.connect(new BdWaybillCodec());
        RedisAsyncCommands<String, BdWaybillOrder> asyncCommands = connection.async();
        BdWaybillOrder bdWaybillOrder = GenerateDomainUtils.generateBdWaybillOrder();
        // 异步操作不会阻塞用户线程
        // Asynchronicity permits other processing to continue before the transmission has finished and the response of the transmission is processed.
        // This means, in the context of Lettuce and especially Redis, that multiple commands can be issued serially without the need of waiting to finish the preceding command. This mode of operation is also known as Pipelining.
        RedisFuture<String> redisFuture = asyncCommands.set(bdWaybillOrder.getWaybillCode(), bdWaybillOrder);
        // 不用等到命令的响应结果，用户线程就可以执行到这步，用户线程后续只需要给出响应结果出来后的操作内容即可
        redisFuture.handleAsync((result, throwable) -> Objects.nonNull(throwable) ? throwable.toString()
                : String.format("%s:%s", bdWaybillOrder.getWaybillCode(), result))
                .thenAccept(str -> {
                    System.out.println(str);
                });
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        connection.close();
        redisClient.shutdown();
    }

    public static class BdWaybillOrderSerializingCodec implements RedisCodec<String, BdWaybillOrder> {
        @Override
        public String decodeKey(ByteBuffer bytes) {
            byte[] contents = new byte[bytes.remaining()];
            bytes.get(contents);
            return new String(contents);
        }

        @Override
        public BdWaybillOrder decodeValue(ByteBuffer bytes) {
            byte[] contents = new byte[bytes.remaining()];
            bytes.get(contents);
            try {
                ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(contents));
                return (BdWaybillOrder) objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        public ByteBuffer encodeKey(String key) {
            return ByteBuffer.wrap(key.getBytes());
        }

        @Override
        public ByteBuffer encodeValue(BdWaybillOrder value) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
                objectOutputStream.writeObject(value);
                objectOutputStream.flush();
                byte[] bytes = out.toByteArray();
                return ByteBuffer.wrap(bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    /**
     * 总结：
     * 1.同步操作适用于后续操作需要依赖同步操作的结果来决定如何处理（例如查询某一个key是否存在，针对存在和不存在有不同的处理逻辑），吞吐量较低
     * 2.异步操作适用于后续操作不依赖redis的操作结果（例如往redis里写一个key，但是后续操作不依赖写没写成功），吞吐量较高
     * 3.pipeline操作适用于要进行多次和redis的交互，并且后续操作不依赖于和redis的操作结果（例如要往多个key写，但后续操作不依赖于这多个key的写入结果），吞吐量应该是最高的
     */
    @Test
    public void testPipeline() {
        /*
        * 异步操作解决了同步操作的阻塞用户线程的问题，但依然有一个问题就是每次发起异步操作，客户端都会马上把这条命令发送到redis服务器。
        * 假设我的处理程序要操作10次redis，那么就要和redis产生10次交互，而每次产生交互所需要的I/O、网络等开销都是不小的。
        * 所以我们可以关闭client的AutoFlushCommands功能，这样每次执行异步操作后，就不会立即和redis产生交互了，而是等用户手动调用
        * flushCommands方法，来使用一次redis的交互把积累的请求都发送出去。这种方式的底层实现就是使用netty的pipeline方式
        * The normal operation mode of Lettuce is to flush every command which means, that every command is written to the transport after it was issued.
        * A flush is an expensive system call and impacts performance. Batching, disabling auto-flushing, can be used under certain conditions and is recommended if:
            You perform multiple calls to Redis and you’re not depending immediately on the result of the call
            You’re bulk-importing
        *
        * pipeline仅适用于异步操作，因为同步操作发送的每一条命令都必须等到响应结果才会让客户线程继续执行，没有办法能让你一次发多条指令
        * Controlling the flush behavior is only available on the async API. The sync API emulates blocking calls and as soon as you invoke a command, you’re no longer able to interact with the connection until the blocking call ends.
        */

        RedisClient redisClient = RedisClient.create("redis://localhost:6379/5");
        StatefulRedisConnection<String, BdWaybillOrder> statefulRedisConnection = redisClient.connect(new BdWaybillOrderSerializingCodec());
        RedisAsyncCommands<String, BdWaybillOrder> asyncCommands = statefulRedisConnection.async();
        // disable auto-flushing
        asyncCommands.setAutoFlushCommands(false);

        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(30);
        // perform a series of independent calls
        List<RedisFuture> futures = new ArrayList<>(bdWaybillOrders.size());
        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            RedisFuture<String> redisFuture = asyncCommands.setex(bdWaybillOrder.getWaybillCode(), 50, bdWaybillOrder);
            futures.add(redisFuture);
        }
        // write all commands to the transport layer
        asyncCommands.flushCommands();

        LettuceFutures.awaitAll(Duration.ofSeconds(30), futures.toArray(new RedisFuture[0]));

        for (String key : bdWaybillOrders.stream().map(BdWaybillOrder::getWaybillCode).collect(Collectors.toList())) {
            RedisFuture<BdWaybillOrder> getFuture = asyncCommands.get(key);
            getFuture.thenAccept(bdWaybillOrder -> {
                System.out.println(bdWaybillOrder);
            });
        }
        asyncCommands.flushCommands();

        statefulRedisConnection.close();
        redisClient.shutdown();
    }

    @Test
    public void test1() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        completableFuture.complete("hello");
        completableFuture.thenApply(str -> {
            return "test" + str;
        }).thenApply(str -> {
            return "hello" + str;
        }).thenAcceptAsync(str -> {
            System.out.println(str);
        });
//        new Thread(() -> {
//            completableFuture.complete("hoho");
//        }).start();

        String a = "a";
    }
}
