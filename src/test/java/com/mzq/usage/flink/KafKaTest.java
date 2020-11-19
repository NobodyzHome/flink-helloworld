package com.mzq.usage.flink;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafKaTest {

    /**
     * consumer订阅分三种：指定topic订阅，指定topic pattern订阅，指定分区订阅。前两种都可以负载均衡，根据组内消费成员的增加或减少，来再均衡每个消费者消费的分区。
     * 指定分区订阅的话，这个消费者就只消费指定的一个或多个分区。注意：一个消费者可以同时订阅多个topic或多个分区。
     * <p>
     * 默认情况下：一个topic的分区只会分发给一个消费组内的一个消费者，不会出现一个分区同时分配给一个消费组内的多个消费者。出现这个情况时，如果这个分区新增了数据，那么
     * 在这个组内的两个消费者都可以消费到。
     */
    @Test
    public void testSubcribe() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "zzGroup");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // 此时只是注册要订阅的topic，并没有真正和broker进行连接
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        /*
            调用poll方法，进行和broker的连接，并拉取数据。poll方法的timeout参数，代表如果分区没有数据时，阻塞主线程的时长。
            在拉取数据时：
            1.首先尝试根据这个消费者的groupId获取每个分区已消费的位点，然后从这个位点之后开始消费。
            2.如果找不到这个消费组对应的已消费位点（例如是一个新的分组），会根据auto.offset.reset的配置，决定这个消费者从哪个位点开始消费数据。如果不设置，那么消费者会从分区的最新位点开始消费，如果设置为earliest，会从分区的起始位点开始消费。
        */
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            String key = consumerRecord.key();
            String value = consumerRecord.value();
            String topic = consumerRecord.topic();
            int partition = consumerRecord.partition();
            long offset = consumerRecord.offset();
            long timestamp = consumerRecord.timestamp();

            System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic, partition, offset, timestamp);
        }

        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }

    @Test
    public void testAssign() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer1");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // 指定消费hello-world的索引为2的分区
        // assign相对于subscribe，稍显不灵活，因为它只消费指定分区。如果当前消费者组增加了新的消费者，新加入的消费者也无法获取当前消费者获取到的分区，进而无法达到负载均衡的目的。
        // 注意：一个consumer在获取分区时，要么使用assign方式，要么使用subscribe方式，不能同时使用这两种方式
        kafkaConsumer.assign(Collections.singleton(new TopicPartition("hello-world", 2)));
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            String key = consumerRecord.key();
            String value = consumerRecord.value();
            String topic = consumerRecord.topic();
            int partition = consumerRecord.partition();
            long offset = consumerRecord.offset();
            long timestamp = consumerRecord.timestamp();

            System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic, partition, offset, timestamp);
        }

        // 从这里可以看到，这个consumer只消费了一个分区
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor("hello-world");
        System.out.println(partitionInfoList);
        // 尽管assign订阅方式是直接订阅指定的分区，这个消费者可以不在任何一个消费组内。但是如果它想提交位点，必须要指定groupId，也就是要为哪个消费组提交位点。
        // 这也就是说，位点的提交，是基于消费组的。kafka会记录一个消费组在一个topic的每个分区所提交的位点。
        kafkaConsumer.commitSync();
        // 注意：在使用完consumer后需要手动的断开连接
        kafkaConsumer.close();
    }

    @Test
    public void testAssignPartitions() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer1");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        /*
            分区是一个逻辑的概念，如果一个topic有四个分区，那么这个topic中的数据都存储在这四个分区中。每一个分区中，必须包含一个leader队列，用于接收写入的消息。
            同时，每个分区中，又可以有多个备份队列，它们会定时的从leader队列中拉取数据。因此，每个分区是由一个leader队列以及零个或多个备份队列组成。
            但在实际存储上，一个分区的leader队列和备份队列不会存储在同一个broker上，以防这个broker挂了以后，这个分区的所有数据就都丢失了。因此，一个分区的leader队列
            和备份队列一般在不同的broker上。
            当调用partitionsFor方法时，会进行与broker的连接，获取当前consumer所消费的分区。PartitionInfo代表一个分区的信息，其中包括leader队列在哪个broker上，
            isr在哪些broker上，ar在哪些broker上。
         */
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor("hello-world");
         /*
            我们可以通过partitionsFor方法，先获取到一个topic的所有分区，然后再通过assign方法，手动订阅这个topic的所有分区。这样，这个消费者一直能处理这个topic在所有分区里的消息。
            注意：使用assign方式订阅一个topic的分区，不会影响使用scribe方式获取分区。例如一个helloGroup的消费者，通过assign方式订阅了hello-world这个topic的所有分区，那么另一个helloGroup的消费者，
            依然可以通过subcribe方式，获取这个topic的分区，不会因为assign方式的消费者已经获取所有分区了，导致subscribe方式的消费者获取不到分区。
         */
        kafkaConsumer.assign(partitionInfoList.stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList()));
        /*
            在使用assign方式获取数据时：
            1.如果没有设置这个消费者的groupId，那么默认它是从分区的最新位点开始消费数据（也就是这个消费者消费不到历史数据，只能消费到此后新增的数据），
            我们也可以设置这个消费者的auto.offset.reset属性，设置为earliest，代表从分区的最开始的位点获取数据。

            2.如果设置了这个消费者的groupId，那么会尝试获取这个消费组对应分区的已消费位点，然后这个消费者从已消费位点之后开始消费
        */
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(120));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            String key = consumerRecord.key();
            String value = consumerRecord.value();
            String topic = consumerRecord.topic();
            int partition = consumerRecord.partition();
            long offset = consumerRecord.offset();
            long timestamp = consumerRecord.timestamp();

            System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic, partition, offset, timestamp);
        }

        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }

    @Test
    public void testAssignment() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer2");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        kafkaConsumer.poll(Duration.ofSeconds(10));

        // 获取当前consumer获取到的分区
        Set<TopicPartition> assignmentPartition = kafkaConsumer.assignment();
        for (TopicPartition topicPartition : assignmentPartition) {
            System.out.printf("topic=%s,position=%d%n", topicPartition.topic(), topicPartition.partition());
        }

        kafkaConsumer.close();
    }

    @Test
    public void testConsumerRecords() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer3");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        // session.timeout.ms用于控制consumer的超时时间。如果consumer在和broker连接后，超过该配置的指定时间还没有再次发起和broker的连接（例如调用consumer的poll方法），那么broker就会从组中删除这个连接，并且发起再均衡操作。
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(20)));

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(12000));
        // 之前我们是直接遍历ConsumerRecords的所有数据，我们也可以使用ConsumerRecords的record方法获取指定分区的所有数据
        // 获取已获取到的数据所属的分区
        Set<TopicPartition> partitions = consumerRecords.partitions();
        for (TopicPartition topicPartition : partitions) {
            // 获取指定分区的数据
            List<ConsumerRecord<String, String>> partitionRecords = consumerRecords.records(topicPartition);
            System.out.printf("topic:%s,partition:%d%n", topicPartition.partition(), topicPartition.partition());
            for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
                String key = consumerRecord.key();
                String value = consumerRecord.value();
                String topic = consumerRecord.topic();
                int partition = consumerRecord.partition();
                long offset = consumerRecord.offset();
                long timestamp = consumerRecord.timestamp();

                System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic, partition, offset, timestamp);
            }
        }

        // 获取分配给这个consumer的分区，都有哪些topic。其实用consumerRecords.partitions()也是可以的，这里只是为了展示consumer的这个方法。
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        List<String> topicList = assignment.stream().map(TopicPartition::topic).distinct().collect(Collectors.toList());

        // ConsumerRecords除了有按照分区获取数据的方法，还有按照topic获取这个topic下所有分区数据的方法
        for (String topic : topicList) {
            System.out.printf("topic:%s%n", topic);
            Iterable<ConsumerRecord<String, String>> topicRecords = consumerRecords.records(topic);
            for (ConsumerRecord<String, String> consumerRecord : topicRecords) {
                String key = consumerRecord.key();
                String value = consumerRecord.value();
                String topic1 = consumerRecord.topic();
                int partition = consumerRecord.partition();
                long offset = consumerRecord.offset();
                long timestamp = consumerRecord.timestamp();

                System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic1, partition, offset, timestamp);
            }
        }

        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }

    @Test
    public void testCommitBasic() {
         /*
            使用kafka consumer的一个基本流程如下：
            1.创建消费者
            2.订阅topic或分区
            3.拉取数据并处理拉取的数据
            4.提交位点
            5.关闭消费者
        */

        // 1.创建消费者
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer3");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(30)));
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 2.订阅topic
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));

        // 3.拉取数据
         /*
            每次拉取数据时，consumer会记录当前消费组拉取到分区的哪个offset了（lastConsumedOffset），但是此时broker还不知道当前分组消费到分区的最新offset。
            我们需要使用位点提交方法来告诉broker，当前分组最新消费到分区的哪个offset了。下次该消费组中的consumer再拉取这个分区时，就知道该从哪个offset开始拉取数据。
            注意：并不是做一次数据拉取就需要进行一次位点提交。可以多次拉取，最后做一次位点提交即可。
        */
        int count = 1;
        List<ConsumerRecords<String, String>> consumerRecordsList = new ArrayList<>(3);
        do {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(120));
            consumerRecordsList.add(consumerRecords);
        } while ((++count) <= 3);

        // 4.提交位点
        // 所谓提交位点，就是把当前consumer拉取的分区的最新位点告诉broker，这样broker就能记录当前消费组针对每个分区，已经消费到哪里了。下次该消费组再有新的consumer加入，也消费该分区的话，就知道应该从哪儿开始消费了
        // 在consumer提交位点时，broker会记录当前消费组针对该分区的LEO（log end offset），LEO的位置是本次提交位点时，拉取到的最后一个消息的offset+1，代表着下次拉取数据时，从LEO所在的位置拉取
        kafkaConsumer.commitSync();

        // 5.关闭消费者
        kafkaConsumer.close();

        /*
            在kafka消费中，最容易发生的异常情况就是消费重复和数据丢失：
            1.消费重复主要是在拉取数据后，提交位点前，处理数据时发生的错误。导致位点没有提交，那么下次拉取数据的话，还是从之前的数据，因此有一部分之前已经被处理的数据又要再处理一遍，进而导致数据被重复处理
            2.数据丢失主要是在拉取数据后，先提交了位点，再处理数据。当数据处理发生错误时，再次拉取数据的话，之前的数据由于已经提交位点了，因此不能再拉取到了，导致之前拉取的数据有一部分没有处理，数据丢失
         */
        for (ConsumerRecords<String, String> consumerRecords : consumerRecordsList) {
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String key = consumerRecord.key();
                String value = consumerRecord.value();
                String topic1 = consumerRecord.topic();
                int partition = consumerRecord.partition();
                long offset = consumerRecord.offset();
                long timestamp = consumerRecord.timestamp();

                System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic1, partition, offset, timestamp);
            }
        }
    }


}