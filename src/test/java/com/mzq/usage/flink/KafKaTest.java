package com.mzq.usage.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
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

    @Test
    public void testCommitByPartition() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer4");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(20)));
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 获取这个topic有几个分区，然后只订阅这个topic的前三个分区
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("hello-world");
        List<TopicPartition> partitionConsumed = new ArrayList<>(5);
        for (int i = 0; i < partitionInfos.size(); i++) {
            if (i <= 2) {
                PartitionInfo partitionInfo = partitionInfos.get(i);
                partitionConsumed.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        kafkaConsumer.assign(partitionConsumed);

        // 在上面，我们使用commitSync()方法，来一次性把所有拉取到数据的分区的位点都提交了。commitSync方法也有更细粒度的位点提交方式，可以针对分区来提交位点，仅提交一个或几个分区的位点
        // 拉取数据，然后每处理一条数据就提交一条数据。虽然每处理一条数据后就提交会非常安全，几乎不会造成数据丢失和重复处理的问题，但是性能很差，每次提交位点都要和broker交互。这里只是展示，生产环境下是不能这么做的。
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            Map<TopicPartition, OffsetAndMetadata> toCommit = Collections.singletonMap(new TopicPartition(consumerRecord.topic()
                    , consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1));
            kafkaConsumer.commitSync(toCommit);
        }

        kafkaConsumer.close();
    }

    @Test
    public void testCommitByPartition1() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer4");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(20)));

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));

        // 连续拉取三次数据
        int pollCount = 3;
        List<ConsumerRecords<String, String>> consumerRecordsList = new ArrayList<>(pollCount);
        for (int i = 1; i <= pollCount; i++) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(120));
            consumerRecordsList.add(consumerRecords);
        }

        // 获取拉取的这批数据里，都有哪些分区
        Set<TopicPartition> topicPartitions = new HashSet<>();
        consumerRecordsList.stream().map(ConsumerRecords::partitions).forEach(topicPartitions::addAll);

        // 遍历所有分区
        for (TopicPartition topicPartition : topicPartitions) {
            long lastConsumedOffset = 0;
            // 遍历每一次拉取的数据中，该分区的数据
            for (ConsumerRecords<String, String> consumerRecords : consumerRecordsList) {
                List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    String key = consumerRecord.key();
                    String value = consumerRecord.value();
                    String topic1 = consumerRecord.topic();
                    int partition = consumerRecord.partition();
                    long offset = consumerRecord.offset();
                    long timestamp = consumerRecord.timestamp();
                    lastConsumedOffset = offset;

                    System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic1, partition, offset, timestamp);
                }
            }
            System.out.printf("topic=%s,partition=%d,lastConsumedOffset=%d%n", topicPartition.topic(), topicPartition.partition(), lastConsumedOffset);
            // 处理完该分区这三次拉取的数据后，提交一次位点
            /*
                现在的情况是：
                1.如果处理完所有分区的数据，然后一次性提交所有分区的位点。那么假设处理完A分区的数据后，在处理B分区的数据时发生异常，导致所有分区的位点都没有提交。这样下次再拉取数据时，A分区已处理的数据还能拉到，导致A分区的数据被重复处理。
                2.如果每处理一条数据就提交一次位点，确实可以避免重复消费和数据丢失的问题，但是由于和broker交互太频繁，导致程序性能下降

                结合这两种情况，我们做一个折中的处理。不一次提交所有分区的数据，也不一条数据处理完就提交一次位点，我们一次处理一个分区的数据，然后这个分区数据处理完之后，提交这个分区的位点。
             */
            kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastConsumedOffset + 1)));
        }
    }

    @Test
    public void testCommittedAndPosition() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer3");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(20)));
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("hello-world");
        PartitionInfo partitionConsumed = partitionInfos.get(1);
        TopicPartition partition = new TopicPartition(partitionConsumed.topic(), partitionConsumed.partition());
        kafkaConsumer.assign(Collections.singleton(partition));

        for (int i = 1; i <= 3; i++) {
            kafkaConsumer.poll(Duration.ofSeconds(10));
            // position方法用于获取指定分区最近一次poll后，分区的LEO，这个位置可能还没有提交给broker。这个数据是存储在consumer中的，所以调用该方法有可能不需要和broker进行交互。
            // 除非调用该方法时，还没有调用poll方法，那么当前consumer不知道要从哪儿拉取数据，只能和broker交互，获取上一次commit的LEO
            long position = kafkaConsumer.position(partition);
            // committed方法用于获取指定分区最后一次commit后的LEO，这个数据是存储在broker中的，所以这个方法需要和broker进行交互
            Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(Collections.singleton(partition));
            System.out.printf("position:%d,committed:%s%n", position, committed.get(partition).offset());

            kafkaConsumer.commitSync();
        }

        kafkaConsumer.close();
    }

    /**
     * 使用seek方法，可以使consumer不只从最近提交的offset开始消费，可以把拉取数据的位点调整到任意位置。
     * 解决了如果有人错误提交了该分组的对应分区的位点，也可以通过seek方法来重新消费已被跳过的数据。
     * <p>
     * 注意：使用seek方法的前提一定要是broker已经给当前consumer分配分区了
     */
    @Test
    public void testSeekBasic() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer4");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(20).toMillis()));
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "4");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor("hello-world");
        Set<TopicPartition> partitions = partitionInfoList.stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toSet());

        // 获取当前group对指定分区最近一次提交的offset。上一次提交的offset一般是上一次拉取的数据的最后一个元素的offset+1
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(partitions);
        // 获取指定分区实际的offset，这个值是分区的最后一个数据的offset+1
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions);

        // 获取当前topic的所有分区中，最新offset和committed offset差距最大的分区
        Long maxSub = null;
        TopicPartition partitionConsumed = null;
        for (TopicPartition topicPartition : partitions) {
            long committedOffset = committed.get(topicPartition).offset();
            Long endOffset = endOffsets.get(topicPartition);
            long sub = endOffset - committedOffset;

            if (Objects.isNull(maxSub)) {
                maxSub = sub;
                partitionConsumed = topicPartition;
            } else if (maxSub.compareTo(sub) < 0) {
                maxSub = sub;
                partitionConsumed = topicPartition;
            }
        }

        if (Objects.isNull(partitionConsumed)) {
            kafkaConsumer.close();
            return;
        }

        // 在使用assign时有一个非常要注意的点：如果有其他consumer，通过subscribe方式已经获取了指定的分区了，那么通过assign方式也获取该分区的话。在提交位点时会报错（poll可以正常拉取数据），服务器响应UNJOINED（the client is not part of a group），
        // 说明当前consumer没有加入到这个组里，尽管我配置了这个consumer的groupId。
        kafkaConsumer.assign(Collections.singleton(partitionConsumed));
        // 在这里poll，并不是想要拉取数据，而是想要broker给当前consumer分配分区
        kafkaConsumer.poll(Duration.ofSeconds(1));

        long committedOffset = committed.get(partitionConsumed).offset(), seekOffset = committedOffset - 15 < 0 ? 0 : committedOffset - 15;
        // 当前consumer重新设置该分区拉取数据的位点。注意：是从该offset拉取，而不是从offset+1的位置拉取数据。例如seek中传入的offset是10，那么下次poll时，是从offset=10的元素开始拉取，而不是从offset=11的元素开始拉取
        // 注意：seek方法必须要在broker已给当前consumer分配分区后才可以使用
        kafkaConsumer.seek(partitionConsumed, seekOffset);

        do {
            // 拉取数据，此时当前consumer会从seek方法指定的offset开始拉取数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            long lastOffset = 0;
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords.records(partitionConsumed)) {
                String key = consumerRecord.key();
                String value = consumerRecord.value();
                String topic1 = consumerRecord.topic();
                int partition = consumerRecord.partition();
                long offset = consumerRecord.offset();
                long timestamp = consumerRecord.timestamp();
                lastOffset = offset;
                System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic1, partition, offset, timestamp);
            }

             /*
                注意：在提交位点时，kafka并未限制分区提交的offset要比该分区当前已提交的offset要大。
                例如：针对消费组helloGroup，broker记录的分区A已提交的offset是20，helloGroup组中的一个consumer是可以把分区A的offset提交到5的。那么此时broker记录的helloGroup针对分区A的offset就是5了。
                那么这个组的consumer，再次poll分区A的数据时，就是从offset=5的元素开始拉取了。相当于offset在5到20之间的数据，又重新被消费一遍。这种情况经常发生在重置位点时。
            */
            kafkaConsumer.commitSync(Collections.singletonMap(partitionConsumed, new OffsetAndMetadata(lastOffset + 1)));
            // position方法用于获取当前consumer最近一次poll数据时的offset，这个值是最近一次poll到的数据的最后一个元素的offset+1
            // 如果在poll方法前调用该方法，那么会和broker交互，获取当前分区最近一次提交的offset
        } while (kafkaConsumer.position(partitionConsumed) < endOffsets.get(partitionConsumed));
    }

    @Test
    public void testSeek() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer3");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(20)));
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(30)));
        // 设置该属性，让消费者每次只拉取5条数据。注意：这个控制的是consumer每次拉取的总数，而不是每个分区拉取的总数。因此consumer每次poll，最多只能拉取5条数据
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor("hello-world");
        List<TopicPartition> partitionList = partitionInfoList.stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList());

        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitionList);
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(new HashSet<>(partitionList));

        Set<TopicPartition> partitionConsumed = new HashSet<>();
        for (TopicPartition topicPartition : partitionList) {
            Long endOffset = endOffsets.get(topicPartition);
            OffsetAndMetadata committedOffset = committed.get(topicPartition);

            if (endOffset - committedOffset.offset() > 10) {
                partitionConsumed.add(topicPartition);
            }
        }

        kafkaConsumer.assign(partitionConsumed);
        kafkaConsumer.poll(Duration.ofSeconds(1));

        for (TopicPartition topicPartition : partitionConsumed) {
            kafkaConsumer.seek(topicPartition, 2);
        }

        // 拉取数据，一直拉取到拉取的offset不比分区end offset小为止
        List<ConsumerRecords<String, String>> consumerRecordsList = new ArrayList<>();
        do {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            consumerRecordsList.add(consumerRecords);

            boolean stopPoll = true;
            for (TopicPartition topicPartition : partitionConsumed) {
                long position = kafkaConsumer.position(topicPartition);
                Long endOffset = endOffsets.get(topicPartition);

                if (position < endOffset) {
                    stopPoll = false;
                    break;
                }
            }
            if (stopPoll) {
                break;
            }
        } while (true);

        for (TopicPartition topicPartition : partitionConsumed) {
            long lastOffset = 0;
            for (ConsumerRecords<String, String> consumerRecords : consumerRecordsList) {
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords.records(topicPartition)) {
                    String key = consumerRecord.key();
                    String value = consumerRecord.value();
                    String topic1 = consumerRecord.topic();
                    int partition = consumerRecord.partition();
                    long offset = consumerRecord.offset();
                    long timestamp = consumerRecord.timestamp();
                    lastOffset = offset;

                    System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic1, partition, offset, timestamp);
                }
            }

            kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)));
        }

        kafkaConsumer.close();
    }

    @Test
    public void seekExceedEnd() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer4");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        kafkaConsumer.poll(Duration.ofSeconds(2));

        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        TopicPartition topicPartition = assignment.iterator().next();
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(Collections.singleton(topicPartition));
        kafkaConsumer.seek(topicPartition, endOffsets.get(topicPartition) + 1);

        /*
          如果使用seek方法，将拉取的位点设置为比分区实际的位点还要大，那么在拉取数据时，会触发拉取该分区的位点重定位。此时会根据consumer中auto.offset.reset属性的配置，决定如何重定位到应拉取的位点。
          如果auto.offset.reset设置为earliest，那么重定位的位点就是分区实际开始的offset。如果为latest，那么重定位的位点就是分区最新的offset

          Fetch position FetchPosition{offset=37, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1001 rack: null)], epoch=0}} is out of range for partition hello-world-0
          Resetting offset for partition hello-world-0 to offset 0
        */
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            String key = consumerRecord.key();
            String value = consumerRecord.value();
            String topic1 = consumerRecord.topic();
            int partition = consumerRecord.partition();
            long offset = consumerRecord.offset();
            long timestamp = consumerRecord.timestamp();

            System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic1, partition, offset, timestamp);
        }
        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }

    @Test
    public void seekAdvanced() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer5");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        TopicPartition topicPartition1 = new TopicPartition("hello-world", 0);
        TopicPartition topicPartition2 = new TopicPartition("hello-world", 1);
        Set<TopicPartition> partitionSet = new HashSet<>(2);
        partitionSet.add(topicPartition1);
        partitionSet.add(topicPartition2);

        kafkaConsumer.assign(partitionSet);
        kafkaConsumer.poll(Duration.ofSeconds(1));

//      Map<TopicPartition, Long> beginningOffsets = kafkaConsumer.beginningOffsets(Collections.singleton(topicPartition1));
//      kafkaConsumer.seek(topicPartition1, beginningOffsets.get(topicPartition1));
        // 把指定分区的拉取点设置成该分区起始的offset，这行和上面两行的作用相同
        kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition1));

//      Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(Collections.singleton(topicPartition2));
//      kafkaConsumer.seek(topicPartition2, endOffsets.get(topicPartition2));
        // 把指定分区的拉取点设置成该分区的最新的offset，这行和上面两行的作用相同
        kafkaConsumer.seekToEnd(Collections.singleton(topicPartition2));

        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
        for (TopicPartition topicPartition : partitionSet) {
            List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
            if (!records.isEmpty()) {
                long lastOffset = 0;
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    String key = consumerRecord.key();
                    String value = consumerRecord.value();
                    String topic1 = consumerRecord.topic();
                    int partition = consumerRecord.partition();
                    long offset = consumerRecord.offset();
                    long timestamp = consumerRecord.timestamp();
                    lastOffset = offset;

                    System.out.printf("key=%s,value=%s,topic=%s,partition=%d,offset=%d,timestamp=%d%n", key, value, topic1, partition, offset, timestamp);
                }
                kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }

        kafkaConsumer.close();
    }

    /**
     * 关于seek方法的总结：
     * 1.在使用seek方法前，一定要先使用poll方法，让broker给该consumer分配分区
     * 2.如果使用seek设置的分区拉取位置超过了分区最新的offset，那么会触发重置分区拉取位置。此时根据consumer的auto.offset.reset配置决定重置后的拉取位置
     * 3.可以使用consumer的beginningOffsets方法获取分区的起始offset，然后使用seek方法将分区的拉取位置调整至分区的起始位置（相当于seekToBeginning方法）
     * 4.可以使用consumer的endOffsets方法获取分区的最新offset，然后使用seek方法将分区的拉取位置调整至分区的最新位置（相当于seekToEnd方法）
     * 5.可以使用consumer的offsetsForTimes方法，获取分区在指定时间点之后的offset，然后使用seek方法，将分区的拉取位置调整至那个位置
     * <p>
     * 所以，kakfa中针对分区，有三套offset：
     * 1.分区实际的起始和终止offset，存储于broker中，随着生产者向该topic发送数据而改变。beginningOffsets、endOffsets、offsetsForTimes方法用于查询分区实际的offset
     * 2.group针对消费的分区所提交的offset，存储于broker中，随着该消费组中的consumer提交对应分区的位点来改变。committed方法用于查询group针对分区提交的offset
     * 3.consumer中记录的指定分区的拉取offset，存储于consumer客户端中，随着poll拉取数据和seek方法设置拉取位置而改变。position方法用于查询当前consumer针对分区所拉取的offset
     */
    @Test
    public void seekByTime() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer6");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        // 在获取为当前consumer分配的分区时，经常遇到一个问题，就是poll的timeout时长设置多少。如果设置很长，有可能阻塞很长时间，浪费性能。
        // 因此可以通过循环遍历，每次poll 100毫秒，直到获取到已分配的分区才退出循环
        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(100));
            assignment = kafkaConsumer.assignment();
        } while (Objects.isNull(assignment) || assignment.isEmpty());

        Map<TopicPartition, Long> topicPartitionToSeek = new HashMap<>(assignment.size());
        long seekFrom = ZonedDateTime.now().truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli();
        assignment.forEach(topicPartition -> topicPartitionToSeek.put(topicPartition, seekFrom));
        // 在上面我们使用seek方法调整拉取分区数据的位置时，只能要么调整到分区的起始offset，要么调整到分区的最新offset。现在我们可以结合offsetsForTimes方法，将拉取位置调整到指定时间后的数据
        // offsetsForTimes方法可以查询指定分区，在某一个时间点之后的元素的第一个offset。有了该方法，就可以结合seek方法，把拉取的数据调整至指定时间点后的数据
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = kafkaConsumer.offsetsForTimes(topicPartitionToSeek);

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> offsets : offsetsForTimes.entrySet()) {
            kafkaConsumer.seek(offsets.getKey(), offsets.getValue().offset());
        }

        // 在这里没有拉取数据，而是获取重置每个分区拉取点后，分区的拉取位置，然后把他们提交上去，这样，后进来的消费者，在获取对应的分区后，就会从调整后的位置开始拉取分区的数据
        for (TopicPartition topicPartition : assignment) {
            long position = kafkaConsumer.position(topicPartition);
            kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(position)));
        }

        kafkaConsumer.close();
    }

    @Test
    public void testCommitAsync() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer7");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        kafkaConsumer.poll(Duration.ofSeconds(2));

        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        Map<TopicPartition, Long> partitionToSeek = new HashMap<>(assignment.size());
        assignment.forEach(topicPartition -> partitionToSeek.put(topicPartition, ZonedDateTime.parse("2020-11-23T00:00:00+08:00").toInstant().toEpochMilli()));
        Map<TopicPartition, OffsetAndTimestamp> partitionOffsets = kafkaConsumer.offsetsForTimes(partitionToSeek);
        for (TopicPartition topicPartition : partitionOffsets.keySet()) {
            OffsetAndTimestamp offsetAndTimestamp = partitionOffsets.get(topicPartition);
            kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
        }

        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(assignment);
        do {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("key:{},value:{},topic:{},partition:{},offset:{},timestamp:{}", consumerRecord.key(), consumerRecord.value()
                        , consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp());
            }

            /*
                异步提交，此时主线程不会阻塞很久，不会等到broker响应commit执行成功才能继续执行
                注意callback的执行时机：
                和普通的理解不太一样，之前以为是broker处理commit请求后通知客户端，客户端接到响应后新起一个线程来执行callback，
                而kafka执行异步提交callback是在执行poll和commitAsync方法时，遍历当前客户端已收到的所有commit响应，执行对应的callback。
                因此，在执行poll、commitAsync方法时，都有可能执行此前已响应的commit请求的callback。

                正是由于这种机制，导致有可能会有callback无法执行，比如只拉取并commit5次数据，假如前四次commit请求都发出去后，在第五次拉取和
                commit时，broker对这四次commit都没有发出响应。那么这五次commit对应的callback不会执行（第五次的commit需要在第六次poll或commit时才
                有可能执行callback，因为只执行五次，所以第五次的callback肯定不会执行）。
                也就是说：异步提交，更适合于无限次的拉取和提交，不太适合有限次数（尤其次数非常少的情况），有可能造成客户端无法知晓服务端是否已响应提交的commit。

                A callback interface that the user can implement to trigger custom actions when a commit request completes.
                The callback may be executed in any thread calling {@link Consumer#poll(java.time.Duration) poll()}
            */
            kafkaConsumer.commitAsync((offsets, exception) -> {
                // 在callback中可以获取每个分区提交的offset以及commit请求是否处理异常
                if (Objects.nonNull(exception)) {
                    log.error("异步提交位点异常", exception);
                } else {
                    for (TopicPartition topicPartition : offsets.keySet()) {
                        OffsetAndMetadata offsetAndMetadata = offsets.get(topicPartition);
                        log.info("分区{}提交位点至{}", topicPartition, offsetAndMetadata.offset());
                    }
                }
            });

            boolean isEnd = false;
            for (TopicPartition topicPartition : assignment) {
                isEnd = kafkaConsumer.position(topicPartition) >= endOffsets.get(topicPartition);
                if (!isEnd) {
                    break;
                }
            }

            if (isEnd) {
                break;
            }


        } while (true);

        kafkaConsumer.close();
    }

    @Test
    public void testCommitAsyncByPartition() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer8");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(DataSize.ofBytes(500).toBytes()));

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("hello-world");
        List<TopicPartition> partitionListConsumed = partitionInfos.subList(0, 3).stream().map(partitionInfo
                -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList());

        kafkaConsumer.assign(partitionListConsumed);
        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(100));
            assignment = kafkaConsumer.assignment();
        } while (Objects.isNull(assignment));

        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(assignment);
        // 调整已分配的分区的拉取位点，调整到分区的起始offset
        kafkaConsumer.seekToBeginning(assignment);

        do {
            // 拉取数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            // 处理数据并提交位点
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                List<ConsumerRecord<String, String>> partitionConsumerRecordList = consumerRecords.records(topicPartition);
                // 处理当前分区的数据
                for (ConsumerRecord<String, String> consumerRecord : partitionConsumerRecordList) {
                    log.info("key:{},value:{},topic:{},partition:{},offset:{},timestamp:{}", consumerRecord.key(), consumerRecord.value()
                            , consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp());
                }

                // 处理完该分区的数据后，异步提交该分区的位点
                kafkaConsumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(partitionConsumerRecordList.get(partitionConsumerRecordList.size() - 1).offset() + 1)),
                        (offsets, exception) -> {
                            if (Objects.nonNull(exception)) {
                                log.error("分区{}异步提交位点失败", topicPartition, exception);
                            } else {
                                OffsetAndMetadata offsetCommitted = offsets.get(topicPartition);
                                log.info("分区{}已完成异步提交位点:{}", topicPartition, offsetCommitted.offset());
                            }
                        });
            }

            // 判断是否继续拉取数据
            boolean needNextPoll = false;
            for (TopicPartition topicPartition : assignment) {
                long position = kafkaConsumer.position(topicPartition);
                Long endOffset = endOffsets.get(topicPartition);

                needNextPoll = endOffset.compareTo(position) > 0;
                if (needNextPoll) {
                    break;
                }
            }

            if (!needNextPoll) {
                break;
            }
        } while (true);
    }

    @Test
    public void rebalance() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer7");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(20)));

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println(partitions);
            }
        });

        // 如果一个consumer拉取到了数据以后，发生了rebalance，那么此时会显示Consumer group 'helloGroup' is rebalancing.此时这个消费组处于暂停状态/
        // 只有拉取到数据的这个consumer关闭连接或者这个consumer再次拉取数据，这时rebalance才可以完成
        kafkaConsumer.poll(Duration.ofSeconds(120));
        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }

    /**
     * ConsumerInterceptor的四个方法中：
     * 1.configure方法
     * configure方法是在创建KafkaConsumer对象时，在主线程中执行
     * 2.onConsume方法
     * onConsume方法是在KafkaConsumer调用poll方法时，在主线程中执行，执行完该方法后，poll方法才会把获取的数据返回给调用方
     * 3.close方法
     * close方法是在调用KafkaConsumer调用close方法时，在主线程中执行
     * 4.onCommit方法
     * 如果同步提交位点，在当前线程执行该方法。如果异步同步位点，在心跳线程执行该方法
     */
    public static class IgnoreSmallValueInterceptor implements ConsumerInterceptor<String, String> {
        /**
         * 该方法会在poll拉取数据后，客户端获取数据之前调用。该方法可以修改从broker中拉取到的数据，也就是客户端接到的数据
         * 注意：
         * 使用该方法删除数据是有一定弊端的，假设offset=4和5的数据被拦截器删除了，那么客户端只能读到offset=3的数据，那么客户端
         * 在提交位点时，提交的offset是4，但其实offset=4和5的数据虽然被忽略了，但是位点也应该被提交上去，即应该提交offset到6
         *
         * @param records
         * @return
         */
        @Override
        public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
            Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>(records.partitions().size());
            for (TopicPartition topicPartition : records.partitions()) {
                List<ConsumerRecord<String, String>> consumerRecordList = records.records(topicPartition);
                List<ConsumerRecord<String, String>> filteredRecordList = consumerRecordList.stream().filter(record -> record.value().length() > 5).collect(Collectors.toList());
                map.put(topicPartition, filteredRecordList);
            }

            return new ConsumerRecords<>(map);
        }

        /**
         * 1.如果主线程使用的是异步方式提交位点，那么该方法会在心跳线程执行，当broker给与commit请求响应后，会先执行该方法，然后再执行客户端的callback
         * 2.如果主线程使用的是同步方式提交位点，那么该方法会在当前线程执行，当broker给与commit请求响应后，会先执行该方法，然后才会执行完位点提交方法
         *
         * @param offsets
         */
        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            for (TopicPartition topicPartition : offsets.keySet()) {
                OffsetAndMetadata offsetAndMetadata = offsets.get(topicPartition);
                log.info("位点提交成功。分区:{}，offset:{}", topicPartition, offsetAndMetadata.offset());
            }
        }

        /**
         * close方法是在主线程调用KafkaConsumer.close方法时调用该方法
         */
        @Override
        public void close() {
            log.info("consumer已关闭");
        }

        /**
         * configure方法在创建KafkaConsumer对象时调用，方法内传入的Map对象是创建consumer时，客户端传入的配置的副本，
         * 也就是说改变这个Map对象并不能改变consumer的配置。
         * <p>
         * 因此这个方法只用于展示或查询客户端给consumer提供的配置的副本
         *
         * @param configs
         */
        @Override
        public void configure(Map<String, ?> configs) {
            log.info("客户端为当前consumer提供的配置：{}", configs);

            // 在这里尝试改变client.id属性，但是实际和broker连接时，client.id还是原有的配置
            Map<String, String> config = (Map<String, String>) configs;
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, "intercept-test");
        }
    }

    @Test
    public void testConsumerInterceptor() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(DataSize.ofBytes(500).toBytes()));
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, IgnoreSmallValueInterceptor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        Set<TopicPartition> assignment;
        do {
            /*
                为什么多次调用kafkaConsumer.poll(Duration.ofMillis(100));，就有可能获取分区？
                因为在调用poll(Duration.ofMillis(100))方法时，有可能因为超时导致本次poll请求没有获取到响应数据就返回给主线程调用方了，
                但是代表这次请求结果的Futrue被存储了起来。当下次调用poll方法时，就可以尝试获取上一次poll请求时broker响应的结果，然后根据
                这个结果来继续发起请求。
                第一次poll时，发起的poll请求中是没有client的member.id的，在broker的响应中会告诉客户端发生了没有member.id的错误，所以无法
                加入group，并且给客户端分配了一个member.id。
                但是第一次poll时超时时间过短，导致没有收到broker响应就返回给主线程了。而再下一次poll时，获取到了上一次poll请求响应的内容，
                也就是获取到了member.id，所以这次poll就可以使用这个member.id来发起加入group的请求了。

                直到某一次poll时，发现上一次poll请求的响应结果成功了（因为上一次请求在获取响应之前有可能因为超时时间设置太短而不能响应结果直接返回给主线程了）
                ，就说明加入group成功了，就可以使用assignment方法获取为当前consumer分配的分区了
            */
            kafkaConsumer.poll(Duration.ofMillis(100));
            assignment = kafkaConsumer.assignment();
        } while (Objects.isNull(assignment) || assignment.isEmpty());

        Map<TopicPartition, Long> partitionSearch = new HashMap<>(assignment.size());
        long timeToSearch = ZonedDateTime.parse("2020-11-23T00:00:00+08:00").toInstant().toEpochMilli();
        assignment.forEach(topicPartition -> partitionSearch.put(topicPartition, timeToSearch));

        Map<TopicPartition, OffsetAndTimestamp> partitionOffsets = kafkaConsumer.offsetsForTimes(partitionSearch);
        for (TopicPartition topicPartition : partitionOffsets.keySet()) {
            OffsetAndTimestamp offsetAndTimestamp = partitionOffsets.get(topicPartition);
            kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
        }


        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(assignment);
        do {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("key:{},value:{},topic:{},partition:{},offset:{},timestamp:{}", consumerRecord.key(), consumerRecord.value()
                        , consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp());
            }
            kafkaConsumer.commitSync();

            boolean needNextPoll = false;
            for (TopicPartition topicPartition : assignment) {
                long position = kafkaConsumer.position(topicPartition);
                Long endOffset = endOffsets.get(topicPartition);

                needNextPoll = endOffset.compareTo(position) > 0;
                if (needNextPoll) {
                    break;
                }
            }

            if (!needNextPoll) {
                break;
            }
        } while (true);

        kafkaConsumer.close();
    }
}