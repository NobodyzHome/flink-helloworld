package com.mzq.usage.flink;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class HelloWorldKafka {

    public static void main(String[] args) {
        String topic = "hello-world";

//        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
//        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 0, "JD1", "hello world test");
//        Future<RecordMetadata> send = kafkaProducer.send(record);
//        kafkaProducer.close();

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloGroup");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "hello123");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(120));

        for (ConsumerRecord<String, String> consumerRecord : poll) {
            String key = consumerRecord.key();
            String value = consumerRecord.value();
            System.out.println(String.format("%s:%s", key, value));
        }
        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }
}