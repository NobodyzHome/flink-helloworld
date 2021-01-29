package com.mzq.usage.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mzq.usage.flink.domain.BdWaybillOrder;
import com.mzq.usage.flink.util.GenerateDomainUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HelloWorldFlink {

    private static final Logger logger = LoggerFactory.getLogger(HelloWorldFlink.class);
    private static final String BD_WAYBILL_ORDER_SETTINGS = "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1},\"mappings\":{\"properties\":{\"waybillCode\":{\"type\":\"keyword\"},\"waybillSign\":{\"type\":\"keyword\"},\"siteCode\":{\"type\":\"keyword\"},\"siteName\":{\"type\":\"keyword\"},\"busiNo\":{\"type\":\"keyword\"},\"busiName\":{\"type\":\"keyword\"},\"sendPay\":{\"type\":\"keyword\"},\"pickupDate\":{\"type\":\"date\"},\"deliveryDate\":{\"type\":\"date\"},\"orderCode\":{\"type\":\"keyword\"},\"orderCreateDate\":{\"type\":\"date\"},\"packageCode\":{\"type\":\"keyword\"},\"timestamp\":{\"type\":\"date\"}}}}";

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        RestClientBuilder clientBuilder = RestClient.builder(HttpHost.create("my-elasticsearch:9200"));
        clientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(clientBuilder);
        IndicesClient indicesClient = restHighLevelClient.indices();
        boolean exists = indicesClient.exists(new GetIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT);
        if (exists) {
            indicesClient.delete(new DeleteIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT);
        }
        indicesClient.create(new CreateIndexRequest("bd_waybill_order").source(BD_WAYBILL_ORDER_SETTINGS, XContentType.JSON), RequestOptions.DEFAULT);

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<BdWaybillOrder> bdWaybillOrderDataStreamSource = streamExecutionEnvironment.addSource(new RichParallelSourceFunction<BdWaybillOrder>() {
            private boolean isRunning;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                isRunning = true;
            }

            @Override
            public void run(SourceContext<BdWaybillOrder> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect(GenerateDomainUtils.generateBdWaybillOrder());
                    Thread.sleep(600);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).setParallelism(3);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        FlinkKafkaProducer<BdWaybillOrder> flinkKafkaProducer = new FlinkKafkaProducer<>("bd_waybill", new KafkaSerializationSchema<BdWaybillOrder>() {
            private ObjectMapper objectMapper;

            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                objectMapper = new ObjectMapper();
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(BdWaybillOrder element, Long timestamp) {
                try {
                    return new ProducerRecord<>("bd_waybill", element.getWaybillCode().getBytes(), objectMapper.writeValueAsBytes(element));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                return null;
            }
        }, properties, FlinkKafkaProducer.Semantic.NONE);
        bdWaybillOrderDataStreamSource.addSink(flinkKafkaProducer).setParallelism(5);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "myClient");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        FlinkKafkaConsumer<BdWaybillOrder> consumer = new FlinkKafkaConsumer<>("bd_waybill", new KafkaDeserializationSchema<BdWaybillOrder>() {
            private ObjectMapper objectMapper;

            @Override
            public TypeInformation<BdWaybillOrder> getProducedType() {
                return TypeInformation.of(BdWaybillOrder.class);
            }

            @Override
            public void open(DeserializationSchema.InitializationContext context) throws Exception {
                objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            }

            @Override
            public boolean isEndOfStream(BdWaybillOrder nextElement) {
                return false;
            }

            @Override
            public BdWaybillOrder deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                return objectMapper.readValue(record.value(), BdWaybillOrder.class);
            }
        }, consumerConfig);

        DataStreamSource<BdWaybillOrder> bdWaybillOrderDataStreamKafkaSource = streamExecutionEnvironment.addSource(consumer).setParallelism(10);
        ElasticsearchSink.Builder<BdWaybillOrder> esBuilder = new ElasticsearchSink.Builder<>(Collections.singletonList(HttpHost.create("my-elasticsearch:9200")), new ElasticsearchSinkFunction<BdWaybillOrder>() {
            private ObjectMapper objectMapper;

            @Override
            public void open() {
                objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            }

            @Override
            public void process(BdWaybillOrder bdWaybillOrder, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                try {
                    String json = objectMapper.writeValueAsString(bdWaybillOrder);
                    UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode()).doc(json, XContentType.JSON).docAsUpsert(true);;
                    requestIndexer.add(updateRequest);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        });

        esBuilder.setRestClientFactory(restClientBuilder -> {
            restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            restClientBuilder.setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    logger.error("访问节点失败！node={}", node);
                }
            });
            restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(1000).setSocketTimeout(1500).setConnectionRequestTimeout(800));
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setMaxConnTotal(200));
        });
        esBuilder.setBulkFlushMaxActions(10);
        esBuilder.setBulkFlushMaxSizeMb(5);
        esBuilder.setBulkFlushInterval(TimeUnit.SECONDS.toMillis(20));
        esBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        esBuilder.setBulkFlushBackoff(true);
        esBuilder.setBulkFlushBackoffDelay(TimeUnit.SECONDS.toMillis(5));
        esBuilder.setBulkFlushBackoffRetries(3);
        esBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
        bdWaybillOrderDataStreamKafkaSource.addSink(esBuilder.build()).setParallelism(12);
        streamExecutionEnvironment.execute();
    }
}
