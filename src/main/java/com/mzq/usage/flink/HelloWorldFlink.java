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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
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
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HelloWorldFlink {

    private static final Logger logger = LoggerFactory.getLogger(HelloWorldFlink.class);
    private static final String BD_WAYBILL_ORDER_SETTINGS = "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1},\"mappings\":{\"properties\":{\"waybillCode\":{\"type\":\"keyword\"},\"waybillSign\":{\"type\":\"keyword\"},\"siteCode\":{\"type\":\"keyword\"},\"siteName\":{\"type\":\"keyword\"},\"busiNo\":{\"type\":\"keyword\"},\"busiName\":{\"type\":\"keyword\"},\"sendPay\":{\"type\":\"keyword\"},\"pickupDate\":{\"type\":\"date\"},\"deliveryDate\":{\"type\":\"date\"},\"orderCode\":{\"type\":\"keyword\"},\"orderCreateDate\":{\"type\":\"date\"},\"packageCode\":{\"type\":\"keyword\"},\"timestamp\":{\"type\":\"date\"}}}}";

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("my-elasticsearch:9200"));
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("访问节点失败！node={}", node);
            }
        });

        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(2000).setConnectTimeout(1000).setConnectionRequestTimeout(800));
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setMaxConnTotal(200));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        IndicesClient indicesClient = restHighLevelClient.indices();
        boolean exists = indicesClient.exists(new GetIndexRequest("bd_waybill"), RequestOptions.DEFAULT);
        if (exists) {
            indicesClient.delete(new DeleteIndexRequest("bd_waybill"), RequestOptions.DEFAULT);
        }
        indicesClient.create(new CreateIndexRequest("bd_waybill").source(BD_WAYBILL_ORDER_SETTINGS, XContentType.JSON), RequestOptions.DEFAULT);

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) -> restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.debug("即将执行bulk操作。executionId={}", executionId);
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.info("执行bulk操作成功。executionId={}，执行用时={}，处理成功docId={}，是否有处理失败={}，处理失败信息：{}", executionId, response.getTook()
                                , Stream.of(response.getItems()).map(BulkItemResponse::getId).collect(Collectors.joining(",")), response.hasFailures(), response.buildFailureMessage());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error("执行bulk操作异常。executionId={}", executionId, failure);
                    }
                });
        bulkProcessorBuilder.setBulkActions(11);
        bulkProcessorBuilder.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB));
        bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueSeconds(30));
        bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(3), 2));
        BulkProcessor bulkProcessor = bulkProcessorBuilder.build();

        ObjectMapper objectMapper = new ObjectMapper();
        for (BdWaybillOrder bdWaybillOrder : GenerateDomainUtils.generateBdWaybillOrders(30)) {
            String json = objectMapper.writeValueAsString(bdWaybillOrder);
            IndexRequest indexRequest = new IndexRequest("bd_waybill").id(bdWaybillOrder.getWaybillCode()).source(json, XContentType.JSON);
            bulkProcessor.add(indexRequest);
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        bulkProcessor.close();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        restHighLevelClient.close();

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<BdWaybillOrder> produceSourceStream = streamExecutionEnvironment.addSource(new RichParallelSourceFunction<BdWaybillOrder>() {
            private boolean isRunning = false;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                isRunning = true;
            }

            @Override
            public void run(SourceContext<BdWaybillOrder> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect(GenerateDomainUtils.generateBdWaybillOrder());
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).setParallelism(5);

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        FlinkKafkaProducer<BdWaybillOrder> kafkaProducer = new FlinkKafkaProducer<>("bd_waybill", new KafkaSerializationSchema<BdWaybillOrder>() {
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
                    throw new IllegalStateException(e);
                }
            }
        }, producerConfig, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        produceSourceStream.addSink(kafkaProducer).setParallelism(8);

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "hello-world-group");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "hello-world");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        FlinkKafkaConsumer<BdWaybillOrder> kafkaConsumer = new FlinkKafkaConsumer<>("bd_waybill", new KafkaDeserializationSchema<BdWaybillOrder>() {
            private ObjectMapper objectMapper;

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

            @Override
            public TypeInformation<BdWaybillOrder> getProducedType() {
                return TypeInformation.of(BdWaybillOrder.class);
            }
        }, consumerProperties);

        DataStreamSource<BdWaybillOrder> bdWaybillOrderConsumerSource = streamExecutionEnvironment.addSource(kafkaConsumer).setParallelism(10);
        ElasticsearchSink.Builder<BdWaybillOrder> elasticsearchSinkBuilder = new ElasticsearchSink.Builder<>(Collections.singletonList(HttpHost.create("my-elasticsearch:9200")),
                new ElasticsearchSinkFunction<BdWaybillOrder>() {
                    private ObjectMapper objectMapper;

                    @Override
                    public void open() {
                        objectMapper = new ObjectMapper();
                    }

                    @Override
                    public void process(BdWaybillOrder bdWaybillOrder, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        try {
                            UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode());
                            updateRequest.doc(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON).docAsUpsert(true).retryOnConflict(5);
                            requestIndexer.add(updateRequest);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                });
        elasticsearchSinkBuilder.setRestClientFactory(restClientBuilder1 -> {
            restClientBuilder1.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            restClientBuilder1.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(1000).setSocketTimeout(2000).setConnectionRequestTimeout(800));
            restClientBuilder1.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setMaxConnTotal(200);
                SystemDefaultCredentialsProvider systemDefaultCredentialsProvider = new SystemDefaultCredentialsProvider();
                systemDefaultCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "pass"));
                httpClientBuilder.setDefaultCredentialsProvider(systemDefaultCredentialsProvider);

                return httpClientBuilder;
            });
        });

        elasticsearchSinkBuilder.setBulkFlushMaxActions(100);
        elasticsearchSinkBuilder.setBulkFlushMaxSizeMb(5);
        elasticsearchSinkBuilder.setBulkFlushInterval(TimeUnit.SECONDS.toMillis(30));
        elasticsearchSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        elasticsearchSinkBuilder.setBulkFlushBackoff(true);
        elasticsearchSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
        elasticsearchSinkBuilder.setBulkFlushBackoffRetries(5);
        elasticsearchSinkBuilder.setBulkFlushBackoffDelay(TimeUnit.SECONDS.toMillis(3));

        bdWaybillOrderConsumerSource.addSink(new RichSinkFunction<BdWaybillOrder>() {


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void invoke(BdWaybillOrder value, Context context) throws Exception {

            }
        });

        bdWaybillOrderConsumerSource.addSink(elasticsearchSinkBuilder.build()).setParallelism(15);
        streamExecutionEnvironment.execute();
    }
}
