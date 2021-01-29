package com.mzq.usage.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mzq.usage.flink.domain.BdWaybillOrder;
import com.mzq.usage.flink.util.GenerateDomainUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
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
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = JacksonAutoConfiguration.class)
public class ElasticsearchTest implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTest.class);
    private static final String BD_WAYBILL_ORDER_SETTINGS = "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1},\"mappings\":{\"properties\":{\"waybillCode\":{\"type\":\"keyword\"},\"waybillSign\":{\"type\":\"keyword\"},\"siteCode\":{\"type\":\"keyword\"},\"siteName\":{\"type\":\"keyword\"},\"busiNo\":{\"type\":\"keyword\"},\"busiName\":{\"type\":\"keyword\"},\"sendPay\":{\"type\":\"keyword\"},\"pickupDate\":{\"type\":\"date\"},\"deliveryDate\":{\"type\":\"date\"},\"orderCode\":{\"type\":\"keyword\"},\"orderCreateDate\":{\"type\":\"date\"},\"packageCode\":{\"type\":\"keyword\"},\"timestamp\":{\"type\":\"date\"}}}}";

    private ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    @Test
    public void testBuildClient() throws IOException {
        // 创建RestClient的最基本要求就是给出es服务器的地址
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        /*
            设置节点选择器，用于选择向哪个节点发送请求。默认是可以向任何客户端已知的节点（包括sniff过来的节点）发送请求，设置为SKIP_DEDICATED_MASTERS则阻止了向master节点发送请求

            Set the node selector to be used to filter the nodes the client will send requests to among the ones that are set to the client itself.
            This is useful for instance to prevent sending requests to dedicated master nodes when sniffing is enabled. By default the client sends requests to every configured node.
         */
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        // FailureListener是在向节点发送请求失败时才会调用（例如请求节点不存在）。而请求发送成功，并且es服务器响应错误时（例如服务器响应index_not_exists），客户端是直接收到Exception，而不会调用该方法
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                System.out.println(node.getHost() + "失败了");
            }
        });
        // 设置httpclient，因为rest方式在底层是使用发送http请求的，所以需要有httpclient
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(200);

            // 如果es服务器设置了密码认证，需要使用此方式进行配置
//            CredentialsProvider credentialsProvider = new SystemDefaultCredentialsProvider();
//            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "pass"));
//            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });
        // 设置请求相关配置，例如建立连接超时、读取响应数据超时等配置
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> {
            // 控制客户端和es服务器建立连接的超时时间
            // the timeout in milliseconds until a connection is established.
            requestConfigBuilder.setConnectTimeout(3000);
            // 控制用户线程从线程池中获取连接的超时时间
            // the timeout in milliseconds used when requesting a connection from the connection manager.
            requestConfigBuilder.setConnectionRequestTimeout(3000);
            // 控制客户端向es服务器发出请求后，等待响应数据的超时时间
            // the socket timeout in milliseconds, which is the timeout for waiting for data or, put differently, a maximum period inactivity between two consecutive data packets).
            requestConfigBuilder.setSocketTimeout(5000);

            return requestConfigBuilder;
        });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        IndicesClient indices = restHighLevelClient.indices();
        boolean exists = indices.exists(new GetIndexRequest("hello_world"), RequestOptions.DEFAULT);
        if (!exists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("hello_world");
            createIndexRequest.settings(Settings.builder().put("number_of_replicas", 1).put("number_of_shards", 1));
            createIndexRequest.mapping("{\"properties\":{\"code\":{\"type\":\"keyword\"},\"name\":{\"type\":\"keyword\"}}}", XContentType.JSON);
            indices.create(createIndexRequest, RequestOptions.DEFAULT);
        }

        HelloWorld helloWorld = new HelloWorld("zhangsan", "张三");
        IndexRequest indexRequest = new IndexRequest("hello_world");
        indexRequest.source(objectMapper.writeValueAsString(helloWorld), XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        System.out.println(id);

        GetResponse getResponse = restHighLevelClient.get(new GetRequest("hello_world", id), RequestOptions.DEFAULT);
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println(sourceAsMap);

        // RestHighLevelClient的生命周期期望和程序是一样的，当程序结束时，也应该主动调用RestHighLevelClient的close方法来关闭和es的连接
        restHighLevelClient.close();
    }

    /**
     * 异步操作一般都是以Async结尾的方法，而同步操作则是对应没有Async结尾的方法。例如countAsync是异步查询数量，而count是同步查询数量
     * 所谓同步操作，就是在客户端将请求发送给es服务器后，会阻塞用户线程，需要等待服务器给出响应后才可以让用户线程继续执行
     * <p>
     * 同步操作适用于后续操作需要依赖当前发出的请求的处理结果。例如是否调用indicesClient.create方法需要indicesClient.exists的响应结果，
     * 但如果需要发送大量的es请求的话，同步操作由于需要阻塞用户线程，因此效率不是很好。
     */
    @Test
    public void testSyncApi() throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("访问节点失败！node={}", node);
            }
        });
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder.setSocketTimeout(2000).setConnectionRequestTimeout(1000).setConnectTimeout(1000));
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setMaxConnTotal(200));

        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);
        IndicesClient indicesClient = highLevelClient.indices();
        boolean exists = indicesClient.exists(new GetIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT);

        if (!exists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("bd_waybill_order");
            createIndexRequest.source(BD_WAYBILL_ORDER_SETTINGS, XContentType.JSON);
            CreateIndexResponse createIndexResponse = indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);
            logger.info("创建索引{}完成。结果：isAcknowledged={},isShardsAcknowledged={}。", createIndexResponse.index(), createIndexResponse.isAcknowledged(), createIndexResponse.isShardsAcknowledged());
        }

        BdWaybillOrder bdWaybillOrder = new BdWaybillOrder();
        bdWaybillOrder.setWaybillCode("JDV2");
        bdWaybillOrder.setBusiNo("001");
        bdWaybillOrder.setDeliveryDate(new Date());
        bdWaybillOrder.setTimestamp(System.currentTimeMillis());

        String source = objectMapper.writeValueAsString(bdWaybillOrder);
        UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode()).doc(source, XContentType.JSON).docAsUpsert(true);
        UpdateResponse updateResponse = highLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        String id = updateResponse.getId();

        GetResponse getResponse = highLevelClient.get(new GetRequest("bd_waybill_order", id), RequestOptions.DEFAULT);
        logger.info("id={}的数据：{}", id, getResponse.getSourceAsMap());

        CountResponse countResponse = highLevelClient.count(new CountRequest("bd_waybill_order").query(QueryBuilders.termQuery("busiNo", bdWaybillOrder.getBusiNo())), RequestOptions.DEFAULT);
        logger.info("共有{}条数据！", countResponse.getCount());
        highLevelClient.close();
    }

    /**
     * 与同步方法不一样的是，异步方法不需要等待请求的响应结果，用户线程不用阻塞直至服务器响应。在调用异步方法时，需要传入ActionListener的实现，当服务器给出响应后，会在新的线程中执行ActionListener。
     * 异步方法非常适用于后续操作不依赖于当前请求的响应结果扽情况。例如需要向es发起多次存储或更新数据的请求，但是用户线程又不需要知道存储或更新的结果。这样使用异步方法可以提高用户线程的处理效率。
     */
    @Test
    public void testAsyncApi() throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("请求指定节点发生错误！节点={}", node);
            }
        });
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(2000).setConnectionRequestTimeout(3000).setSocketTimeout(2000));
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(200);

            CredentialsProvider credentialsProvider = new SystemDefaultCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "pass"));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        IndicesClient indicesClient = restHighLevelClient.indices();
        indicesClient.existsAsync(new GetIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT, new ActionListener<Boolean>() {
            /**
             * onResponse方法会在服务器对请求给出响应后，在另一个线程中执行这个方法
             */
            @Override
            public void onResponse(Boolean exists) {
                if (!exists) {
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest("bd_waybill_order");
                    createIndexRequest.source(BD_WAYBILL_ORDER_SETTINGS, XContentType.JSON);
                    try {
                        CreateIndexResponse createIndexResponse = indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);
                        if (!createIndexResponse.isAcknowledged()) {
                            throw new IllegalStateException(String.format("创建索引失败！index=%s,source=%s", "bd_waybill_order", BD_WAYBILL_ORDER_SETTINGS));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            /**
             * onFailue方法会在将请求发送给es服务器产生异常时调用。例如根据HttpHost找不到对应的es服务器，或者RestClient已关闭
             */
            @Override
            public void onFailure(Exception e) {
                logger.error("existsAsync执行失败！", e);
            }
        });

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("hello_world").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()));
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            logger.info(objectMapper.writeValueAsString(searchHit.getSourceAsMap()));
        }

        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(10);
        List<String> idList = Collections.synchronizedList(new ArrayList<>(bdWaybillOrders.size()));
        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            IndexRequest indexRequest = new IndexRequest("bd_waybill_order");
            indexRequest.source(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON);
            restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    idList.add(indexResponse.getId());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("indexAsync执行发生异常！", e);
                }
            });
        }

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        idList.forEach(id -> multiGetRequest.add("bd_waybill_order", id));
        MultiGetResponse multiGetItemResponses = restHighLevelClient.mget(multiGetRequest, RequestOptions.DEFAULT);
        Arrays.stream(multiGetItemResponses.getResponses()).forEach(multiGetItemResponse -> System.out.println(multiGetItemResponse.getResponse().getSourceAsMap()));

        restHighLevelClient.close();
    }

    /**
     * 上面介绍的无论是同步方法还是异步方法，它的一个最大的弊端就是每次执行都会进行一次和数据库的交互。假如要插入1000条数据的话，客户端就要和服务器有1000次交互
     * 解决的办法就是使用BulkRequest，把这1000个请求在内存中装到一个BulkRequest对象中，然后把这个BulkRequest对象发送给服务器，这样就只用了一次和服务器的请求就把1000个请求都发过去了
     * <p>
     * 注意：
     * 1.只能在BulkRequest中添加写入请求（例如UpdateRequest、DeleteRequest）
     * 2.BulkResponse中的响应顺序和BulkRequest中的请求顺序是一致的
     */
    @Test
    public void testBulk() throws IOException {
        RestClientBuilder clientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        clientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(2000).setConnectionRequestTimeout(3000).setConnectTimeout(1000));
        clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setMaxConnTotal(200));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(clientBuilder);

        IndicesClient indicesClient = restHighLevelClient.indices();
        if (!indicesClient.exists(new GetIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT)) {
            CreateIndexResponse createIndexResponse = indicesClient.create(new CreateIndexRequest("bd_waybill_order").source(BD_WAYBILL_ORDER_SETTINGS, XContentType.JSON), RequestOptions.DEFAULT);
            boolean acknowledged = createIndexResponse.isAcknowledged();
            if (!acknowledged) {
                throw new IllegalStateException("创建索引失败！");
            }
        }

        SearchRequest searchRequest = new SearchRequest("bd_waybill_order");
        searchRequest.source(SearchSourceBuilder.searchSource().size(15).query(QueryBuilders.matchAllQuery()));
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        List<String> idList;
        if (searchResponse.getHits().getTotalHits().value > 0) {
            idList = Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList());
        } else {
            List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(10);
            BulkRequest bulkRequest = new BulkRequest();
            for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
                IndexRequest indexRequest = new IndexRequest("bd_waybill_order");
                indexRequest.source(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON);
                UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode()).doc(Collections.singletonMap("timestamp", System.currentTimeMillis()));
                DeleteRequest deleteRequest = new DeleteRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode());

                bulkRequest.add(updateRequest, deleteRequest, indexRequest);
            }
            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            logger.info("执行批处理共花费了{},是否有处理失败的请求：{}，失败信息：{}"
                    , bulkResponse.getTook(), bulkResponse.hasFailures(), bulkResponse.buildFailureMessage());
            idList = Arrays.stream(bulkResponse.getItems()).filter(bulkItemResponse -> !bulkItemResponse.isFailed()).map(BulkItemResponse::getId).collect(Collectors.toList());
        }

        BulkRequest bulkRequest = new BulkRequest();
        idList.stream().map(id -> new DeleteRequest("bd_waybill_order", id)).forEach(bulkRequest::add);
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("执行批处理共花费了{},是否有处理失败的请求：{}，失败信息：{}", bulkResponse.getTook(), bulkResponse.hasFailures(), bulkResponse.buildFailureMessage());
        restHighLevelClient.close();
    }

    /**
     * 在上面使用bulk方法的例子中，和其他的同步请求一样，bulk方法的一个弊端就是用户线程需要阻塞直至es服务器对BulkRequest响应完毕。
     * 当BulkRequest中的请求非常多时，es服务器可能需要几秒来完成响应，所以用户线程就需要阻塞几秒，导致用户线程的处理效率非常低。
     * 所以一般在使用发起Bulk请求时，都是使用异步bulkAsync的方式，减少用户线程的阻塞。
     */
    @Test
    public void testBulkAsync() throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("访问节点失败！节点={}", node);
            }
        });
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(2000).setConnectionRequestTimeout(1000).setConnectTimeout(1000));
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(200);

            CredentialsProvider credentialsProvider = new SystemDefaultCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "pass"));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        BulkRequest bulkRequest = new BulkRequest();
        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(10);
        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            IndexRequest indexRequest = new IndexRequest("bd_waybill_order");
            indexRequest.source(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON);

            UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode());
            updateRequest.doc(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON);
            bulkRequest.add(indexRequest, updateRequest);
        }

        restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                logger.info("BulkRequest处理完毕，用时：{}。响应成功的docId列表：{}，是否有处理失败的请求：{}，处理失败的信息：{}", bulkItemResponses.getTook()
                        , Stream.of(bulkItemResponses.getItems()).filter(bulkItemResponse -> !bulkItemResponse.isFailed()).map(BulkItemResponse::getId).collect(Collectors.joining(","))
                        , bulkItemResponses.hasFailures(), bulkItemResponses.buildFailureMessage());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("BulkRequest处理异常！", e);
            }
        });

        // 这里模拟用户线程发起bulk请求后就去做其他事情了
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(15));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        restHighLevelClient.close();
    }

    /**
     * 使用BulkRequest的一个缺点就是用户线程必须要提前整理好都有哪些请求，把他们创建成一个BulkRequest，然后用户线程自己判断应该何时调用bulk方法。而现实中更多的需求是我提前并不知道有多少请求，而是上游来一次调用则会产生一个请求
     * 这时就需要一个处理器来帮我创建BulkRequest并存储已添加的请求，同时由于当上游调用我时，我不知道已经有多少请求了，所以需要这个处理器来帮我判断何时应该进行BulkRequest的提交。
     * es提供的BulkProcessor就是为了解决这种场景的问题，它让用户无需提前准备好所有的请求，用户每来一个请求只需要调用BulkProcessor的add方法。而在BulkProcessor内部
     * 配置了有多少条请求以后发送BulkRequest以及请求有多大（KB）以后发送BulkRequest，由BulkProcessor自己决定何时进行BulkRequest的提交，用户就不需要关心创建和发送BulkRequest的问题了。
     */
    @Test
    public void testBulkProcessor() throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("访问节点失败！节点：{}", node);
            }
        });
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(2000).setConnectionRequestTimeout(2000).setSocketTimeout(5000));
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setMaxConnTotal(150));

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

         /*
            给BulkProcessor传入的BiConsumer用于处理BulkRequest并调用ActionListener，具有这种功能的一般是RestHighLevelClient
            在这里我们把BiConsumer赋值是调用RestHighLevelClient的bulkAsync方法，因此当BulkProcessor认为应当把BulkRequest提交时，实际上是通过异步提交的方式来把BulkRequest提交给es服务器
            所以我们看到BulkProcessor的主要职责是：
            1.创建BulkRequest对象
            2.往BulkRequets中添加DocWriteRequest
            3.判断合适的时机，来把BulkRequest提交给传入的BiConsumer进行请求提交
        */
        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) -> restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener)
                , new BulkProcessor.Listener() {
                    /**
                     * 在准备提交BulkRequest时，在把BulkRequest提交给es服务器之前调用
                     */
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.info("即将发送BulkRequest。executionId={}", executionId);
                    }

                    /**
                     * 在es服务器对提交的BulkRequest作出响应后调用
                     */
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.info("BulkRequest执行成功。executionId={}", executionId);
                    }

                    /**
                     * 在把BulkRequest发送es服务器失败时调用
                     */
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error("BulkRequest执行失败。executionId={}", executionId, failure);
                    }
                });

        // 设置当存储的BulkRequest中积聚了多少个请求后就提交BulkRequest
        bulkProcessorBuilder.setBulkActions(7);
        // 设置当存储的BulkRequest中积聚的请求的大小（调用BulkRequest.estimatedSizeInBytes方法判断）有多少时就提交BulkRequest
        bulkProcessorBuilder.setBulkSize(new ByteSizeValue(10, ByteSizeUnit.KB));
        // 设置多长时间刷新一次BulkRequest，也就是多长时间把存储的BulkRequest提交给es服务器。该配置是为了防止由于请求添加的比较少而无法满足BulkActions或BulkSize时，导致长时间不提交BulkRequest的问题。
        bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueSeconds(10));
        // 设置BulkRequest的重试策略，在这里配置的是5秒后发起重试，最多发起3次重试
        // 有以下两种情况会重试：1.es服务器响应执行成功，但是响应的BulkResponse中有状态码是429（TOO_MANY_REQUESTS）2.es服务器响应执行BulkRequest失败
        bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(5), 3));
        // 设置同时有多少个线程可以使用同一个BulkProcessor对象来提交BulkRequest请求
        bulkProcessorBuilder.setConcurrentRequests(5);
        BulkProcessor bulkProcessor = bulkProcessorBuilder.build();

        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(10);
        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            IndexRequest indexRequest = new IndexRequest("bd_waybill_order");
            indexRequest.source(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON);
            // 在给BulkProcessor添加请求时，会判断请求数量或大小是否已满足BulkSize或BulkActions的配置，如果满足了，就会自动地进行BulkRequest的提交，不需要调用方来决策何时进行BulkRequest的提交
            // 这是和普通使用BulkRequest的最大区别，普通使用BulkRequest时，需要用户来决定何时调用RestHighLevelClient的bulk方法来提交请求
            bulkProcessor.add(indexRequest);

            UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode());
            updateRequest.doc(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON);
            bulkProcessor.add(updateRequest);
        }

        try {
            // 阻塞当前线程一段时间，让FlushInterval生效一次，将因为没有满足BulkActions或BulkSize，但到达FlushInterval生效时间时，把存储的BulkRequest请求提交出去
            Thread.sleep(TimeUnit.SECONDS.toMillis(15));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // BulkProcessor.close方法会把BulkProcessor会把还没有发出去的BulkRequest发送到es服务器中
        // 注意：BulkProcessor的close方法应该和RestHighLevelClient的close方法在执行上有一定间隔，否则关闭了和es服务器的连接后就不能把BulkRequest提交出去了，也无法接收到BulkRequest的响应
        bulkProcessor.close();

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        restHighLevelClient.close();
    }

    @Test
    public void testBulkProcessorExercise() throws IOException {
        RestClientBuilder clientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(clientBuilder);

        BulkProcessor.Builder builder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) ->
                        restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener)
                , new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.info("即将发送BulkRequest！executionId={}", executionId);
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.info("BulkRequest执行成功，executionId={},用时：{}。是否有处理失败的请求：{}，处理失败的信息：{}", executionId, response.getTook(), response.hasFailures(), response.buildFailureMessage());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error("BulkRequest执行异常。executionId={}", executionId, failure);
                    }
                });

        builder.setBulkActions(6);
        builder.setBulkSize(new ByteSizeValue(10, ByteSizeUnit.KB));
        builder.setFlushInterval(TimeValue.timeValueSeconds(15));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(3), 5));

        BulkProcessor bulkProcessor = builder.build();
        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(35);
        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode());
            updateRequest.doc(objectMapper.writeValueAsString(bdWaybillOrder), XContentType.JSON).docAsUpsert(true);

            bulkProcessor.add(updateRequest);
        }

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        bulkProcessor.close();

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        restHighLevelClient.close();
    }

    @Test
    public void testElasticsearchSink() throws Exception {
        RestClientBuilder clientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        clientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(1000).setSocketTimeout(1000).setConnectTimeout(500));
        clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setMaxConnTotal(200));
        clientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("访问节点失败！节点={}", node);
            }
        });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(clientBuilder);
        IndicesClient indicesClient = restHighLevelClient.indices();
        boolean exists = indicesClient.exists(new GetIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT);
        if (exists) {
            indicesClient.delete(new DeleteIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT);
        }
        indicesClient.create(new CreateIndexRequest("bd_waybill_order").source(BD_WAYBILL_ORDER_SETTINGS, XContentType.JSON), RequestOptions.DEFAULT);
        restHighLevelClient.close();

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<BdWaybillOrder> bdWaybillOrderDataStreamSource = streamExecutionEnvironment.addSource(new ParallelSourceFunction<BdWaybillOrder>() {
            @Override
            public void run(SourceContext<BdWaybillOrder> ctx) throws Exception {
                BdWaybillOrder bdWaybillOrder = GenerateDomainUtils.generateBdWaybillOrder();
                ctx.collect(bdWaybillOrder);
            }

            @Override
            public void cancel() {
            }
        }).setParallelism(6).disableChaining();

        // 在ElasticsearchSink.Builder中既要配置RestClintBuilder，也要配置BulkProcessor。因为RequestIndexer的实现类是Elasticsearch7BulkProcessorIndexer，它在内部处理DocWriteRequest时是用BulkProcessor来处理
        // 也就是说ElasticsearchSink的职责就是在初始化时（open方法）创建RestClientBuilder和BulkProcessor，然后在处理数据时（invoke方法）把数据和BulkProcessor传给ElasticsearchSinkFunction，
        // 而在ElasticsearchSinkFunction的实现中，只需要把数据转换成DocWriteRequest，然后提交给BulkProcessor就可以了
        ElasticsearchSink.Builder<BdWaybillOrder> esSinkBuilder = new ElasticsearchSink.Builder<>(Collections.singletonList(HttpHost.create("localhost:9200")),
                (element, ctx, indexer) -> {
                    IndexRequest indexRequest = new IndexRequest("bd_waybill_order");
                    try {
                        String json = objectMapper.writeValueAsString(element);
                        indexRequest.source(json, XContentType.JSON);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    indexer.add(indexRequest);
                });

        // 在这里配置RestClientBuilder
        esSinkBuilder.setRestClientFactory(restClientBuilder -> {
            restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            restClientBuilder.setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    logger.error("访问节点失败！节点={}", node);
                }
            });
            restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(1000).setSocketTimeout(500).setConnectionRequestTimeout(800));
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setMaxConnTotal(200);

                CredentialsProvider credentialsProvider = new SystemDefaultCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "pass"));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                return httpClientBuilder;
            });
        });

        // 在这里配置BulkProcessor
        // 配置BulkProcessor的bulkAction
        esSinkBuilder.setBulkFlushMaxActions(8);
        // 配置BulkProcessor的bulkSize
        esSinkBuilder.setBulkFlushMaxSizeMb(5);
        // 配置BulkProcessor的flushInterval
        esSinkBuilder.setBulkFlushInterval(TimeUnit.SECONDS.toMillis(10));
        // 配置BulkProcessor的BulkProcessor.Listener
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        // 下面四行代码相当于配置bulkProcessor.setBackoffPolicy(BackoffPolicy.constantBackoff(delay,maxRetry))
        esSinkBuilder.setBulkFlushBackoff(true);
        esSinkBuilder.setBulkFlushBackoffDelay(TimeUnit.SECONDS.toMillis(5));
        esSinkBuilder.setBulkFlushBackoffRetries(3);
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);

        DataStreamSink<BdWaybillOrder> bdWaybillOrderDataStreamSink = bdWaybillOrderDataStreamSource.addSink(esSinkBuilder.build());
        bdWaybillOrderDataStreamSink.setParallelism(6);
        streamExecutionEnvironment.execute();
    }

    @Test
    public void testElasticsearchSinkExcercise() throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("localhost:9200"));
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("节点连接失败！节点={}", node);
            }
        });
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(1000).setSocketTimeout(800).setConnectionRequestTimeout(500));
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(200);

            CredentialsProvider credentialsProvider = new SystemDefaultCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "pass"));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        IndicesClient indicesClient = restHighLevelClient.indices();
        boolean indexExists = indicesClient.exists(new GetIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT);
        if (indexExists) {
            indicesClient.delete(new DeleteIndexRequest("bd_waybill_order"), RequestOptions.DEFAULT);
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("bd_waybill_order");
        createIndexRequest.source(BD_WAYBILL_ORDER_SETTINGS, XContentType.JSON);
        indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) -> restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.info("即将执行BulkRequest。executionId={}，requestSize={}", executionId, request.requests().size());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.info("BulkRequest处理完毕，用时：{}。executionId={}，处理成功docId列表={}，是否有处理失败={}，处理失败信息={}", response.getTook(), executionId
                                , Arrays.stream(response.getItems()).filter(bulkItemResponse -> !bulkItemResponse.isFailed()).map(BulkItemResponse::getId).collect(Collectors.toList()), response.hasFailures(), response.buildFailureMessage());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error("BulkRequest处理异常。executionId={},requestSize={}", executionId, request.requests().size(), failure);
                    }
                });
        bulkProcessorBuilder.setBulkActions(4);
        bulkProcessorBuilder.setBulkSize(new ByteSizeValue(10, ByteSizeUnit.KB));
        bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueSeconds(15));
        bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(5), 3));
        bulkProcessorBuilder.setGlobalIndex("bd_waybill_order");

        BulkProcessor bulkProcessor = bulkProcessorBuilder.build();
        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(30);
        bdWaybillOrders.stream().flatMap(bdWaybillOrder -> {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                String json = objectMapper.writeValueAsString(bdWaybillOrder);
                IndexRequest indexRequest = new IndexRequest();
                indexRequest.id(bdWaybillOrder.getWaybillCode()).source(json, XContentType.JSON);
                return Stream.of(indexRequest);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return Stream.empty();
            }
        }).forEach(bulkProcessor::add);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        bulkProcessor.close();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        restHighLevelClient.close();

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
                    BdWaybillOrder bdWaybillOrder = GenerateDomainUtils.generateBdWaybillOrder();
                    ctx.collect(bdWaybillOrder);

                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).setParallelism(8);

        ElasticsearchSink.Builder<BdWaybillOrder> esSinkBuilder = new ElasticsearchSink.Builder<>(Collections.singletonList(HttpHost.create("localhost:9200")),
                new ElasticsearchSinkFunction<BdWaybillOrder>() {

                    private ObjectMapper objectMapper;

                    @Override
                    public void open() {
                        objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
                        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                    }

                    @Override
                    public void process(BdWaybillOrder element, RuntimeContext ctx, RequestIndexer indexer) {
                        try {
                            String json = objectMapper.writeValueAsString(element);
                            IndexRequest indexRequest = new IndexRequest("bd_waybill_order");
                            indexRequest.source(json, XContentType.JSON).id(element.getWaybillCode());

                            indexer.add(indexRequest);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                }
        );

        esSinkBuilder.setRestClientFactory(restClientBuilder1 -> {
            restClientBuilder1.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            restClientBuilder1.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(1000).setSocketTimeout(1200).setConnectTimeout(500));
            restClientBuilder1.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setMaxConnTotal(200));
        });

        esSinkBuilder.setBulkFlushMaxActions(20);
        esSinkBuilder.setBulkFlushMaxSizeMb(5);
        esSinkBuilder.setBulkFlushInterval(TimeUnit.SECONDS.toMillis(15));
        esSinkBuilder.setBulkFlushBackoff(true);
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
        esSinkBuilder.setBulkFlushBackoffDelay(3000);
        esSinkBuilder.setBulkFlushBackoffRetries(5);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        ElasticsearchSink<BdWaybillOrder> elasticsearchSink = esSinkBuilder.build();
        bdWaybillOrderDataStreamSource.addSink(elasticsearchSink).setParallelism(12);
    }

    @Test
    public void testFlinkKafka() throws Exception {
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
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).setParallelism(3);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
        bdWaybillOrderDataStreamSource.addSink(flinkKafkaProducer).setParallelism(6);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
        ElasticsearchSink.Builder<BdWaybillOrder> esBuilder = new ElasticsearchSink.Builder<>(Collections.singletonList(HttpHost.create("localhost:9200")), new ElasticsearchSinkFunction<BdWaybillOrder>() {
            private ObjectMapper objectMapper;

            @Override
            public void open() {
                objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            }

            @Override
            public void process(BdWaybillOrder bdWaybillOrder, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                try {
                    String json = objectMapper.writeValueAsString(bdWaybillOrder);
                    UpdateRequest updateRequest = new UpdateRequest("bd_waybill_order", bdWaybillOrder.getWaybillCode()).doc(json, XContentType.JSON).docAsUpsert(true);
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

    public static class HelloWorld {
        private String code;
        private String name;

        public HelloWorld(String code, String name) {
            this.code = code;
            this.name = name;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
