package com.mzq.usage.flink;

import com.mzq.usage.flink.domain.WaybillC;
import com.mzq.usage.flink.func.source.WaybillCSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class EventTimeTest {

    /**
     * 每一个元素都有一个时间戳，代表这个元素发生的时间，flink用这个时间戳来决定将元素放到哪个窗口（或者新建一个窗口），在progress_time中，每个元素的时间戳就是算子接收这个元素的时间。
     * 然而事实上，由于网络等原因，算子接收到的这个元素有可能是之前产生的元素，只不过由于网络不通畅，导致延迟了很长时间才发送出来。
     * <p>
     * 为了解决这个问题，flink提出了event_time，就是根据元素的内容本身来获取一个时间戳（例如元素中有crate_time字段，代表这个元素实际被创建的时间），flink根据这个时间戳来决定元素处于
     * 哪个窗口中。我们现在知道了窗口中有哪些元素，但是问题来了，我们要怎么知道系统"当前时间"已经超过了窗口的边界。在progress_time中，这个问题很好解决，因为窗口的边界是根据系统时间（类似于System.currentTimeMillis()）来创建的，
     * 自然也是系统时间一到就触发。但是在event_time中，窗口的边界是由元素自己给的时间戳来决定的，那么系统的"当前时间"也需要由用户给出。用户给出的这个"当前时间"就是水位线，它代表在水位线指定的时间之前就不会有新的元素来了。
     * 假如一个元素的event_time是13:00（从这个元素的create_time字段中提取），flink给它分配了一个13:00到14:00的窗口，只有用户主动提交了14:00的水位线，这个窗口才会fire。
     * 所以，使用event_time有两个需要考虑的事情，这两个事情都是由用户给出：一个是每一个元素是几点发生的，另一个是当前系统时间是几点（即用户提供的水位线）。
     * <p>
     * 在event_time处理中，每一个元素都应该有一个时间戳，代表这个元素真实产生时的时间，这个时间戳可以是元素里的某一个字段的值。例如元素里create_time字段代表元素真正创建时的时间
     * 在event_time中，每一个元素的时间戳用于决定这个元素要放到哪个窗口中，而水位线则用于告诉flink当前系统时间是几点，也就是在这个点之前都不会认为有新元素产生了，窗口根据水位线决定是否要fire
     * 说白了，在progress_time中，当前系统所处于的时间就是当前时间，在这个时间之前自然不会有新的数据。而在event_time的处理中，当前系统所处于的时间并不是当前时间，而是最新提交的水位线的时间。
     * <p>
     * 使用event_time的一个好处是，假如flink任务崩了3个小时，那么当flink任务重启之后，是不是就要往前追积压的消息，假设某个算子开了一个小时的窗口，那么如果按progressing_time来进行开窗的话，这些积压的数据都
     * 进入到了同一个窗口，但是如果按event_time的话，这些积压的数据都能被分配到不同的窗口，因为每个元素的时间戳不是当前时间，而是元素中某一个字段值的时间，这个时间反应的是元素的创建时间。
     * <p>
     * 注意：新来的event_time在水位线之前的元素，flink会认为这个元素迟到了，那么如果流不允许迟到的话，这个元素就不会发至下游算子，而是输出到旁路流中。
     */
    @Test
    public void testWaterMarkManually() throws Exception {
        // 1.水位线超过window的边界，window才会fire
        // 2.如果没有提交水位线，那么flink会自动提交一个Long.max_value的watermark，用于让窗口fire
        // 3.如果有元素在来到时，它的时间戳比水位线要小，说明这个元素迟到了，那么窗口就会丢弃这个元素，输出到旁路流中（如果设置了sideOutputLateData的话）
        // 4.当使用event_time后，窗口的边界依旧是由flink来决定，但是窗口何时fire是由用户何时emit水位线来决定，只有用户emit的水位线超过了窗口的end，窗口才会提交
        // 下面的Tuple4中，第二个属性代表了当前元素的时间戳，第三个属性代表了要提交的水位线。提交元素的时间戳可以使用SourceContext的collect(element,timestamp)方法，
        // 提交水位线可以使用SourceContext的emitWatermark方法
        Tuple4<String, Long, Long, Integer> e1 = new Tuple4<>("a", 500L, -1L, 20);
        Tuple4<String, Long, Long, Integer> e2 = new Tuple4<>("a", 1800L, 2100L, 30);
        // 注意：由于水位线已经提交到2100，所以再来时间戳为1900的元素就会被认为是迟到的，就会被忽略，不输出到下游算子
        Tuple4<String, Long, Long, Integer> e3 = new Tuple4<>("a", 1900L, -1L, 11);
        Tuple4<String, Long, Long, Integer> e4 = new Tuple4<>("a", 3000L, -1L, 22);

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Tuple4<String, Long, Long, Integer>> sourceDataStream = streamExecutionEnvironment.addSource(new SourceFunction<Tuple4<String, Long, Long, Integer>>() {
            @Override
            public void run(SourceContext<Tuple4<String, Long, Long, Integer>> ctx) throws Exception {
                for (Tuple4<String, Long, Long, Integer> element : new Tuple4[]{e1, e2, e3, e4}) {
                    ctx.collectWithTimestamp(element, element.f1);
                    if (element.f2 > 0) {
                        ctx.emitWatermark(new Watermark(element.f2));
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });

        // 元素经过window算子后，就会被滞留在window内，待window算子接收到水位线后，才会将水位线超过window边界的元素进行计算，然后运送至下游算子
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggregateStream = sourceDataStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .aggregate(new AggregateFunction<Tuple4<String, Long, Long, Integer>, List<Tuple4<String, Long, Long, Integer>>, Tuple2<String, Integer>>() {
                    @Override
                    public List<Tuple4<String, Long, Long, Integer>> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<Tuple4<String, Long, Long, Integer>> add(Tuple4<String, Long, Long, Integer> value, List<Tuple4<String, Long, Long, Integer>> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(List<Tuple4<String, Long, Long, Integer>> accumulator) {
                        String name = null;
                        Integer sum = 0;
                        for (Tuple4<String, Long, Long, Integer> e : accumulator) {
                            name = e.f0;
                            sum += e.f3;
                        }
                        return new Tuple2<>(name, sum);
                    }

                    @Override
                    public List<Tuple4<String, Long, Long, Integer>> merge(List<Tuple4<String, Long, Long, Integer>> a, List<Tuple4<String, Long, Long, Integer>> b) {
                        a.addAll(b);
                        return a;
                    }
                });

        aggregateStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * event time和tumbling time window的最大区别是event time会根据元素的时间戳来进行window的划分，而process time根本不看元素的时间戳，只看当前时间来决定元素应该放在哪个window里
     *
     * @throws Exception
     */
    @Test
    public void testEventTimeByWatermarkStrategy() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamExecutionEnvironment.getConfig().setAutoWatermarkInterval(10000);
        streamExecutionEnvironment.setParallelism(1);

        // 上面我们是使用手动发送时间戳和提交水位线的方式，flink也提供了assignTimestampsAndWatermarks算子，可以为收到的每一个元素赋时间戳以及将水位线发送至下游算子
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        SingleOutputStreamOperator<WaybillC> assignTimeStampStream = waybillCDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaybillC>forBoundedOutOfOrderness(Duration.ofMillis(2)).withTimestampAssigner(context -> (element, recordTimestamp) -> element.getTimeStamp()));
        SingleOutputStreamOperator<String> aggregateStream = assignTimeStampStream.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5))).aggregate(
                new AggregateFunction<WaybillC, List<WaybillC>, String>() {
                    @Override
                    public List<WaybillC> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<WaybillC> add(WaybillC value, List<WaybillC> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public String getResult(List<WaybillC> accumulator) {
                        return accumulator.stream().map(WaybillC::getWaybillCode).collect(Collectors.joining(","));
                    }

                    @Override
                    public List<WaybillC> merge(List<WaybillC> a, List<WaybillC> b) {
                        a.addAll(b);
                        return a;
                    }
                }
        );
        aggregateStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * 注意：只有关心时间的算子（例如window operator）才会使用元素的时间戳，才会有迟到、watermark一说
     * <p>
     * window operator的处理元素的流程是：
     * 1.使用window assigner获取这个元素应该处于哪些window里
     * 2.遍历window，判断当前window的end + allowedLateness <= watermark，如果是true的话忽略这个window
     * 3.如果所有window判断window的end + allowedLateness <= watermark都是true，那么认为新来的这个元素迟到了，这个元素就被丢弃了。
     * 如果设置了outputtag，那么这个元素会被丢进旁路流中，如果没有设置，window operator把记录的丢弃的元素数+1
     * 4.如果不都是true，说明这个元素不能被丢弃，然后使用trigger判断window end <= watermark，如果为true则让这个窗口fire。如果为false，则这个元素存储到这个window中，待水位线超过window end再fire这个window
     * <p>
     * 总的来说就是，一个元素来了，分配一个窗口，先根据window end + allowedLateness和watermark判断是否忽略这个元素，不忽略的话则根据window end和watermark判断是否要fire window
     * 从中可以看出，window operator判断一个输入的元素是否超时，使用的不是这个元素的时间戳和watermark的比较，而是给这个元素分配的window的end是否小于watermark
     *
     * @throws Exception
     */
    @Test
    public void testElementLate() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // window [0,3000),e1
        Tuple4<String, Integer, Long, Long> tuple1 = new Tuple4<>("a", 35, 1000L, -1L);
        // window [0,3000),e1,e2 fire
        Tuple4<String, Integer, Long, Long> tuple2 = new Tuple4<>("a", 20, 1500L, 3000L);
        // window end 2999<=watermark 3000,drop
        Tuple4<String, Integer, Long, Long> tuple3 = new Tuple4<>("a", 11, 100L, -1L);
        // window end 2999<=watermark 3000,drop
        Tuple4<String, Integer, Long, Long> tuple4 = new Tuple4<>("a", 23, 1700L, -1L);
        // window [3000,6000) e5
        Tuple4<String, Integer, Long, Long> tuple5 = new Tuple4<>("a", 60, 3100L, -1L);
        // window [3000,6000) e5、e6 fire
        Tuple4<String, Integer, Long, Long> tuple6 = new Tuple4<>("a", 5, 4000L, 7000L);
        // window end 2999<7000L drop
        Tuple4<String, Integer, Long, Long> tuple7 = new Tuple4<>("a", 3, 2300L, -1L);
        Tuple4<String, Integer, Long, Long> tuple8 = new Tuple4<>("a", 3, 1500L, 5000L);
        // 注意这个：虽然timestamp是6500,watermark=7000,看似这个元素迟到了，但是这个元素的window是[6000,9000)，window end=8999 > watermar 7000，因此这个元素实际是不会被忽略的
        Tuple4<String, Integer, Long, Long> tuple9 = new Tuple4<>("a", 12, 6500L, -1L);
        // store in window [6000,9000),fire
        Tuple4<String, Integer, Long, Long> tuple10 = new Tuple4<>("a", 5, 7100L, 10000L);
        // window end 8999,watermark 10000,drop
        Tuple4<String, Integer, Long, Long> tuple11 = new Tuple4<>("a", 27, 8900L, -1L);
        // window [9000,12000)
        Tuple4<String, Integer, Long, Long> tuple12 = new Tuple4<>("a", 12, 9100L, -1L);
        // window [9000,12000) fire
        Tuple4<String, Integer, Long, Long> tuple13 = new Tuple4<>("a", 16, 9300L, 13000L);


        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                for (Tuple4<String, Integer, Long, Long> tuple : new Tuple4[]{tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8, tuple9, tuple10, tuple11, tuple12, tuple13}) {
                    ctx.collectWithTimestamp(new Tuple2<>(tuple.f0, tuple.f1), tuple.f2);
                    if (tuple.f3 > 0) {
                        ctx.emitWatermark(new Watermark(tuple.f3));
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = dataStreamSource.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1));

        reduceStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * 在看window算子时，主要考虑以下两点：
     * 1.window end + allowLateness <= watermark，true的话丢掉这个元素
     * 2.否则看window end <= watermark，true则窗口fire，false则窗口不会fire
     */
    @Test
    public void testAllowLate() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // window [0,3000),2999>LONG.MIN,not drop,not fire. elements:t1
        Tuple3<Integer, Long, Long> tuple1 = new Tuple3<>(35, 1000L, -1L);
        // window [0,3000) ,2999>LONG.MIN,not drop,not fire. elements:t1,t2
        Tuple3<Integer, Long, Long> tuple2 = new Tuple3<>(11, 100L, -1L);
        // window [0,3000) ,2999>LONG.MIN,not drop,not fire. elements:t1,t2,t3
        // emit watermark 6100,fire window [0,3000):t1,t2,t3
        Tuple3<Integer, Long, Long> tuple3 = new Tuple3<>(13, 2800L, 6100L);
        // window [0,3000) window end 2999 + 2000<=6100,drop
        Tuple3<Integer, Long, Long> tuple4 = new Tuple3<>(2, 1500L, -1L);
        // window end 2999 + 2000 <= 6100,drop
        Tuple3<Integer, Long, Long> tuple5 = new Tuple3<>(19, 2500L, -1L);
        // 1.window end 5999 + 2000 > watermark 6100,not drop
        // 2.window end 5999 < watermark 6100,window fire:t6
        Tuple3<Integer, Long, Long> tuple6 = new Tuple3<>(22, 3100L, -1L);
        // 5999 + 2000 >= 6100 not drop , 5999<=6100,window fire:t6,t7
        Tuple3<Integer, Long, Long> tuple7 = new Tuple3<>(18, 4700L, -1L);
        // 5999 + 2000 >= 6100 not drop , 5999 <= 6100,window fire:t6,t7,t8
        Tuple3<Integer, Long, Long> tuple8 = new Tuple3<>(17, 5900L, -1L);
        // window [6000,9000),8999>6100,not drop,not fire. t9
        Tuple3<Integer, Long, Long> tuple9 = new Tuple3<>(29, 6000L, -1L);
        // window [6000,9000),8999>6100,not drop,not fire t9,t10
        Tuple3<Integer, Long, Long> tuple10 = new Tuple3<>(16, 8000L, -1L);
        // window [9000,12000),11999>6100,not drop. 11999>6100,not fire t11
        Tuple3<Integer, Long, Long> tuple11 = new Tuple3<>(77, 9100L, -1L);
        // window [9000,12000),11999>6100,not drop. 11999>6100,not fire t11,t12
        Tuple3<Integer, Long, Long> tuple12 = new Tuple3<>(65, 9500L, -1L);
        // window [9000,12000),11999>6100,not drop. 11999>6100,not fire t11,t12,t13
        // emit wartermark 13000,[9000,12000) fire,elements:t11,t12,t13.[6000,9000) fire,elements:t9,t10
        Tuple3<Integer, Long, Long> tuple13 = new Tuple3<>(14, 11000L, 13000L);
        // window end 2999 + 2000 <= 13000,drop
        Tuple3<Integer, Long, Long> tuple14 = new Tuple3<>(89, 100L, -1L);
        // window end 5999 + 2000 <= 13000,drop
        Tuple3<Integer, Long, Long> tuple15 = new Tuple3<>(72, 5500L, -1L);
        // window end 8999 + 2000 <= 13000,drop
        Tuple3<Integer, Long, Long> tuple16 = new Tuple3<>(32, 7900L, -1L);
        // window end 11999 + 2000 >= 13000,not drop. 11999<=13000 window fire,elements: t11,t12,t13,t17
        Tuple3<Integer, Long, Long> tuple17 = new Tuple3<>(10, 11500L, -1L);

        DataStreamSource<Integer> dataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (Tuple3<Integer, Long, Long> tuple :
                        new Tuple3[]{tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8, tuple9, tuple10, tuple11, tuple12, tuple13, tuple14, tuple15, tuple16, tuple17}) {
                    ctx.collectWithTimestamp(tuple.f0, tuple.f1);
                    if (tuple.f2 > 0) {
                        ctx.emitWatermark(new Watermark(tuple.f2));
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<String> aggregateStream = dataStreamSource.windowAll(TumblingEventTimeWindows.of(Time.seconds(3))).allowedLateness(Time.seconds(2))
                .aggregate(new AggregateFunction<Integer, StringJoiner, String>() {
                    @Override
                    public StringJoiner createAccumulator() {
                        return new StringJoiner(",");
                    }

                    @Override
                    public StringJoiner add(Integer value, StringJoiner accumulator) {
                        return accumulator.add(value.toString());
                    }

                    @Override
                    public String getResult(StringJoiner accumulator) {
                        return accumulator.toString();
                    }

                    @Override
                    public StringJoiner merge(StringJoiner a, StringJoiner b) {
                        a.merge(b);
                        return a;
                    }
                });
        aggregateStream.print().setParallelism(1);
        streamExecutionEnvironment.execute();
    }

    @Test
    public void testLateSide() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 假设window size=5000,allow late=2000
        // window [0,5000) watermark Long.MIN not drop,not fire t1
        Tuple3<Integer, Long, Long> tuple1 = Tuple3.of(1, 1100L, -1L);
        // window [0,5000) not fire not drop t1,t2
        Tuple3<Integer, Long, Long> tuple2 = Tuple3.of(2, 300L, -1L);
        // window [5000,10000) watermark Long.Min not drop not fire t3
        Tuple3<Integer, Long, Long> tuple3 = Tuple3.of(3, 5100L, -1L);
        // window [0,5000) not drop not fire t1,t2,t4
        Tuple3<Integer, Long, Long> tuple4 = Tuple3.of(4, 4600L, -1L);
        // window [0,5000) not drop not fire t1,t2,t4,t5
        Tuple3<Integer, Long, Long> tuple5 = Tuple3.of(5, 2900L, -1L);
        // window [10000,15000) not drop not fire t6
        Tuple3<Integer, Long, Long> tuple6 = Tuple3.of(6, 13000L, -1L);
        // window [5000,10000) not drop not fire t3,t7
        // emit watermark 11000,[0,5000) fire t1,t2,t4,t5 [5000,10000) fire t3,t7
        Tuple3<Integer, Long, Long> tuple7 = Tuple3.of(7, 9100L, 11000L);
        // window end 4999 + 2000 <= 11000,drop
        Tuple3<Integer, Long, Long> tuple8 = Tuple3.of(8, 3000L, -1L);
        // window end 9999 + 2000 > 11000,not drop 9999< 11000,fire t3,t7,t9
        Tuple3<Integer, Long, Long> tuple9 = Tuple3.of(9, 6700L, -1L);
        // window end 9999 + 2000 > 11000,not drop,9999<11000,fire t3,t7,t9,t10
        // emit watermark 18000,[10000,15000) fire t6
        Tuple3<Integer, Long, Long> tuple10 = Tuple3.of(10, 9900L, 18000L);
        // window end 14999 + 2000<=18000,drop
        Tuple3<Integer, Long, Long> tuple11 = Tuple3.of(11, 13000L, -1L);
        // window end 9999 + 2000<=18000,drop
        Tuple3<Integer, Long, Long> tuple12 = Tuple3.of(12, 8888L, -1L);
        // window end 14999 + 2000<=18000,drop
        Tuple3<Integer, Long, Long> tuple13 = Tuple3.of(13, 12000L, -1L);
        // window end 19999 > 18000,not drop not fire,t14
        Tuple3<Integer, Long, Long> tuple14 = Tuple3.of(14, 15300L, -1L);
        // window end 19999 > 18000,not drop not fire,t14,t15
        Tuple3<Integer, Long, Long> tuple15 = Tuple3.of(15, 17200L, -1L);
        // window end 4999 + 2000<=18000,drop
        Tuple3<Integer, Long, Long> tuple16 = Tuple3.of(16, 1300L, -1L);
        // window end 24999>18000,not drop not fire t17
        Tuple3<Integer, Long, Long> tuple17 = Tuple3.of(17, 21000L, -1L);
        // window end 24999>18000,not drop not fire t17,t18
        Tuple3<Integer, Long, Long> tuple18 = Tuple3.of(18, 23000L, -1L);
        // window end 9999 +2000<=18000,drop
        // emit watermark 26000,[20000,25000) fire t17,t18,[15000,20000) fire t14,t15
        Tuple3<Integer, Long, Long> tuple19 = Tuple3.of(19, 5000L, 26000L);
        // window end 19999 + 2000 <26000, drop
        Tuple3<Integer, Long, Long> tuple20 = Tuple3.of(20, 19000L, -1L);

        DataStreamSource<Integer> dataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (Tuple3<Integer, Long, Long> tuple :
                        new Tuple3[]{tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8, tuple9, tuple10, tuple11
                                , tuple12, tuple13, tuple14, tuple15, tuple16, tuple17, tuple18, tuple19, tuple20}) {
                    ctx.collectWithTimestamp(tuple.f0, tuple.f1);
                    if (tuple.f2 > 0) {
                        ctx.emitWatermark(new Watermark(tuple.f2));
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });
        OutputTag<Integer> lateTag = new OutputTag<>("late", Types.INT);
        // 所有被丢弃的元素会被发送到旁路流中（如果设置了sideOutputLateData的话）
        SingleOutputStreamOperator<String> aggregateStream = dataStreamSource.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(2)).sideOutputLateData(lateTag)
                .aggregate(new AggregateFunction<Integer, StringJoiner, String>() {
                    @Override
                    public StringJoiner createAccumulator() {
                        return new StringJoiner(",", "(", ")");
                    }

                    @Override
                    public StringJoiner add(Integer value, StringJoiner accumulator) {
                        accumulator.add(value.toString());
                        return accumulator;
                    }

                    @Override
                    public String getResult(StringJoiner accumulator) {
                        return accumulator.toString();
                    }

                    @Override
                    public StringJoiner merge(StringJoiner a, StringJoiner b) {
                        a.merge(b);
                        return a;
                    }
                });

        aggregateStream.print("output:").setParallelism(5);
        // 获取旁路流中的数据，在这里是获取被丢弃的数据
        DataStream<Integer> lateStream = aggregateStream.getSideOutput(lateTag);
        lateStream.print("late output:").setParallelism(2);
        streamExecutionEnvironment.execute();
    }

    @Test
    public void test1() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = streamExecutionEnvironment.fromElements(Tuple2.of("hello", 0L), Tuple2.of("world", 1L), Tuple2.of("nice", 6L), Tuple2.of("girl", 3L), Tuple2.of("haha", 2L));
        SingleOutputStreamOperator<String> stringStream = tuple2DataStreamSource.map(tuple -> tuple.f0).returns(String.class);
        stringStream.print();


        streamExecutionEnvironment.execute();
    }
}
