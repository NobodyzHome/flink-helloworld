package com.mzq.usage.flink;

import com.mzq.usage.flink.domain.*;
import com.mzq.usage.flink.func.source.WaybillCSource;
import com.mzq.usage.flink.func.source.WaybillESource;
import com.mzq.usage.flink.func.source.WaybillMSource;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Stream;

/**
 * 为什么要使用窗口？
 * 我们知道，数据流的定义是流中的数据是无限的，源源不断地有数据进入到流中。因此，我们不可能对流中的所有数据求和。但是我们可以使用窗口将无限数据的流拆分成一段一段的有限数据，
 * 然后对这一段一段的数据进行汇总。我们把存储这一段有限数据的东西称作窗口。
 * 比如我无法汇总一个摄像头输出的所有数据（因为摄像头每时每刻都会产生违章拍摄数据），但是我可以汇总这个摄像头每分钟产生的数据（因为只处理一分钟的数据时，
 * 每一个窗口中的数据就有了边界，当窗口收集齐了这个边界下的所有数据后，就可以进行汇总）
 * <p>
 * 窗口的使用允许流不再是来一个元素就立即处理一个元素，而是变为一批相关的元素（即窗口中的所有元素）来齐了以后再统一处理
 * <p>
 * 当想象开窗时，我们要确认每个元素到来的时间点（比如第1秒到来的，第2秒到来的）以及窗口的大小（比如窗口大小是3秒），这样我们就知道每个窗口有哪些元素以及窗口聚合后产生的新元素所在的时间点
 * （例如3秒一个窗口fire，那么第一个窗口聚合后的结果所在的时间点应该是第3秒之后）。可以用笔画一个时间轴来看下。
 * <p>
 * 使用window算子时，使元素暂存在window中，不立即下发到下游，只有在window fire后，window中的元素才会经过window function生成计算结果，发送给下游
 */
public class WindowTest {

    /**
     * 说到窗口，比较重要的内容是：
     * 1.数据流是否key by，如果key by了，那么对每一个key的数据都可以开窗，因此对窗口的计算可以是并行的（即窗口计算算子的并行度大于1）。如果没有key by，那么计算算子的并行度只能是1
     * 2.WindowAssigner，决定元素加入到哪个窗口。并且WindowAssigner的defaultTrigger决定了元素加入到窗口后是否需要fire（如果没有单独设置trigger的话）
     * 3.Trigger，决定窗口在新加入元素时是否需要fire或何时需要fire
     * 4.Evictor，当trigger反馈当前窗口需要fire后，会把窗口中所有数据发送给evictor，evictor可以在将数据发送给计算函数之前或之后对窗口的元素进行删除
     * （注意：不是窗口的元素交给计算函数处理之后就需要把这些元素从窗口中清除，是否清除窗口的元素取决于trigger是否反馈了purge）
     * 5.计算函数，ReduceFunction、WindowFunction等，用于对窗口中的元素进行聚合计算，并将结果传递给下游算子
     * <p>
     * GlobalWindows的特点：
     * 1.它没有固定的大小
     * 2.它创建的窗口自身是不会Fire的，也就是说使用GlobalWindows时需要我们自己指定trigger、evictor
     * 3.它创建的窗口自身也没有计算函数，也需要我们来指定
     */
    @Test
    public void testGlobalWindowAssigner() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapDataSource = stringDataStreamSource.<String>flatMap((value, out) -> Stream.of(value.split(" ")).forEach(out::collect)).returns(Types.STRING);
        AllWindowedStream<String, GlobalWindow> allWindowedStream = flatMapDataSource.windowAll(GlobalWindows.create()).trigger(CountTrigger.of(2)).evictor(CountEvictor.of(5));
        SingleOutputStreamOperator<String> applyStream = allWindowedStream.apply(new AllWindowFunction<String, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<String> values, Collector<String> out) throws Exception {
                out.collect(String.join(" + ", values));
            }
        });
        applyStream.print();

        streamExecutionEnvironment.execute();
    }

    /**
     * TumblingWindows的特点：
     * 1.窗口的大小是固定的
     * 2.窗口与窗口间的元素是不重叠的，即window assigner把每个元素都只会放到一个窗口中
     * 3.创建的窗口有默认的trigger，即处理时间到达窗口size就fire窗口
     * <p>
     * 在确定开窗前，一个很重要的内容是确定是否对流进行key by，如果key by了，那么窗口计算的算子可以使用多个并行度，否则算子的并行度只能是1。
     * 假设TumblingWindows的window size是2秒，那么TumblingWindows会把所有时间戳在[0,2)之间的元素都放到[0,2)窗口中，把所有时间戳在[2,3)秒的元素
     * 放到[2,3)窗口中。
     * 例如：TumblingWindows size=5；
     * e1(timestamp=2) => 窗口[0,5)
     * e2(timestamp=3) => 窗口[0,5)
     * e1(timestamp=4) => 窗口[0,5)
     * e1(timestamp=9) => 窗口[5,10)
     * e1(timestamp=11) => 窗口[10,15)
     */
    @Test
    public void testTumblingWindowAssigner() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStreamSource = streamExecutionEnvironment.addSource(new RichSourceFunction<Integer>() {

            private IntCounter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                counter = getRuntimeContext().getIntCounter("counter");
            }

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {

                while (true) {
                    counter.add(1);
                    ctx.collect(counter.getLocalValue());
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        AllWindowedStream<Integer, TimeWindow> allWindowedStream = dataStreamSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)));
        SingleOutputStreamOperator<Integer> reduceStream = allWindowedStream.reduce(Integer::sum);
        reduceStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * SlidingWindows的特点：
     * 1.窗口的大小是固定的
     * 2.窗口与窗口间的元素有可能重叠（当window size>slide size），也就是说window assigner有可能把一个元素分配给多个窗口
     * 3.创建的窗口有默认的trigger，即处理时间到达slide就fire窗口
     */
    @Test
    public void testSlidingWindowAssigner() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Integer>> dataStreamSource = streamExecutionEnvironment.addSource(new RichSourceFunction<Tuple2<Integer, Integer>>() {

            private IntCounter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                counter = getRuntimeContext().getIntCounter("counter");
            }

            @Override
            public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

                while (true) {
                    counter.add(1);
                    ctx.collect(new Tuple2<>(RandomUtils.nextInt(1, 5), counter.getLocalValue()));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        /*
         * 当给WindowAssigner设置了evictor，那么则阻止了增量聚合的发生。因为需要收集齐窗口的所有元素，然后交给evictor进行剔除，只有剔除后的元素才会交给
         * window function进行计算。而增量聚合是希望窗口每来一个元素就聚合一下，这明显不能满足evictor的要求。
         * <p>
         * 注意：和Window的processFunction一样，一旦不能增量聚合，那么在flink内部就需要设置一个缓冲区，存储窗口的所有元素，当窗口fire时，会将缓冲区中
         * 所有元素一起交出去。这样的效率肯定没有增量的聚合好，因为增量聚合不需要记录窗口的每一个元素，而是来一个元素就聚合一下。
         */
        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = dataStreamSource.keyBy(0);
        WindowedStream<Tuple2<Integer, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .evictor(CountEvictor.of(3));
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> reduceStream = windowedStream.apply(new WindowFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                Object key = tuple.getField(0);
                int sum = 0;
                for (Tuple2<Integer, Integer> e : input) {
                    sum += e.f1;
                }
                out.collect(new Tuple2<>((Integer) key, sum));
            }
        }).setParallelism(10);
        reduceStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * session window的本质是一个可以合并窗口的处理，当产生一个元素时，如果gap秒后没有新元素，就会fire窗口，而在gap内如果有元素，就会把这两个窗口合并，形成一个新的窗口，这个窗口的fire时间自然也就就向后移了；
     * <p>
     * 举例来说，现在session window的gap是2，当第一个元素来的时候，那么窗口的start和end为[0,2)，如果在第2秒时没来新的元素，那么这个窗口就fire了。但是如果在第1秒又来了一个元素，那么会创建一个窗口[1,3)，
     * 由于这个窗口和[0,2)有交集，因此窗口就被合并了，合并为[0,3)，并且这个窗口有这两个元素。如果在第3秒后还没有元素来，合并后的窗口就会fire。否则当前这个窗口就会继续和新的窗口合并，扩展窗口中的元素。
     * 也就是说当新来一个元素时，这个元素的时间戳（在上面例子中是1）在已有的窗口的边界内（在上面例子中是[0,2)），那么窗口就会被延伸。而如果没有在边界内的话，上一个窗口就不会再扩展了，而是产生了一个新的窗口。
     * 总结：
     * 当使用session window的元素时，一个元素在进来后的gap秒内，如果没有新的元素，那么窗口就会fire。而在gap秒后如果有新的元素，那么窗口的fire时间就会被延伸，窗口中的元素也在扩展。
     * <p>
     * session window的场景：假设我有一个流，这个流里对于同一个运单的数据会在短时间内产生多条数据，为了不每一条数据来了都更新一次数据库，就使用session window，把数据产生间隔相近的数据都放在一个window里，
     * 直到在窗口的end时间内没有来这个运单的数据，这个窗口就会fire，这样只更新一次数据库。
     * 但session window也相应的有一个弊端，就是如果这个运单一直以较低的间隔的源源不断的产生，那么窗口则会越来越大，窗口中的元素也越来越多，导致这个窗口长时间不fire，这样一个是fire时元素太多了，处理起来慢，还有就是数据要很长时间才能落到库里。
     */
    @Test
    public void testSessionGlobalAssigner() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 假设session window的gap是2秒
        // 产生窗口[1000,3000)
        Tuple4<String, Integer, Long, Long> tuple1 = new Tuple4<>("a", 21, 1000L, -1L);
        // 产生窗口[1500,3500)，由于在gap内，和上一个元素合并成一个窗口，合并后的窗口为[1000,3500)
        Tuple4<String, Integer, Long, Long> tuple2 = new Tuple4<>("a", 32, 1500L, -1L);
        // 在时间戳3600时新来了一个元素，创建窗口[3600,5600)，由于不能和上个窗口合并，因此不能和上个窗口融合在一起。同时提交了水位线5500，超过了上个窗口的end，因此上个窗口就fire，窗口中共有两个元素
        Tuple4<String, Integer, Long, Long> tuple3 = new Tuple4<>("a", 13, 3600L, 5500L);

        // 注意：由于当前水位线已经到5500了，因此新元素的时间戳不能低于5500，否则会被认为是迟到的元素，不会传递至下游算子
        // [5600,7600)，由于key不同，因此不会考虑和key "a"的窗口合并，元素tuple7
        Tuple4<String, Integer, Long, Long> tuple4 = new Tuple4<>("b", 3, 5600L, -1L);
        // [6000,8000) => [5600,8000)，元素tuple4、tuple5
        Tuple4<String, Integer, Long, Long> tuple5 = new Tuple4<>("b", 9, 6000L, -1L);
        // [7500,9500) => [5600,9500)，元素tuple4、tuple5、tuple6
        Tuple4<String, Integer, Long, Long> tuple6 = new Tuple4<>("b", 22, 7500L, -1L);
        // [10000,12000)，元素tuple7
        Tuple4<String, Integer, Long, Long> tuple7 = new Tuple4<>("b", 50, 10000L, -1L);
        // [11000,13000) => [10000,13000)，元素tuple7,tuple8。event watermark 9500，[3600,5600)和[5600,9500) fire
        Tuple4<String, Integer, Long, Long> tuple8 = new Tuple4<>("b", 12, 11000L, 9500L);
        // [15000,17000)，元素tuple9。event watermark 13500，[10000,13000) fire
        Tuple4<String, Integer, Long, Long> tuple9 = new Tuple4<>("b", 5, 15000L, 13500L);

        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                for (Tuple4<String, Integer, Long, Long> tuple : new Tuple4[]{tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8, tuple9}) {
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

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer[]>> aggregateStream = dataStreamSource.keyBy(0).window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, Tuple3<String, Integer, Integer[]>>() {
                    @Override
                    public List<Tuple2<String, Integer>> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<Tuple2<String, Integer>> add(Tuple2<String, Integer> value, List<Tuple2<String, Integer>> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public Tuple3<String, Integer, Integer[]> getResult(List<Tuple2<String, Integer>> accumulator) {
                        String name = null;
                        Integer sum = 0, index = 0;
                        Integer[] history = new Integer[accumulator.size()];
                        for (Tuple2<String, Integer> tuple : accumulator) {
                            name = tuple.f0;
                            sum += tuple.f1;
                            history[index++] = tuple.f1;
                        }

                        return new Tuple3<>(name, sum, history);
                    }

                    @Override
                    public List<Tuple2<String, Integer>> merge(List<Tuple2<String, Integer>> a, List<Tuple2<String, Integer>> b) {
                        a.addAll(b);
                        return a;
                    }
                });
        aggregateStream.print();
        streamExecutionEnvironment.execute();
    }

    // reduce function是增量的对窗口中的元素进行聚合的函数，它的一个特点是function产生的数据和输入的数据的类型是相同的
    @Test
    public void testReduceFunction() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (true) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of("类别" + RandomStringUtils.random(1, "A"), RandomUtils.nextInt(100, 1000));
                    ctx.collect(tuple2);

                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = dataStreamSource.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1));

        reduceStream.print();
        streamExecutionEnvironment.execute();
    }

    // aggregate function是reduce function的一个通用处理，它和reduce function相同的是，它也是增量聚合，但不同点是它允许function输出的数据的类型和输入的数据类型不一致
    @Test
    public void testAggregateFunction() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    ctx.collect(RandomStringUtils.random(1, "ABCDEFGH"));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<ProductIncome> mapStream = dataStreamSource.map(productName -> new ProductIncome(productName, RandomUtils.nextInt(100, 1000)), Types.GENERIC(ProductIncome.class));
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer[]>> aggregateStream = mapStream.keyBy(ProductIncome::getProductName)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))).aggregate(
                        new AggregateFunction<ProductIncome, List<ProductIncome>, Tuple3<String, Integer, Integer[]>>() {
                            /**
                             * 创建一个累加器，在窗口被fire之前，将窗口中的元素存储在累加器中
                             */
                            @Override
                            public List<ProductIncome> createAccumulator() {
                                return new ArrayList<>(50);
                            }

                            /**
                             * 将窗口中的元素添加至累加器中
                             * value是当前往窗口中添加的元素 accumulator是累加器，也就是createAccumulator方法创建的累加器
                             */
                            @Override
                            public List<ProductIncome> add(ProductIncome value, List<ProductIncome> accumulator) {
                                accumulator.add(value);
                                return accumulator;
                            }

                            /**
                             * 当窗口被fire时，根据累加器中的内容计算出聚合的结果，用于传递给下一个算子
                             */
                            @Override
                            public Tuple3<String, Integer, Integer[]> getResult(List<ProductIncome> accumulator) {
                                String productName = null;
                                Integer sum = 0, index = 0;
                                Integer[] detail = new Integer[accumulator.size()];

                                for (ProductIncome productIncome : accumulator) {
                                    productName = productIncome.getProductName();
                                    sum += productIncome.getIncome();
                                    detail[index++] = productIncome.getIncome();
                                }
                                return new Tuple3<>(productName, sum, detail);
                            }

                            /**
                             * 当需要对累加器进行合并时，给出如何进行合并操作
                             */
                            @Override
                            public List<ProductIncome> merge(List<ProductIncome> a, List<ProductIncome> b) {
                                a.addAll(b);
                                return a;
                            }
                        }
                );

        aggregateStream.print().setParallelism(1);
        streamExecutionEnvironment.execute();
    }

    /**
     * 对窗口的计算函数中，process function与reduce、aggregate的主要区别是reduce、aggregate是增量聚合的，窗口中来一个元素就提前聚合一下，flink不需要记录窗口中都有哪些元素。
     * 而process function是全量的，它会在窗口fire之前，使用一个buffer来存储窗口中都有哪些元素，等窗口fire时再将这些元素全部交给process function。正是因为使用process function
     * 会导致flink创建一个buffer来存储窗口的全部元素，因此导致process function会比reduce、aggregate效率低。但使用process function可以通过入参的Context对象访问这个窗口的元数据
     */
    @Test
    public void testProcessWindowFunction() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        DataStreamSource<WaybillE> waybillEDataStreamSource = streamExecutionEnvironment.addSource(new WaybillESource());

        DataStream<WaybillCEM> waybillCEMDataStream = waybillCDataStreamSource.join(waybillEDataStreamSource).where(WaybillC::getWaybillCode).equalTo(WaybillE::getWaybillCode)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3))).apply(new JoinFunction<WaybillC, WaybillE, WaybillCEM>() {
                    @Override
                    public WaybillCEM join(WaybillC waybillC, WaybillE waybillE) throws Exception {
                        WaybillCEM waybillCEM = new WaybillCEM();
                        waybillCEM.setWaybillCode(waybillC.getWaybillCode());
                        waybillCEM.setWaybillSign(waybillC.getWaybillSign());
                        waybillCEM.setSiteCode(waybillC.getSiteCode());
                        waybillCEM.setSiteName(waybillC.getSiteName());
                        waybillCEM.setBusiNo(waybillE.getBusiNo());
                        waybillCEM.setBusiName(waybillE.getBusiName());
                        waybillCEM.setSendPay(waybillE.getSendPay());

                        return waybillCEM;
                    }
                });

        SingleOutputStreamOperator<String> windowApplyStream = waybillCEMDataStream.keyBy(WaybillCEM::getSiteCode).window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .process(new ProcessWindowFunction<WaybillCEM, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaybillCEM> elements, Collector<String> out) throws Exception {
                        StringJoiner joiner = new StringJoiner(",");
                        elements.forEach(element -> joiner.add(element.getWaybillCode()));
                        out.collect(String.format("%s:%s", key, joiner.toString()));
                    }
                });
        windowApplyStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * 现在我们知道了reduce、aggregate是增量聚合，process是全量聚合。增量聚合效率更加高一些，它不需要存储窗口的所有元素，而是来一个元素就提前聚合一下。全量聚合虽然缓冲了窗口的所有元素，但是process function中
     * 可以获得窗口的元数据，在一些高级别的操作中是很有用的。
     * <p>
     * 那么现在如果我既想用增量聚合（因为效率比全量聚合高），又想获取窗口的元数据，应该怎么办呢？
     * flink的reduce、aggregate方法都提供了reduce(reduce function,process function)、aggregate(aggregate function,process function)的方法，flink先使用reduce function进行增量聚合，然后将聚合结果
     * 传递给process function，由process function输出最终聚合的结果。此时process function的elements入参就只有reduce function聚合的结果值，也就是只有一个元素。
     */
    @Test
    public void testAggWithProcess() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        DataStreamSource<WaybillE> waybillEDataStreamSource = streamExecutionEnvironment.addSource(new WaybillESource());
        DataStreamSource<WaybillM> waybillMDataStreamSource = streamExecutionEnvironment.addSource(new WaybillMSource());

        SingleOutputStreamOperator<WaybillCEM> waybillCEMStream = waybillCDataStreamSource.keyBy(WaybillC::getWaybillCode)
                .connect(waybillEDataStreamSource.keyBy(WaybillE::getWaybillCode))
                .flatMap(new RichCoFlatMapFunction<WaybillC, WaybillE, WaybillCEM>() {
                    private ValueState<WaybillCEM> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<WaybillCEM> valueStateDescriptor = new ValueStateDescriptor<>("waybill", WaybillCEM.class);
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void flatMap1(WaybillC value, Collector<WaybillCEM> out) throws Exception {
                        WaybillCEM state = valueState.value();
                        boolean hasState = Objects.nonNull(state);
                        if (!hasState) {
                            state = new WaybillCEM();
                        }
                        state.setWaybillCode(value.getWaybillCode());
                        state.setSiteCode(value.getSiteCode());
                        state.setSiteName(value.getSiteName());
                        state.setWaybillSign(value.getWaybillSign());

                        valueState.update(state);
                        if (hasState) {
                            out.collect(state);
                        }
                    }

                    @Override
                    public void flatMap2(WaybillE value, Collector<WaybillCEM> out) throws Exception {
                        WaybillCEM waybillCEM = valueState.value();
                        boolean hasState = Objects.nonNull(waybillCEM);
                        if (!hasState) {
                            waybillCEM = new WaybillCEM();
                        }
                        waybillCEM.setWaybillCode(value.getWaybillCode());
                        waybillCEM.setBusiNo(value.getBusiNo());
                        waybillCEM.setBusiName(value.getBusiName());
                        waybillCEM.setSendPay(value.getSendPay());

                        valueState.update(waybillCEM);
                        if (hasState) {
                            out.collect(waybillCEM);
                        }
                    }
                }).setParallelism(5);

        SingleOutputStreamOperator<String> aggregateStream = waybillCEMStream.keyBy(WaybillCEM::getSiteName).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AggregateFunction<WaybillCEM, List<WaybillCEM>, String[]>() {
                    @Override
                    public List<WaybillCEM> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<WaybillCEM> add(WaybillCEM value, List<WaybillCEM> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public String[] getResult(List<WaybillCEM> accumulator) {
                        return accumulator.stream().map(WaybillCEM::getWaybillCode).toArray(String[]::new);
                    }

                    @Override
                    public List<WaybillCEM> merge(List<WaybillCEM> a, List<WaybillCEM> b) {
                        a.addAll(b);
                        return a;
                    }
                }, new ProcessWindowFunction<String[], String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<String[]> elements, Collector<String> out) throws Exception {
                        // 此时elements中只有aggregate function聚合结果的一条数据，因此直接使用next方法获取数据即可
                        String[] waybills = elements.iterator().next();
                        out.collect(String.format("%s: %s", key, String.join(",", waybills)));
                        if (waybills.length <= 2) {
                            OutputTag<String> outputTag = new OutputTag<>("lessSite", Types.GENERIC(String.class));
                            context.output(outputTag, key);
                        }
                    }
                }).setParallelism(5);

        OutputTag<String> outputTag = new OutputTag<>("lessSite", Types.GENERIC(String.class));
        DataStream<String> lessSisteStream = aggregateStream.getSideOutput(outputTag);
        aggregateStream.print("aggregate:").setParallelism(2);
        lessSisteStream.print("lessSite:").setParallelism(2);

        DataStream<WaybillCEM> waybillEMDataStream = waybillEDataStreamSource.join(waybillMDataStreamSource).where(WaybillE::getWaybillCode).equalTo(WaybillM::getWaybillCode)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .apply(new JoinFunction<WaybillE, WaybillM, WaybillCEM>() {
                    @Override
                    public WaybillCEM join(WaybillE waybillE, WaybillM waybillM) throws Exception {
                        WaybillCEM waybillCEM = new WaybillCEM();
                        waybillCEM.setWaybillCode(waybillE.getWaybillCode());
                        waybillCEM.setBusiNo(waybillE.getBusiNo());
                        waybillCEM.setBusiName(waybillE.getBusiName());
                        waybillCEM.setSendPay(waybillE.getSendPay());
                        waybillCEM.setPickupDate(waybillM.getPickupDate());
                        waybillCEM.setDeliveryDate(waybillM.getDeliveryDate());

                        return waybillCEM;
                    }
                });
        waybillEMDataStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * join有两种实现方式：一种是通过窗口的实现方式，还有一种是通过时间间隔的实现方式。通过窗口的实现方式是开一个窗口，找到窗口内可以凑成一对的两个元素，然后扔给apply算子。
     * 而时间间隔的实现方式不是通过窗口来实现的，它会根据A流的一个元素的时间戳，找到相对这个时间范围内的B流的元素。下面我们主要说窗口实现方式的join。
     * <p>
     * join相当于内连接，这就会发生：
     * 1.如果A流的一个元素和B流的多个元素有相同的key（在同一个窗口内），那么A流的这个元素和B流的这些元素进行多次连接。导致A流的这个元素的数据被计算多次
     * 2.如果A流的一个元素先来，在这个窗口内，具有相同key的B流的元素两秒后才来，会导致A流的这个元素在到达A流的两秒后才会被处理，增加了A流的这个元素的响应时间
     * 3.如果A流的一个元素不能和B流的元素有相同的key（在同一个窗口内），那么这个元素就不会被处理了，也就是A流的这个元素的处理就丢失了
     * <p>
     * 所以使用join进行连接操作是有一定的风险的：同一个数据被处理多次、数据处理的响应时间变长、数据处理丢失的问题。
     * 但它带来的好处是把两条不同流中的数据拉平成一条数据。
     */
    @Test
    public void testWindowJoin() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);

        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        DataStreamSource<WaybillE> waybillEDataStreamSource = streamExecutionEnvironment.addSource(new WaybillESource());
        DataStreamSource<WaybillM> waybillMDataStreamSource = streamExecutionEnvironment.addSource(new WaybillMSource());

        JoinedStreams<WaybillC, WaybillE> waybillCEStream = waybillCDataStreamSource.join(waybillEDataStreamSource);
        DataStream<WaybillCEM> waybillCEDataStream = waybillCEStream.where(WaybillC::getWaybillCode).equalTo(WaybillE::getWaybillCode)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .apply(new JoinFunction<WaybillC, WaybillE, WaybillCEM>() {
                    @Override
                    public WaybillCEM join(WaybillC waybillC, WaybillE waybillE) throws Exception {
                        WaybillCEM waybillCEM = new WaybillCEM();
                        waybillCEM.setWaybillCode(waybillC.getWaybillCode());
                        waybillCEM.setWaybillSign(waybillC.getWaybillSign());
                        waybillCEM.setSiteCode(waybillC.getSiteCode());
                        waybillCEM.setSiteName(waybillC.getSiteName());
                        waybillCEM.setBusiNo(waybillE.getBusiNo());
                        waybillCEM.setBusiName(waybillE.getBusiName());
                        waybillCEM.setSendPay(waybillE.getSendPay());

                        return waybillCEM;
                    }
                });

        DataStream<WaybillCEM> waybillCEMDataStream = waybillCEDataStream.join(waybillMDataStreamSource).where(WaybillCEM::getWaybillCode).equalTo(WaybillM::getWaybillCode)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .apply(new FlatJoinFunction<WaybillCEM, WaybillM, WaybillCEM>() {
                    @Override
                    public void join(WaybillCEM waybillCEM, WaybillM waybillM, Collector<WaybillCEM> out) throws Exception {
                        waybillCEM.setPickupDate(waybillM.getPickupDate());
                        waybillCEM.setDeliveryDate(waybillM.getDeliveryDate());

                        out.collect(waybillCEM);
                    }
                });

        SingleOutputStreamOperator<WaybillCEM> reduceStream = waybillCEMDataStream.keyBy(WaybillCEM::getSiteCode).reduce((value1, value2) -> {
            value2.setWaybillCode(String.join(",", value1.getWaybillCode(), value2.getWaybillCode()));
            return value2;
        });
        reduceStream.print();
        streamExecutionEnvironment.execute();
    }


}
