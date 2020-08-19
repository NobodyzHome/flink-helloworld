package com.mzq.usage.flink;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class HelloWorldFlinkTest {

    @Test
    public void testMap() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> wordSource = source.flatMap((word, collector) -> Stream.of(word.split(" ")).forEach(collector::collect), Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleSource = wordSource.map(word -> new Tuple2<>(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = tupleSource.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumSource = tuple2TupleKeyedStream.sum(1);
        sumSource.print();

        streamExecutionEnvironment.execute();
    }

    @Test
    public void testAllWindow() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> wordSource = source.flatMap((word, collector) -> Stream.of(word.split(" ")).forEach(collector::collect), Types.STRING);
        AllWindowedStream<String, GlobalWindow> allWindowedStream = wordSource.countWindowAll(5);
        SingleOutputStreamOperator<String> apply = allWindowedStream.apply(new AllWindowFunction<String, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<String> values, Collector<String> out) throws Exception {
                String join = String.join(",", values);
                out.collect(join);
            }
        });
        apply.print();
        streamExecutionEnvironment.execute();
    }

    @Test
    public void testKeyBy() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = executionEnvironment.fromElements(WordCountData.WORDS);

        SingleOutputStreamOperator<String> flatMapSource = dataSource.<String>flatMap((value, collector)
                -> Stream.of(value.split(" ")).forEach(collector::collect)).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMapSource.map(word -> new Tuple2<>(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);
        // KeyedStream的聚合操作是滚动的操作（rolling），不是把一个分组上的所有元素聚合成一个元素，而是在每个元素上体现聚合结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        sum.print();

        executionEnvironment.execute();
    }

    @Test
    public void testKeyWindow() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        // 在这里我们使用让process算子既完成把句子拆分成单词的工作，也完成把单词转换成Tuple的工作，其实这样的效率并不好，应该让两个算子去做，提高加工程序的效率
        SingleOutputStreamOperator<Tuple2<String, Integer>> processStream = dataStreamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) {
                Stream.of(value.split(" ")).map(word -> new Tuple2<>(word, 1)).forEach(out::collect);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = processStream.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> windowedStream = keyedStream.countWindow(3);
        /*
         * 使用window进行聚合需要注意：
         * 1.只有在window的数据集齐了后，才会进行一次聚合计算操作。例如window的size是5，然而数据源中某个key只有3个元素，那么这个窗口装不满，则永远不会计算这个key的聚合结果
         * 2.和KeyedStream的聚合操作不同的是，WindowedStream在对窗口里的数据进行聚合后，只会产生一个计算结果，即这个窗口的聚合结果，把这个元素下发给下一个算子
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = windowedStream.sum(1).setParallelism(3);
        DataStreamSink<Tuple2<String, Integer>> printSink = sumStream.print();
        printSink.setParallelism(2);

        streamExecutionEnvironment.execute();
    }

    @Test
    public void testReduce() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapStream = dataStreamSource.flatMap(((value, out)
                -> Stream.of(value.split(" ")).forEach(out::collect)), Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMapStream.map(word -> new Tuple2<>(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> windowedStream = keyedStream.countWindow(5);
        // reduce相对于聚合函数sum、min这些，提供了相对底层的实现，使用户自己决定如何对数据进行聚合。同时与聚合函数一样，reduce产生的数据和原数据流中的数据类型相同，不会改变数据类型。
        // 例如聚合前，数据流中数据的类型是Tuple2，那么聚合后产生的新数据流，它的数据类型也是Tuple2。这点是聚合函数与apply的本质区别。
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = windowedStream.reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1));
        reduce.print();

        streamExecutionEnvironment.execute();
    }

    @Test
    public void testWindowApply() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        // SingleOutputStreamOperator代表经过一个transform算子后产生的流，可以使用该流来为这个transform算子设置名称，并行度等
        // 以下面这行为例，DataStream经过flatMap算子后获得SingleOutputStreamOperator，我们可以使用它设置并行度，从而来设置flatMap算子的并行度
        SingleOutputStreamOperator<String> flatMapStream = dataStreamSource.flatMap((word, collector) -> Stream.of(word.split(" ")).forEach(collector::collect), Types.STRING).setParallelism(3);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = flatMapStream.map(value -> new Tuple2<>(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).setParallelism(2);
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> windowedStream = keyedStream.countWindow(6);
        // WindowedStream的apply方法与聚合方法不同的是，聚合方法返回的是和流中数据相同类型的数据流，而apply方法可以返回一种新的数据类型的数据流
        SingleOutputStreamOperator<String> applyStream = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
                String word = tuple.getField(0);
                int count = 0;
                for (Tuple2<String, Integer> tupleIn : input) {
                    count += tupleIn.f1;
                }
                out.collect(word + ":" + count);
            }
        }).setParallelism(2);
        DataStreamSink<String> printSink = applyStream.print();
        printSink.setParallelism(3);
        streamExecutionEnvironment.execute();
    }

    @Test
    public void testMax() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapStream = dataStreamSource.<String>flatMap((value, out) -> Stream.of(value.split(" ")).forEach(out::collect)).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = flatMapStream.map(value -> new Tuple2<>(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> apply = keyedStream.timeWindow(Time.milliseconds(100), Time.milliseconds(50)).apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                int count = 0;
                for (Tuple2<String, Integer> t : input) {
                    count += t.f1;
                }
                out.collect(new Tuple2<>(tuple.getField(0), count));
            }
        });
        apply.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * union、connect、join都是将两个流合成一个流。它们的主要区别是：
     * 1.union是合并相同数据类型的流，将数据混在一起，由一个方法来处理，就和普通的处理一个流是一样的
     * 2.connect可以合并数据类型不同的流，然后计算算子在处理时，会把第一个流的数据交给方法1来处理，第二个流的数据交给方法2来处理，也就是说计算算子在处理时针对流的不同调用不同的方法
     * 3.join也可以合并数据类型不同的流，但不同于connect，join必须要找到两个流中相同key的元素，然后将这两个元素交给同一个方法来处理。因此join相比connect不同的是，是把两个流的可以凑成一对的数据交给同一个方法
     */
    @Test
    public void testUnion() throws Exception {
        Path path = Paths.get("/Users/maziqiang/logs/README.txt");
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 流1
        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        // 流2
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.readTextFile(path.toUri().toString());
        // 合流
        DataStream<String> unionStream = stringDataStreamSource.union(dataStreamSource);

        SingleOutputStreamOperator<String> flatMapStream = unionStream.<String>flatMap((value, out) -> {
            String trimValue = StringUtils.trimToEmpty(value);
            Stream.of(trimValue.split(" ")).filter(StringUtils::isNotBlank).forEach(out::collect);
        }).returns(Types.STRING).setParallelism(3);

        SingleOutputStreamOperator<String> filterStream = flatMapStream.filter(value -> !value.startsWith("https") && !value.contains("--")).setParallelism(2);
        SingleOutputStreamOperator<String> mapStream = filterStream.map(value -> StringUtils.remove(value, ",")).setParallelism(2);
        AllWindowedStream<String, GlobalWindow> allWindowedStream = mapStream.countWindowAll(10);
        SingleOutputStreamOperator<String> applyStream = allWindowedStream.apply(new AllWindowFunction<String, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<String> values, Collector<String> out) throws Exception {
                String statement = String.join(" --- ", values);
                out.collect(statement);
            }
        });

        DataStreamSink<String> streamSink = applyStream.print();
        streamSink.setParallelism(2);
        streamExecutionEnvironment.execute();
    }

    @Test
    public void testJoin() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream1 = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapStream1 = dataStream1.flatMap((String value, Collector<String> out) -> Stream.of(value.split(" ")).forEach(out::collect)).returns(Types.STRING).setParallelism(3);
        SingleOutputStreamOperator<String> fliterStream1 = flatMapStream1.filter(value -> !value.startsWith("http") && !value.contains("--")).setParallelism(2);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream1 = fliterStream1.map(value -> new Tuple2<>(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).setParallelism(2);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream1 = mapStream1.keyBy(0).sum(1).setParallelism(3);

        DataStreamSource<String> dataStream2 = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapStream2 = dataStream2.<String>flatMap((value, out) -> Stream.of(value.split(" ")).forEach(out::collect)).returns(Types.STRING).setParallelism(3);
        SingleOutputStreamOperator<Tuple2<String, LocalDateTime>> mapStream2 = flatMapStream2.map(value -> new Tuple2<>(value, LocalDateTime.now().plusDays(RandomUtils.nextInt(1, 200)))).returns(Types.TUPLE(Types.STRING, Types.LOCAL_DATE_TIME)).setParallelism(2);

        JoinedStreams<Tuple2<String, Integer>, Tuple2<String, LocalDateTime>> joinedStream = sumStream1.join(mapStream2);
        DataStream<Tuple3<String, Integer, LocalDateTime>> joinedApplyStream = joinedStream.where(tuple -> tuple.f0, Types.STRING).equalTo(tuple -> tuple.f0, Types.STRING).window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .apply((first, second) -> new Tuple3<>(first.f0, first.f1, second.f1), Types.TUPLE(Types.STRING, Types.INT, Types.LOCAL_DATE_TIME));

        DataStreamSink<Tuple3<String, Integer, LocalDateTime>> printSink = joinedApplyStream.print();
        printSink.setParallelism(3);
        streamExecutionEnvironment.execute();
    }

    @Test
    public void testConnect() throws Exception {
        class StrData {
            private String word;
            private Integer id;
            private LocalDateTime localDateTime;

            public StrData() {
            }

            public String getWord() {
                return word;
            }

            public void setWord(String word) {
                this.word = word;
            }

            public Integer getId() {
                return id;
            }

            public void setId(Integer id) {
                this.id = id;
            }

            public LocalDateTime getLocalDateTime() {
                return localDateTime;
            }

            public void setLocalDateTime(LocalDateTime localDateTime) {
                this.localDateTime = localDateTime;
            }

            @Override
            public String toString() {
                return String.format("word=%s,id=%d,localDateTime=%s", getWord(), getId(), getLocalDateTime());
            }
        }

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream1 = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapStream1 = dataStream1.<String>flatMap((value, out) -> Stream.of(value.split(" ")).forEach(out::collect)).returns(Types.STRING).setParallelism(3);
        SingleOutputStreamOperator<String> filterStream1 = flatMapStream1.filter(value -> StringUtils.isNoneBlank(value) && !value.contains("--")).setParallelism(3);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream1 = filterStream1.map(value -> new Tuple2<>(value, RandomUtils.nextInt(1, 10000))).returns(Types.TUPLE(Types.STRING, Types.INT)).setParallelism(2);
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream1 = mapStream1.keyBy(0);

        DataStreamSource<String> dataStream2 = streamExecutionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapStream2 = dataStream2.<String>flatMap((value, out) -> Stream.of(value.split(" ")).forEach(out::collect)).returns(Types.STRING).setParallelism(3);
        SingleOutputStreamOperator<Tuple2<String, LocalDateTime>> mapStream2 = flatMapStream2.map(value -> new Tuple2<>(value, LocalDateTime.now())).returns(Types.TUPLE(Types.STRING, Types.LOCAL_DATE_TIME)).setParallelism(2);
        KeyedStream<Tuple2<String, LocalDateTime>, Tuple> keyedStream2 = mapStream2.keyBy(0);

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, LocalDateTime>> connectStream = keyedStream1.connect(keyedStream2);
        SingleOutputStreamOperator<StrData> mapStream = connectStream.process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, LocalDateTime>, StrData>() {

            private ValueState<StrData> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<StrData> descriptor = new ValueStateDescriptor<>("state1", StrData.class);
                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<StrData> out) throws Exception {
                StrData stateData = valueState.value();
                if (Objects.isNull(stateData)) {
                    stateData = new StrData();
                }
                stateData.setWord(value.f0);
                stateData.setId(value.f1);
                valueState.update(stateData);

                out.collect(stateData);
            }

            @Override
            public void processElement2(Tuple2<String, LocalDateTime> value, Context ctx, Collector<StrData> out) throws Exception {
                StrData stateData = valueState.value();
                if (Objects.isNull(stateData)) {
                    stateData = new StrData();
                }
                stateData.setWord(value.f0);
                stateData.setLocalDateTime(value.f1);
                valueState.update(stateData);

                out.collect(stateData);
            }
        }).setParallelism(3);

        KeyedStream<StrData, String> keyedStream = mapStream.keyBy(new KeySelector<StrData, String>() {
            @Override
            public String getKey(StrData value) throws Exception {
                return value.getWord();
            }
        });

        WindowedStream<StrData, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.milliseconds(15));
        SingleOutputStreamOperator<StrData> applyStream = windowedStream.apply(new WindowFunction<StrData, StrData, String, TimeWindow>() {
            @Override
            public void apply(String tuple, TimeWindow window, Iterable<StrData> input, Collector<StrData> out) throws Exception {
                StrData strDataNew = new StrData();
                for (StrData strData : input) {
                    strDataNew.setId(strData.getId());
                    strDataNew.setWord(strData.getWord());
                    strDataNew.setLocalDateTime(strData.getLocalDateTime());
                }
                out.collect(strDataNew);
            }
        }).setParallelism(5);

        applyStream.print();
        streamExecutionEnvironment.execute();
    }

    @Test
    public void testSideStream() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = executionEnvironment.fromElements(WordCountData.WORDS);
        SingleOutputStreamOperator<String> flatMapSource = dataStreamSource.<String>flatMap((value, out) -> Stream.of(value.split(" ")).forEach(out::collect)).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> process = flatMapSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                int counterValue = RandomUtils.nextInt(1, 30);
                Tuple2<String, Integer> element = new Tuple2<>(value, counterValue);
                out.collect(element);

                if (counterValue < 10) {
                    OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<>("side", Types.TUPLE(Types.STRING, Types.INT));
                    ctx.output(outputTag, element);
                }
            }
        });
        DataStream<Tuple> sideStream = process.getSideOutput(new OutputTag<>("side", Types.TUPLE(Types.STRING, Types.INT)));
        sideStream.print();

        executionEnvironment.execute();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testKeyedStreamFold() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = executionEnvironment.addSource(new ParallelSourceFunction<Tuple2<String, Integer>>() {
            private boolean running;

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                running = true;

                while (running) {
                    String category = "类别" + RandomStringUtils.random(1, "ABCDEF");
                    int sales = RandomUtils.nextInt(200, 2000);

                    ctx.collect(new Tuple2<>(category, sales));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(5);

        // keyed stream可以用于对相同key的数据进行一个累加操作（rolling）
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = dataStreamSource.keyBy(0);
        // fold方法虽然被弃用了，但是目前没有keyed stream的方法中没有能代替该方法的。fold方法与sum、max等方法的相同点是对同一个key下的元素进行滚动累加操作，但fold不同的是它可以产生一个新的数据类型的流，而sum、max等只能产生和原数据流相同类型的数据流
        // 例如下面使用fold方法，将Tuple2<String,Integer>数据类型的流转换成了Tuple3<String,Integer,Integer>，其目的是为了既存储当前产品的当前销售额（存储在Tuple3的f1属性中），又存储当前产品的累计销售额（存储在Tuple3的f2属性中）
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> foldStream = keyedStream.fold(new Tuple3<>("", 0, 0), (accumulator, value) -> {
            accumulator.f0 = value.f0;
            accumulator.f1 = value.f1;
            accumulator.f2 = Optional.of(accumulator).map(tuple -> tuple.f2).orElse(0) + value.f1;

            return accumulator;
        }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT)).setParallelism(3);

        foldStream.print().setParallelism(2);
        executionEnvironment.execute();
    }

    @Test
    public void testInterative() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        DataStreamSource<Integer> longDataStreamSource = streamExecutionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        IterativeStream<Integer> iterate = longDataStreamSource.iterate();
        SingleOutputStreamOperator<Integer> map = iterate.map(value -> value + 1);
        SingleOutputStreamOperator<Integer> filter = map.filter(value -> value <= 5);
        filter.print("feedback:");

        iterate.closeWith(filter);
        iterate.filter(value -> value > 5).print("result:");
        streamExecutionEnvironment.execute();
    }
}
