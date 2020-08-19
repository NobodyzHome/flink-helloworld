package com.mzq.usage.flink;

import com.mzq.usage.flink.domain.*;
import com.mzq.usage.flink.func.source.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

/**
 * 为什么要使用state?
 * state被称作为状态，而flink是处理有状态的数据流框架，那么为什么要使用state呢？在无状态的数据计算中，我们只需要根据当前元素本身的数据就可以计算出结果。但是在数据处理中，往往有这种情况，
 * 我们要计算所有已经处理的元素的总价。这样，在数据处理中，我们不仅要知道当前数据的总价，还要记录已处理的元素的价格之和。我们存储的这个价格之和就是总价，它提供了除当前数据以外的额外数据，
 * 用于计算函数获取这些数据进行处理。
 * 因此，我们使用state是想把state存储的内容和当前元素的内容都提供给计算函数，让计算函数可使用的内容更多。
 * <p>
 * 大部分使用state的场景都是在keyedStream中使用，这样流按照key被逻辑拆分，同时从这个流中获取的state也会按key来拆分。同一个key的元素共享同一个内容，也就是说不同key的state值是不同的。
 * 我们使用state，主要就是让算子记录过往经历过这个算子的数据，好让这个算子在新来元素时可以使用这些经过该算子的过往的数据和当前数据一起进行计算
 */
public class StateTest {

    /**
     * keyed state的ValueState的存储结构如下：
     * <p>
     * -----------------value
     * ------------------------------
     * key1       a single object
     * ------------------------------
     * key2       a single object
     * ------------------------------
     */
    @Test
    public void testValueState() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        KeyedStream<WaybillC, String> waybillCStringKeyedStream = waybillCDataStreamSource.keyBy(WaybillC::getSiteCode);
        SingleOutputStreamOperator<WaybillC> mapStream = waybillCStringKeyedStream.map(new RichMapFunction<WaybillC, WaybillC>() {
            private ValueState<WaybillC> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<WaybillC> valueStateDescriptor = new ValueStateDescriptor<>("site-waybill", Types.GENERIC(WaybillC.class));
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public WaybillC map(WaybillC value) throws Exception {
                // 从state中获取数据
                WaybillC latest = valueState.value();
                // 使用state和当前元素数据进行计算
                if (Objects.nonNull(latest)) {
                    value.setSiteWaybills(String.join(",", value.getWaybillCode(), latest.getSiteWaybills()));
                } else {
                    value.setSiteWaybills(value.getWaybillCode());
                }
                // 将计算结果更新至state
                valueState.update(value);
                return value;
            }
        });
        mapStream.print();

        DataStreamSource<ProductIncome> productSource = streamExecutionEnvironment.addSource(new ProductSource());
        SingleOutputStreamOperator<ProductIncome> filterStream = productSource.keyBy(ProductIncome::getProductName).filter(new RichFilterFunction<ProductIncome>() {
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("summary-state", Types.INT);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(ProductIncome value) throws Exception {
                Integer summary = valueState.value();
                if (Objects.isNull(summary)) {
                    summary = value.getIncome();
                } else {
                    summary += value.getIncome();
                }
                valueState.update(summary);
                return summary >= 100;
            }
        });
        SingleOutputStreamOperator<Tuple3<String, Integer, List<Integer>>> mapStream1 = filterStream.keyBy(ProductIncome::getProductName)
                .map(new RichMapFunction<ProductIncome, Tuple3<String, Integer, List<Integer>>>() {
                    private ListState<Integer> detailState;
                    private ValueState<Integer> summaryState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("detail", Types.INT);
                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("summary", Types.INT);

                        detailState = getRuntimeContext().getListState(listStateDescriptor);
                        summaryState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public Tuple3<String, Integer, List<Integer>> map(ProductIncome value) throws Exception {
                        Integer summary = summaryState.value();
                        if (Objects.isNull(summary)) {
                            summary = value.getIncome();
                        } else {
                            summary += value.getIncome();
                        }

                        summaryState.update(summary);
                        detailState.add(value.getIncome());

                        List<Integer> list = new ArrayList<>(10);
                        detailState.get().forEach(list::add);
                        return new Tuple3<>(value.getProductName(), summary, list);
                    }
                });
        mapStream1.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * keyed state的ListState的存储结构类似于如下
     * <p>
     * -----------------value
     * -------------------------------
     * key1       [obj1,obj2,obj3,...]
     * -------------------------------
     * key2       [obj1,obj2,obj3,...]
     * -------------------------------
     */
    @Test
    public void testListState() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        DataStreamSource<WaybillE> waybillEDataStreamSource = streamExecutionEnvironment.addSource(new WaybillESource());
        DataStreamSource<WaybillM> waybillMDataStreamSource = streamExecutionEnvironment.addSource(new WaybillMSource());

        SingleOutputStreamOperator<WaybillCEM> waybillCMap = waybillCDataStreamSource.map(new MapFunction<WaybillC, WaybillCEM>() {
            @Override
            public WaybillCEM map(WaybillC value) throws Exception {
                WaybillCEM waybillCEM = new WaybillCEM();
                waybillCEM.setWaybillCode(value.getWaybillCode());
                waybillCEM.setWaybillSign(value.getWaybillSign());
                waybillCEM.setSiteCode(value.getSiteCode());
                waybillCEM.setSiteName(value.getSiteName());
                return waybillCEM;
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<WaybillCEM> waybillEMap = waybillEDataStreamSource.map(new MapFunction<WaybillE, WaybillCEM>() {
            @Override
            public WaybillCEM map(WaybillE value) throws Exception {
                WaybillCEM waybillCEM = new WaybillCEM();
                waybillCEM.setWaybillCode(value.getWaybillCode());
                waybillCEM.setBusiNo(value.getBusiNo());
                waybillCEM.setBusiName(value.getBusiName());
                waybillCEM.setSendPay(value.getSendPay());
                return waybillCEM;
            }
        }).setParallelism(3);

        SingleOutputStreamOperator<WaybillCEM> waybillMMap = waybillMDataStreamSource.map(new MapFunction<WaybillM, WaybillCEM>() {
            @Override
            public WaybillCEM map(WaybillM value) throws Exception {
                WaybillCEM waybillCEM = new WaybillCEM();
                waybillCEM.setWaybillCode(value.getWaybillCode());
                waybillCEM.setPickupDate(value.getPickupDate());
                waybillCEM.setDeliveryDate(value.getDeliveryDate());
                return waybillCEM;
            }
        }).setParallelism(4);

        SingleOutputStreamOperator<WaybillCEM> waybillCEMStream = waybillCMap.union(waybillEMap, waybillMMap).keyBy(WaybillCEM::getWaybillCode).map(new RichMapFunction<WaybillCEM, WaybillCEM>() {
            private ListState<WaybillCEM> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<WaybillCEM> listStateDescriptor = new ListStateDescriptor<>("waybill-state", Types.GENERIC(WaybillCEM.class));
                listState = getRuntimeContext().getListState(listStateDescriptor);
            }

            @Override
            public WaybillCEM map(WaybillCEM value) throws Exception {
                listState.add(value);
                WaybillCEM waybillCEM = new WaybillCEM();
                for (WaybillCEM entry : listState.get()) {
                    Optional.ofNullable(entry.getWaybillCode()).ifPresent(waybillCEM::setWaybillCode);
                    Optional.ofNullable(entry.getWaybillSign()).ifPresent(waybillCEM::setWaybillSign);
                    Optional.ofNullable(entry.getSiteCode()).ifPresent(waybillCEM::setSiteCode);
                    Optional.ofNullable(entry.getSiteName()).ifPresent(waybillCEM::setSiteName);
                    Optional.ofNullable(entry.getBusiNo()).ifPresent(waybillCEM::setBusiNo);
                    Optional.ofNullable(entry.getBusiName()).ifPresent(waybillCEM::setBusiName);
                    Optional.ofNullable(entry.getSendPay()).ifPresent(waybillCEM::setSendPay);
                    Optional.ofNullable(entry.getPickupDate()).ifPresent(waybillCEM::setPickupDate);
                    Optional.ofNullable(entry.getDeliveryDate()).ifPresent(waybillCEM::setDeliveryDate);
                }
                return waybillCEM;
            }
        }).setParallelism(5);

        waybillCEMStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * keyed state的MapState的存储结构如下：
     * -----------------value
     * ------------------------------------
     * key1       {key1=value1,key2=value2}
     * ------------------------------------
     * key2       {key1=value1,key2=value2}
     * ------------------------------------
     * <p>
     * 如果在一个算子里使用多个state，可以理解为一个列表：
     * -----------------my value state   ｜   my list state      ｜     my map state
     * -------------------------------------------------------------------------------------------
     * key1             single object1   ｜   [obj1,obj2,obj3]   ｜     {key1=value1,key2=value2}
     * -------------------------------------------------------------------------------------------
     * key2             single object2   ｜   [obj1,obj2,obj3]   ｜     {key1=value1,key2=value2}
     * -------------------------------------------------------------------------------------------
     */
    @Test
    public void testMapState() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.enableCheckpointing(3000);

        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillRouteLinkSource());

        SingleOutputStreamOperator<WaybillCRouteLink> waybillCRouteLinkStream = waybillCDataStreamSource.connect(waybillRouteLinkDataStreamSource).keyBy(WaybillC::getWaybillCode, WaybillRouteLink::getWaybillCode)
                .flatMap(new RichCoFlatMapFunction<WaybillC, WaybillRouteLink, WaybillCRouteLink>() {

                    private ValueState<WaybillC> waybillCValueState;
                    private MapState<String, WaybillRouteLink> routeLinkMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<WaybillC> valueStateDescriptor = new ValueStateDescriptor<>("value-state", Types.GENERIC(WaybillC.class));
                        MapStateDescriptor<String, WaybillRouteLink> routeLinkMapStateDescriptor = new MapStateDescriptor<>("map-state", Types.STRING, Types.GENERIC(WaybillRouteLink.class));

                        waybillCValueState = getRuntimeContext().getState(valueStateDescriptor);
                        routeLinkMapState = getRuntimeContext().getMapState(routeLinkMapStateDescriptor);
                    }

                    @Override
                    public void flatMap1(WaybillC value, Collector<WaybillCRouteLink> out) throws Exception {
                        waybillCValueState.update(value);

                        WaybillCRouteLink waybillCRouteLink = new WaybillCRouteLink();
                        waybillCRouteLink.setWaybillCode(value.getWaybillCode());
                        waybillCRouteLink.setWaybillSign(value.getWaybillSign());
                        waybillCRouteLink.setSiteCode(value.getSiteCode());
                        waybillCRouteLink.setSiteName(value.getSiteName());

                        if (!routeLinkMapState.isEmpty()) {
                            for (Map.Entry<String, WaybillRouteLink> entry : routeLinkMapState.entries()) {
                                waybillCRouteLink.setPackageCode(entry.getKey());
                                waybillCRouteLink.setStaticDeliveryTime(entry.getValue().getStaticDeliveryTime());
                                out.collect(waybillCRouteLink);
                            }
                        } else {
                            out.collect(waybillCRouteLink);
                        }
                    }

                    @Override
                    public void flatMap2(WaybillRouteLink value, Collector<WaybillCRouteLink> out) throws Exception {
                        routeLinkMapState.put(value.getPackageCode(), value);
                        WaybillCRouteLink waybillCRouteLink = new WaybillCRouteLink();
                        waybillCRouteLink.setWaybillCode(value.getWaybillCode());
                        waybillCRouteLink.setPackageCode(value.getPackageCode());
                        waybillCRouteLink.setStaticDeliveryTime(value.getStaticDeliveryTime());

                        WaybillC waybillC = waybillCValueState.value();
                        if (Objects.nonNull(waybillC)) {
                            waybillCRouteLink.setWaybillSign(waybillC.getWaybillSign());
                            waybillCRouteLink.setSiteCode(waybillC.getSiteCode());
                            waybillCRouteLink.setSiteName(waybillC.getSiteName());
                        }

                        out.collect(waybillCRouteLink);
                    }
                }).setParallelism(5);

        waybillCRouteLinkStream.print().setParallelism(3);
        streamExecutionEnvironment.execute();
    }

    /**
     * ReduceState类似于ValueState，也是每一个key存储一个值。但是与ValueState不同的是，ReduceState提供聚合功能，它可以将要加入state中的对象和当前state中已存储的对象进行聚合。
     * 然后将聚合后的结果存储到state中，省去了我们自己获取Value state的值，然后手动计算，最后更新到state里的步骤。
     * 在调用ReduceState的add方法往state里添加数据时，就会进行将state中已有数据和当前要添加的数据进行聚合，然后将聚合结果存储到state中。而调用get方法时，就直接获取聚合的结果了。
     * <p>
     * 在这里可以看到ConnectedStream中使用state的好处，如果两个流中有key相同的数据，那么这两个来自不同流的key相同的数据可以共享同一份state，这样可以达到两个流中相同key的处理进行通信的目的。
     */
    @Test
    public void testReduceState() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);

        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        DataStreamSource<WaybillE> waybillEDataStreamSource = streamExecutionEnvironment.addSource(new WaybillESource());
        DataStreamSource<WaybillM> waybillMDataStreamSource = streamExecutionEnvironment.addSource(new WaybillMSource());
        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillRouteLinkSource());

        SingleOutputStreamOperator<WaybillCEM> waybillCMapStream = waybillCDataStreamSource.map(new MapFunction<WaybillC, WaybillCEM>() {
            @Override
            public WaybillCEM map(WaybillC value) throws Exception {
                WaybillCEM waybillCEM = new WaybillCEM();
                waybillCEM.setWaybillCode(value.getWaybillCode());
                waybillCEM.setWaybillSign(value.getWaybillSign());
                waybillCEM.setSiteCode(value.getSiteCode());
                waybillCEM.setSiteName(value.getSiteName());
                return waybillCEM;
            }
        });

        SingleOutputStreamOperator<WaybillCEM> waybillEMapStream = waybillEDataStreamSource.map(new MapFunction<WaybillE, WaybillCEM>() {
            @Override
            public WaybillCEM map(WaybillE value) throws Exception {
                WaybillCEM waybillCEM = new WaybillCEM();
                waybillCEM.setWaybillCode(value.getWaybillCode());
                waybillCEM.setBusiNo(value.getBusiNo());
                waybillCEM.setBusiName(value.getBusiName());
                waybillCEM.setSendPay(value.getSendPay());
                return waybillCEM;
            }
        });

        SingleOutputStreamOperator<WaybillCEM> waybillMMapStream = waybillMDataStreamSource.map(new MapFunction<WaybillM, WaybillCEM>() {
            @Override
            public WaybillCEM map(WaybillM value) throws Exception {
                WaybillCEM waybillCEM = new WaybillCEM();
                waybillCEM.setWaybillCode(value.getWaybillCode());
                waybillCEM.setPickupDate(value.getPickupDate());
                waybillCEM.setDeliveryDate(value.getDeliveryDate());
                return waybillCEM;
            }
        });

        SingleOutputStreamOperator<WaybillCEMRouteLink> waybillCEMRouteLinkStream = waybillCMapStream.union(waybillEMapStream, waybillMMapStream)
                .connect(waybillRouteLinkDataStreamSource)
                .keyBy(WaybillCEM::getWaybillCode, WaybillRouteLink::getWaybillCode)
                .flatMap(new RichCoFlatMapFunction<WaybillCEM, WaybillRouteLink, WaybillCEMRouteLink>() {
                    private ReducingState<WaybillCEM> reducingState;
                    private ListState<String> packageCodeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<WaybillCEM> reducingStateDescriptor = new ReducingStateDescriptor<>("cem-state", (value1, value2) -> {
                            Optional.ofNullable(value1.getWaybillCode()).ifPresent(value2::setWaybillCode);
                            Optional.ofNullable(value1.getWaybillSign()).ifPresent(value2::setWaybillSign);
                            Optional.ofNullable(value1.getSiteCode()).ifPresent(value2::setSiteCode);
                            Optional.ofNullable(value1.getSiteName()).ifPresent(value2::setSiteName);
                            Optional.ofNullable(value1.getBusiNo()).ifPresent(value2::setBusiNo);
                            Optional.ofNullable(value1.getBusiName()).ifPresent(value2::setBusiName);
                            Optional.ofNullable(value1.getSendPay()).ifPresent(value2::setSendPay);
                            Optional.ofNullable(value1.getPickupDate()).ifPresent(value2::setPickupDate);
                            Optional.ofNullable(value1.getDeliveryDate()).ifPresent(value2::setDeliveryDate);

                            return value2;
                        }, WaybillCEM.class);

                        reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);

                        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<String>("package-code-state", String.class);
                        packageCodeState = getRuntimeContext().getListState(listStateDescriptor);
                    }

                    @Override
                    public void flatMap1(WaybillCEM value, Collector<WaybillCEMRouteLink> out) throws Exception {
                        reducingState.add(value);
                        WaybillCEM waybillCEM = reducingState.get();
                        WaybillCEMRouteLink waybillCEMRouteLink = new WaybillCEMRouteLink();
                        waybillCEMRouteLink.setWaybillCode(waybillCEM.getWaybillCode());
                        waybillCEMRouteLink.setWaybillSign(waybillCEM.getWaybillSign());
                        waybillCEMRouteLink.setSiteCode(waybillCEM.getSiteCode());
                        waybillCEMRouteLink.setSiteName(waybillCEM.getSiteName());
                        waybillCEMRouteLink.setBusiNo(waybillCEM.getBusiNo());
                        waybillCEMRouteLink.setBusiName(waybillCEM.getBusiName());
                        waybillCEMRouteLink.setSendPay(waybillCEM.getSendPay());
                        waybillCEMRouteLink.setPickupDate(waybillCEM.getPickupDate());
                        waybillCEMRouteLink.setDeliveryDate(waybillCEM.getDeliveryDate());

                        boolean hasPackageCode = false;
                        for (String packageCode : packageCodeState.get()) {
                            hasPackageCode = true;
                            waybillCEMRouteLink.setPackageCode(packageCode);
                            out.collect(waybillCEMRouteLink);
                        }
                        if (!hasPackageCode) {
                            out.collect(waybillCEMRouteLink);
                        }
                    }

                    @Override
                    public void flatMap2(WaybillRouteLink value, Collector<WaybillCEMRouteLink> out) throws Exception {
                        packageCodeState.add(value.getPackageCode());

                        WaybillCEM waybillCEM = reducingState.get();
                        if (Objects.nonNull(waybillCEM)) {
                            WaybillCEMRouteLink waybillCEMRouteLink = new WaybillCEMRouteLink();
                            waybillCEMRouteLink.setWaybillCode(waybillCEM.getWaybillCode());
                            waybillCEMRouteLink.setWaybillSign(waybillCEM.getWaybillSign());
                            waybillCEMRouteLink.setSiteCode(waybillCEM.getSiteCode());
                            waybillCEMRouteLink.setSiteName(waybillCEM.getSiteName());
                            waybillCEMRouteLink.setBusiNo(waybillCEM.getBusiNo());
                            waybillCEMRouteLink.setBusiName(waybillCEM.getBusiName());
                            waybillCEMRouteLink.setSendPay(waybillCEM.getSendPay());
                            waybillCEMRouteLink.setPickupDate(waybillCEM.getPickupDate());
                            waybillCEMRouteLink.setDeliveryDate(waybillCEM.getDeliveryDate());
                            waybillCEMRouteLink.setPackageCode(value.getPackageCode());
                            out.collect(waybillCEMRouteLink);
                        } else {
                            WaybillCEMRouteLink waybillCEMRouteLink = new WaybillCEMRouteLink();
                            waybillCEMRouteLink.setWaybillCode(value.getWaybillCode());
                            waybillCEMRouteLink.setPackageCode(value.getPackageCode());
                            out.collect(waybillCEMRouteLink);
                        }

                    }
                });

        waybillCEMRouteLinkStream.print();
        streamExecutionEnvironment.execute();
    }

    /**
     * process function的功能：
     * 1.输入的数据类型可以和输出的数据类型不同
     * 2.可以使用state（不像其他function需要继承rich function来获取RuntimeContext）
     * 3.可以使用定时器timer
     * 4.可以将元素输出到旁路流
     * <p>
     * aggregating state类似于reduce state，也是每个key存储一个值，而且在往state中添加元素后，还会将当前state的值和新加入的state的值进行聚合。
     * 但是与reduce state不一样的是，aggregating state在add元素时，不会进行聚合，而是放到一个累加器里。当使用aggregating state的get方法时，
     * 才会从累加器里获取所有已添加的数据，然后进行聚合。
     */
    @Test
    public void testAggregateState() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<ProductIncome> productSource = streamExecutionEnvironment.addSource(new ProductSource());
        OutputTag<ProductIncome> productIncomeOutputTag = new OutputTag<>("less summary product", Types.GENERIC(ProductIncome.class));
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> moreSummaryStream = productSource.keyBy(ProductIncome::getProductName)
                .process(new KeyedProcessFunction<String, ProductIncome, Tuple3<String, Integer, Integer>>() {

                    private AggregatingState<ProductIncome, Integer> aggregatingState;
                    private ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<ProductIncome, List<ProductIncome>, Integer> aggregatingStateDescriptor
                                = new AggregatingStateDescriptor<>("summary-state", new AggregateFunction<ProductIncome, List<ProductIncome>, Integer>() {
                            @Override
                            public List<ProductIncome> createAccumulator() {
                                return new ArrayList<>();
                            }

                            @Override
                            public List<ProductIncome> add(ProductIncome value, List<ProductIncome> accumulator) {
                                accumulator.add(value);
                                return accumulator;
                            }

                            @Override
                            public Integer getResult(List<ProductIncome> accumulator) {
                                return accumulator.stream().mapToInt(ProductIncome::getIncome).sum();
                            }

                            @Override
                            public List<ProductIncome> merge(List<ProductIncome> a, List<ProductIncome> b) {
                                a.addAll(b);
                                return a;
                            }
                        }, Types.LIST(Types.GENERIC(ProductIncome.class)));

                        aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("detail-state", Types.INT));
                    }

                    @Override
                    public void processElement(ProductIncome value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        listState.add(value.getIncome());
                        aggregatingState.add(value);

                        Integer summray = aggregatingState.get();
                        List<Integer> detail = new ArrayList<>();
                        listState.get().forEach(detail::add);
                        Integer[] detailArray = detail.toArray(new Integer[0]);

                        value.setSummary(summray);
                        value.setDetail(detailArray);
                        if (summray <= 10000) {
                            ctx.output(productIncomeOutputTag, value);
                        } else {
                            Tuple3<String, Integer, Integer> tuple3 = Tuple3.of(value.getProductName(), value.getIncome(), summray);
                            out.collect(tuple3);
                        }
                    }
                });

        DataStream<ProductIncome> lessSummaryStream = moreSummaryStream.getSideOutput(productIncomeOutputTag);
        lessSummaryStream.print("less summary product:");
        moreSummaryStream.print("more summary product:");

        streamExecutionEnvironment.execute();
    }

    @Test
    public void testState() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<ProductIncome> productStreamSource = streamExecutionEnvironment.addSource(new ProductSource());
        DataStreamSource<ProductSale> productSaleStreamSource = streamExecutionEnvironment.addSource(new ProductSaleSource());

        SingleOutputStreamOperator<Tuple3<String, Integer, Double>> produceWithSaleStream = productStreamSource.connect(productSaleStreamSource).keyBy(ProductIncome::getProductName, ProductSale::getProductName)
                .flatMap(new RichCoFlatMapFunction<ProductIncome, ProductSale, Tuple3<String, Integer, Double>>() {
                    private ValueState<Double> saleState;
                    private ListState<Integer> incomeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        saleState = getRuntimeContext().getState(new ValueStateDescriptor<>("sale-state", Types.DOUBLE));
                        incomeState = getRuntimeContext().getListState(new ListStateDescriptor<>("income-state", Types.INT));
                    }

                    @Override
                    public void flatMap1(ProductIncome value, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
                        Double sale = saleState.value();
                        if (Objects.nonNull(sale)) {
                            out.collect(Tuple3.of(value.getProductName(), value.getIncome(), sale));
                            boolean hasOtherIncome = false;
                            for (Integer income : incomeState.get()) {
                                out.collect(Tuple3.of(value.getProductName(), income, sale));
                                hasOtherIncome = true;
                            }
                            if (hasOtherIncome) {
                                incomeState.clear();
                            }
                        } else {
                            incomeState.add(value.getIncome());
                        }
                    }

                    @Override
                    public void flatMap2(ProductSale value, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
                        saleState.update(value.getSale());

                        boolean hasOtherIncome = false;
                        for (Integer income : incomeState.get()) {
                            out.collect(Tuple3.of(value.getProductName(), income, value.getSale()));
                            hasOtherIncome = true;
                        }
                        if (hasOtherIncome) {
                            incomeState.clear();
                        }
                    }
                });

        produceWithSaleStream.print("meta data:");
        SingleOutputStreamOperator<ProductIncome> realProductIncomeStream = produceWithSaleStream.map(new MapFunction<Tuple3<String, Integer, Double>, ProductIncome>() {
            @Override
            public ProductIncome map(Tuple3<String, Integer, Double> value) throws Exception {
                return new ProductIncome(value.f0, new BigDecimal(value.f1).multiply(BigDecimal.valueOf(value.f2)).intValue());
            }
        });


        streamExecutionEnvironment.execute();
    }
}
