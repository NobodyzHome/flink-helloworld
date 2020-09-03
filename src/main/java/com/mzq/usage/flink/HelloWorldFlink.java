package com.mzq.usage.flink;

import com.mzq.usage.flink.domain.*;
import com.mzq.usage.flink.func.source.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.*;

public class HelloWorldFlink {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        DataStreamSource<WaybillE> waybillEDataStreamSource = streamExecutionEnvironment.addSource(new WaybillESource());
        DataStreamSource<WaybillM> waybillMDataStreamSource = streamExecutionEnvironment.addSource(new WaybillMSource());
        DataStreamSource<WaybillOrder> waybillOrderDataStreamSource = streamExecutionEnvironment.addSource(new WaybillOrderSource());
        DataStreamSource<Order> orderDataStreamSource = streamExecutionEnvironment.addSource(new OrderSource());
        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillRouteLinkSource());

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

        SingleOutputStreamOperator<WaybillCEM> waybillCEMStream = waybillCMap.union(waybillEMap, waybillMMap).keyBy(WaybillCEM::getWaybillCode).reduce(new ReduceFunction<WaybillCEM>() {
            @Override
            public WaybillCEM reduce(WaybillCEM value1, WaybillCEM value2) throws Exception {
                Optional.ofNullable(value1.getWaybillCode()).ifPresent(value2::setWaybillCode);
                Optional.ofNullable(value1.getWaybillSign()).ifPresent(value2::setWaybillSign);
                Optional.ofNullable(value1.getSiteCode()).ifPresent(value2::setSiteCode);
                Optional.ofNullable(value1.getSiteName()).ifPresent(value2::setSiteName);
                Optional.ofNullable(value1.getBusiName()).ifPresent(value2::setBusiName);
                Optional.ofNullable(value1.getBusiNo()).ifPresent(value2::setBusiNo);
                Optional.ofNullable(value1.getSendPay()).ifPresent(value2::setSendPay);
                Optional.ofNullable(value1.getDeliveryDate()).ifPresent(value2::setDeliveryDate);
                Optional.ofNullable(value1.getPickupDate()).ifPresent(value2::setPickupDate);

                return value2;
            }
        }).setParallelism(3);

        SingleOutputStreamOperator<WaybillCEMRouteLink> waybillCEMRouteLinkStream = waybillCEMStream.connect(waybillRouteLinkDataStreamSource)
                .keyBy(WaybillCEM::getWaybillCode, WaybillRouteLink::getWaybillCode)
                .flatMap(new RichCoFlatMapFunction<WaybillCEM, WaybillRouteLink, WaybillCEMRouteLink>() {
                    private ValueState<WaybillCEM> waybillCEMState;
                    private MapState<String, Date> packageState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        waybillCEMState = getRuntimeContext().getState(new ValueStateDescriptor<>("waybill-cem-state", Types.GENERIC(WaybillCEM.class)));
                        packageState = getRuntimeContext().getMapState(new MapStateDescriptor<>("route-link-state", Types.STRING, Types.GENERIC(Date.class)));
                    }

                    @Override
                    public void flatMap1(WaybillCEM value, Collector<WaybillCEMRouteLink> out) throws Exception {
                        waybillCEMState.update(value);

                        WaybillCEMRouteLink waybillCEMRouteLink = new WaybillCEMRouteLink();
                        waybillCEMRouteLink.setWaybillCode(value.getWaybillCode());
                        waybillCEMRouteLink.setWaybillSign(value.getWaybillSign());
                        waybillCEMRouteLink.setSiteCode(value.getSiteCode());
                        waybillCEMRouteLink.setSiteName(value.getSiteName());
                        waybillCEMRouteLink.setBusiNo(value.getBusiNo());
                        waybillCEMRouteLink.setBusiName(value.getBusiName());
                        waybillCEMRouteLink.setSendPay(value.getSendPay());
                        waybillCEMRouteLink.setPickupDate(value.getPickupDate());
                        waybillCEMRouteLink.setDeliveryDate(value.getDeliveryDate());

                        boolean hasPackage = false;
                        for (Map.Entry<String, Date> entry : packageState.entries()) {
                            waybillCEMRouteLink.setPackageCode(entry.getKey());
                            waybillCEMRouteLink.setStaticDeliveryTime(entry.getValue());
                            hasPackage = true;
                            out.collect(waybillCEMRouteLink);
                        }

                        if (hasPackage) {
                            packageState.clear();
                        } else {
                            out.collect(waybillCEMRouteLink);
                        }
                    }

                    @Override
                    public void flatMap2(WaybillRouteLink value, Collector<WaybillCEMRouteLink> out) throws Exception {
                        WaybillCEM waybillCEM = waybillCEMState.value();
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
                            waybillCEMRouteLink.setStaticDeliveryTime(value.getStaticDeliveryTime());

                            out.collect(waybillCEMRouteLink);
                        } else {
                            packageState.put(value.getPackageCode(), value.getStaticDeliveryTime());
                        }
                    }
                }).setParallelism(5);

        SingleOutputStreamOperator<Tuple3<String, String, Date>> orderStream = waybillOrderDataStreamSource.connect(orderDataStreamSource)
                .keyBy(WaybillOrder::getOrderId, Order::getOrderCode)
                .flatMap(new RichCoFlatMapFunction<WaybillOrder, Order, Tuple3<String, String, Date>>() {
                    private ListState<String> waybillListState;
                    private ValueState<Date> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        waybillListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("waybill-list", Types.STRING));
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Date>("order-state", Types.GENERIC(Date.class)));
                    }

                    @Override
                    public void flatMap1(WaybillOrder value, Collector<Tuple3<String, String, Date>> out) throws Exception {
                        Date staticDeliveryDate = valueState.value();
                        out.collect(Tuple3.of(value.getWaybillCode(), value.getOrderId(), staticDeliveryDate));

                        if (Objects.isNull(staticDeliveryDate)) {
                            waybillListState.add(value.getWaybillCode());
                        }
                    }

                    @Override
                    public void flatMap2(Order value, Collector<Tuple3<String, String, Date>> out) throws Exception {
                        valueState.update(value.getCreateTime());
                        boolean hasWaybill = false;
                        for (String waybillCode : waybillListState.get()) {
                            out.collect(Tuple3.of(waybillCode, value.getOrderCode(), value.getCreateTime()));
                            hasWaybill = true;
                        }
                        if (hasWaybill) {
                            waybillListState.clear();
                        }
                    }
                }).setParallelism(3);

        SingleOutputStreamOperator<BdWaybillOrder> bdWaybillOrderStream = waybillCEMRouteLinkStream.connect(orderStream)
                .keyBy(WaybillCEMRouteLink::getWaybillCode, new KeySelector<Tuple3<String, String, Date>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Date> value) throws Exception {
                        return value.f0;
                    }
                }).flatMap(new RichCoFlatMapFunction<WaybillCEMRouteLink, Tuple3<String, String, Date>, BdWaybillOrder>() {
                    private ListState<WaybillCEMRouteLink> routeLinkState;
                    private ValueState<Order> orderState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        routeLinkState = getRuntimeContext().getListState(new ListStateDescriptor<WaybillCEMRouteLink>("waybill-state", Types.GENERIC(WaybillCEMRouteLink.class)));
                        orderState = getRuntimeContext().getState(new ValueStateDescriptor<Order>("order-state", Types.GENERIC(Order.class)));
                    }

                    @Override
                    public void flatMap1(WaybillCEMRouteLink value, Collector<BdWaybillOrder> out) throws Exception {
                        Order order = orderState.value();
                        if (Objects.isNull(order)) {
                            routeLinkState.add(value);
                        } else {
                            BdWaybillOrder bdWaybillOrder = new BdWaybillOrder();
                            bdWaybillOrder.setWaybillCode(value.getWaybillCode());
                            bdWaybillOrder.setWaybillSign(value.getWaybillSign());
                            bdWaybillOrder.setSiteCode(value.getSiteCode());
                            bdWaybillOrder.setSiteName(value.getSiteName());
                            bdWaybillOrder.setBusiNo(value.getBusiNo());
                            bdWaybillOrder.setBusiName(value.getBusiName());
                            bdWaybillOrder.setSendPay(value.getSendPay());
                            bdWaybillOrder.setPickupDate(value.getPickupDate());
                            bdWaybillOrder.setDeliveryDate(value.getDeliveryDate());
                            bdWaybillOrder.setPackageCode(value.getPackageCode());
                            bdWaybillOrder.setOrderCode(order.getOrderCode());
                            bdWaybillOrder.setOrderCreateDate(order.getCreateTime());

                            out.collect(bdWaybillOrder);
                        }
                    }

                    @Override
                    public void flatMap2(Tuple3<String, String, Date> value, Collector<BdWaybillOrder> out) throws Exception {
                        Order order = new Order();
                        order.setOrderCode(value.f1);
                        order.setCreateTime(value.f2);
                        orderState.update(order);

                        boolean hasWaybill = false;
                        for (WaybillCEMRouteLink waybillCEMRouteLink : routeLinkState.get()) {
                            BdWaybillOrder bdWaybillOrder = new BdWaybillOrder();
                            bdWaybillOrder.setWaybillCode(waybillCEMRouteLink.getWaybillCode());
                            bdWaybillOrder.setWaybillSign(waybillCEMRouteLink.getWaybillSign());
                            bdWaybillOrder.setSiteCode(waybillCEMRouteLink.getSiteCode());
                            bdWaybillOrder.setSiteName(waybillCEMRouteLink.getSiteName());
                            bdWaybillOrder.setBusiNo(waybillCEMRouteLink.getBusiNo());
                            bdWaybillOrder.setBusiName(waybillCEMRouteLink.getBusiName());
                            bdWaybillOrder.setSendPay(waybillCEMRouteLink.getSendPay());
                            bdWaybillOrder.setPickupDate(waybillCEMRouteLink.getPickupDate());
                            bdWaybillOrder.setDeliveryDate(waybillCEMRouteLink.getDeliveryDate());
                            bdWaybillOrder.setPackageCode(waybillCEMRouteLink.getPackageCode());
                            bdWaybillOrder.setOrderCode(order.getOrderCode());
                            bdWaybillOrder.setOrderCreateDate(order.getCreateTime());

                            out.collect(bdWaybillOrder);
                            hasWaybill = true;
                        }
                        if (hasWaybill) {
                            routeLinkState.clear();
                        }
                    }
                }).setParallelism(6);

        bdWaybillOrderStream.print();
        streamExecutionEnvironment.execute();
    }
}
