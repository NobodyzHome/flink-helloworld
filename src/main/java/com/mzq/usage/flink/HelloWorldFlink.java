package com.mzq.usage.flink;

import com.mzq.usage.flink.domain.WaybillC;
import com.mzq.usage.flink.domain.WaybillCRouteLink;
import com.mzq.usage.flink.domain.WaybillRouteLink;
import com.mzq.usage.flink.func.source.WaybillCSource;
import com.mzq.usage.flink.func.source.WaybillRouteLinkSource;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Objects;

public class HelloWorldFlink {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
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
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());
                        routeLinkMapStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

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
}
