package com.mzq.usage.flink.func.flat;

import com.mzq.usage.flink.domain.WaybillCEM;
import com.mzq.usage.flink.domain.WaybillCEMRouteLink;
import com.mzq.usage.flink.domain.WaybillRouteLink;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

public class FlatMapTest extends RichCoFlatMapFunction<WaybillCEM, WaybillRouteLink, WaybillCEMRouteLink> implements CheckpointedFunction {

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

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        WaybillCEM value = waybillCEMState.value();
        System.out.println(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
