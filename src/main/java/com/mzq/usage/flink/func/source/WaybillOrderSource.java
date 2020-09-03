package com.mzq.usage.flink.func.source;

import com.mzq.usage.flink.domain.WaybillOrder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;

public class WaybillOrderSource extends AbstractSourceFunction<WaybillOrder> {

    private IntCounter counter;

    @Override
    protected WaybillOrder createElement(SourceContext<WaybillOrder> ctx) {
        counter.add(1);
        Integer counter = this.counter.getLocalValue();
        WaybillOrder waybillOrder = new WaybillOrder();
        waybillOrder.setWaybillCode("JD" + StringUtils.leftPad(counter.toString(), 10, "0"));
        waybillOrder.setOrderId("Order" + StringUtils.leftPad(counter.toString(), 10, "0"));

//        if (counter % 3 == 0) {
//            for (int i = 1; i <= 2; i++) {
//                WaybillOrder waybillOrder1 = new WaybillOrder();
//                waybillOrder1.setWaybillCode("JD" + StringUtils.leftPad(String.valueOf(counter + i), 10, "0"));
//                waybillOrder1.setOrderId(waybillOrder.getOrderId());
//                ctx.collect(waybillOrder1);
//            }
//        }
        return waybillOrder;
    }

    @Override
    protected void init() {
        counter = getRuntimeContext().getIntCounter("counter");
    }
}
