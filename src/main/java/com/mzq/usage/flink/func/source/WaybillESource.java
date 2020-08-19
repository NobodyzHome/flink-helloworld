package com.mzq.usage.flink.func.source;

import com.mzq.usage.flink.domain.WaybillE;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;

public class WaybillESource extends AbstractSourceFunction<WaybillE> {

    private IntCounter intCounter;

    @Override
    protected void init() {
        intCounter = getRuntimeContext().getIntCounter("waybillE-counter");
    }

    @Override
    protected WaybillE createElement(SourceContext<WaybillE> ctx) {
        intCounter.add(1);
        Integer value = intCounter.getLocalValue();

        WaybillE waybillE = new WaybillE();
        waybillE.setWaybillCode("JD" + StringUtils.leftPad(value.toString(), 10, "0"));
        waybillE.setBusiNo(String.valueOf(RandomUtils.nextInt(1, 100)));
        waybillE.setBusiName(String.format("商家%s", waybillE.getBusiNo()));
        waybillE.setSendPay(RandomStringUtils.random(50, "01"));

        if (value % 3 == 0) {
//            ctx.collect(waybillE);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
        return waybillE;
    }
}
