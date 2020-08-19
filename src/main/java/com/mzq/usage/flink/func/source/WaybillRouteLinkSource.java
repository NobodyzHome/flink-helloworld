package com.mzq.usage.flink.func.source;

import com.mzq.usage.flink.domain.WaybillRouteLink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class WaybillRouteLinkSource extends RichSourceFunction<WaybillRouteLink> {

    private IntCounter intCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        intCounter = getRuntimeContext().getIntCounter("counter");
    }

    @Override
    public void run(SourceContext<WaybillRouteLink> ctx) throws Exception {
        while (true) {
            intCounter.add(1);
            int count = intCounter.getLocalValuePrimitive();
            for (int i = 1; i <= 5; i++) {
                WaybillRouteLink waybillRouteLink = new WaybillRouteLink();
                waybillRouteLink.setWaybillCode("JD" + StringUtils.leftPad(String.valueOf(count), 10, "0"));
                waybillRouteLink.setPackageCode(String.format("%s-%d", waybillRouteLink.getWaybillCode(), i));
                waybillRouteLink.setStaticDeliveryTime(new Date());
                ctx.collect(waybillRouteLink);
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
    }

    @Override
    public void cancel() {

    }
}
