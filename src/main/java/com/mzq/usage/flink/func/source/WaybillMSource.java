package com.mzq.usage.flink.func.source;

import com.mzq.usage.flink.domain.WaybillM;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;

import java.sql.Date;
import java.time.ZonedDateTime;

public class WaybillMSource extends AbstractSourceFunction<WaybillM> {

    private IntCounter intCounter;

    @Override
    protected void init() {
        intCounter = getRuntimeContext().getIntCounter("waybillM-counter");
    }

    @Override
    protected WaybillM createElement(SourceContext<WaybillM> ctx) {
        intCounter.add(1);
        Integer value = intCounter.getLocalValue();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();

        WaybillM waybillM = new WaybillM();
        waybillM.setWaybillCode("JD" + StringUtils.leftPad(value.toString(), 10, "0"));
        waybillM.setPickupDate(Date.from(zonedDateTime.plusDays(RandomUtils.nextLong(1, 10)).toInstant()));
        waybillM.setDeliveryDate(Date.from(zonedDateTime.plusDays(RandomUtils.nextLong(1, 5)).toInstant()));
        return waybillM;
    }
}
