package com.mzq.usage.flink.func.source;

import com.mzq.usage.flink.domain.Order;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;

import java.util.Date;

public class OrderSource extends AbstractSourceFunction<Order> {

    private IntCounter counter;

    @Override
    protected Order createElement(SourceContext<Order> ctx) {
        counter.add(1);
        Integer value = counter.getLocalValue();

        Order order = new Order();
        order.setOrderCode("Order" + StringUtils.leftPad(value.toString(), 10, "0"));
        order.setCreateTime(new Date());

        return order;
    }

    @Override
    protected void init() {
        counter = getRuntimeContext().getIntCounter("counter");
    }
}
