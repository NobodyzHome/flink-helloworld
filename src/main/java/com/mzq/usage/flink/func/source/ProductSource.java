package com.mzq.usage.flink.func.source;

import com.mzq.usage.flink.domain.ProductIncome;
import com.mzq.usage.flink.domain.WaybillC;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;

import java.util.concurrent.atomic.AtomicInteger;

public class ProductSource extends AbstractSourceFunction<ProductIncome> {

    @Override
    protected void init() {

    }

    @Override
    protected ProductIncome createElement(SourceContext<ProductIncome> ctx) {
        ProductIncome productIncome = new ProductIncome();
        productIncome.setProductName("类别" + RandomStringUtils.random(1, "ABCDEFG"));
        productIncome.setIncome(RandomUtils.nextInt(100, 3000));
        return productIncome;
    }
}
