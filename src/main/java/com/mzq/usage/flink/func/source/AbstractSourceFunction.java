package com.mzq.usage.flink.func.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.time.Duration;

public abstract class AbstractSourceFunction<T> extends RichSourceFunction<T> {
    private boolean isRunning;

    @Override
    public void open(Configuration parameters) throws Exception {
        init();
        isRunning = true;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(createElement(ctx));
            try {
                Thread.sleep(interval().toMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    protected abstract T createElement(SourceContext<T> ctx);

    protected abstract void init();

    protected Duration interval() {
        return Duration.ofSeconds(1);
    }


}
