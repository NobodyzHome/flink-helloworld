package com.mzq.usage.flink.func.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class TestSink implements SinkFunction<Integer>, CheckpointedFunction {

    private ListState<Integer> operatorState;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        operatorState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("op-state", Types.INT));
    }

    @Override
    public void invoke(Integer value, Context context) throws Exception {
        operatorState.add(value);
        Integer sum = 0;
        for (Integer val : operatorState.get()) {
            sum += val;
        }
        System.out.println(String.format("cur is:%d,sum is:%d", value, sum));
    }
}
