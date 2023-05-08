package com.wildgoose.orderpaydetect;

import com.wildgoose.orderpaydetect.beans.OrderEvent;
import com.wildgoose.orderpaydetect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * @Description : 订单超时检测
 * @Author : JustxzzZ
 * @Date : 2023.05.05 10:43
 */
public class OrderTimeoutWithoutCEP {

    // 定义一个订单超时侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("orderTimeout") {};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件读取数据
        String filePath = Objects.requireNonNull(OrderTimeoutWithoutCEP.class.getResource("/OrderLog.csv")).getPath();
        DataStreamSource<String> inputStream = env.readTextFile(filePath);

        // 2.转换成 POJO，并指定 eventTime 和 watermark
        SingleOutputStreamOperator<OrderEvent> orderEventStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // TODO 3.订单超时检测 - 15分钟内未完成支付，不用 CEP 实现
        // 主流输出正常事件，侧输出流输出超时事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        // 4.输出结果
        resultStream.print("payed");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");

    }


    /**
     * 自定义处理函数，检测订单支付情况，主流输出正常事件，侧输出流输出超时事件
     */
    private static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        private ValueState<Boolean> isCreatedState; // 定义状态，保存是否创建状态
        private ValueState<Boolean> isPayedState;   // 定义状态，保存是否支付状态
        private ValueState<Long> timerTsState;      // 定义状态，保存注册的定时器的时间戳

        @Override
        public void open(Configuration parameters) throws Exception {
            isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<>("is-created", Boolean.class, false));
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<>("is-payed", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value,
                                   KeyedProcessFunction<Long, OrderEvent, OrderResult>.Context ctx,
                                   Collector<OrderResult> out) throws Exception {

            // 获取状态变量的值
            Boolean isCreated = isCreatedState.value();
            Boolean isPayed = isPayedState.value();
            Long timerTs = timerTsState.value();

            // 判断当前事件类型
            if ("create".equals(value.getEventType())) {
                // 1.如果来的是 create 事件，要判断是否支付过（因为数据可能是乱序的）
                if (isPayed) {
                    // 1.1 如果已经正常支付，输出正常匹配结果
                    out.collect(new OrderResult(ctx.getCurrentKey(), "payed successfully"));

                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);

                } else {
                    // 1.2 如果没有支付过，注册一个 15min 后的定时器，开始等待支付事件到来
                    Long ts = (value.getTimestamp() + 15 * 60) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                // 2.如果来的是 pay 事件，要判断是否有下单事件来过
                if (isCreated) {
                    // 2.1 已经有过下单事件，需要判断事件时间戳是否在定时器触发之前
                    if (value.getTimestamp() * 1000L < timerTs) {
                        // 2.1.1 如果在定时器触发之前
                        out.collect(new OrderResult(ctx.getCurrentKey(), "payed successfully"));

                    } else {
                        // 2.1.2 如果在定时器触发之后
                        ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but already timeout"));

                    }

                    // 情况状态
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 2.2 没有下单事件，注册一个定时器，等待下单事件到来（乱序情况）
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L);

                    // 更新状态
                    isPayedState.update(true);
                    timerTsState.update(value.getTimestamp() * 1000L);
                }
            }
        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Long, OrderEvent, OrderResult>.OnTimerContext ctx,
                            Collector<OrderResult> out) throws Exception {

            // 定时器触发，说明一定有一个事件没来
            if (isPayedState.value()) {
                // 如果 pay 来了，则说明 create 没来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "already payed but not found created log"));
            } else {
                // 如果 pay 没来，支付超时
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "order pay timeout"));
            }

            // 清空状态
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();

        }
    }
}
