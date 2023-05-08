package com.wildgoose.orderpaydetect;

import com.wildgoose.orderpaydetect.beans.OrderEvent;
import com.wildgoose.orderpaydetect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * @Description : 支付实时对账
 * @Author : JustxzzZ
 * @Date : 2023.05.05 15:54
 */
public class TxPayMatch {

    // 定义侧输出流
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays") {};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts") {};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> orderEventSource =
                env.readTextFile(Objects.requireNonNull(TxPayMatch.class.getResource("/OrderLog.csv")).getPath());
        DataStreamSource<String> receiptEventSource =
                env.readTextFile(Objects.requireNonNull(TxPayMatch.class.getResource("/ReceiptLog.csv")).getPath());

        // 2.转换成 POJO，并指定 eventTime 和 watermark
        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderEventSource.map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getTimestamp() * 1000L;
            }
        }).filter(data -> !"".equals(data.getTxId()));

        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = receiptEventSource.map(line -> {
            String[] fields = line.split(",");
            return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // TODO 3.支付实时对账，双流匹配
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        // 4.输出结果
        resultStream.print("matched");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute("transaction pay match job");
    }


    /**
     * 自定义 CoProcessFunction 函数，双流对账匹配
     */
    private static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        // 定义状态，保存当前已经到来的订单支付事件和到账事件，用于相互等待
        private ValueState<OrderEvent> payState;
        private ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<>("pay", OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<>("receipt", ReceiptEvent.class));
        }

        /**
         * 处理订单支付事件流
         */
        @Override
        public void processElement1(
                OrderEvent pay,
                CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context ctx,
                Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

            // 订单支付事件到了，判断是否有对应的到账事件
            ReceiptEvent receipt = receiptState.value();

            if (receipt != null) {
                // 如果 receipt 不为 null，说明到账事件已经到来，输出匹配事件，清空状态
                out.collect(new Tuple2<>(pay, receipt));
                payState.clear();
                receiptState.clear();
            } else {
                // 如果到账事件没来，注册一个定时器，开始等待
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp() + 5) * 1000L);  // 让 pay 等待5s，等多久具体看数据
                // 更新状态
                payState.update(pay);
            }

        }

        /**
         * 处理到账事件流
         */
        @Override
        public void processElement2(
                ReceiptEvent receipt,
                CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context ctx,
                Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

            // 到账事件到了，判断是否有对应的支付事件
            OrderEvent pay = payState.value();

            if (pay != null) {
                // 如果 pay 不为 null，说明支付事件已经到来，输出匹配事件，清空状态
                out.collect(new Tuple2<>(pay, receipt));
                payState.clear();
                receiptState.clear();
            } else {
                // 如果支付事件没来，注册一个定时器，开始等待
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp() + 3) * 1000L);  // 让 receipt 等待3s，等多久具体看数据
                // 更新状态
                receiptState.update(receipt);
            }

        }

        @Override
        public void onTimer(
                long timestamp,
                CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.OnTimerContext ctx,
                Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

            // 定时器触发，有可能是有一个事件没来，不匹配；也有可能是都来过了，已经输出并清空了状态（这种情形不用在此处输出）
            OrderEvent pay = payState.value();
            ReceiptEvent receipt = receiptState.value();

            // 如果 pay 不为 null，说明 pay 到了但 receipt 没到
            if (pay != null) {
                ctx.output(unmatchedPays, pay);
            }
            // 如果 receipt 不为 null，说明 receipt 到了但 pay 没到
            if (receipt != null) {
                ctx.output(unmatchedReceipts, receipt);
            }

            // 清空状态
            payState.clear();
            receiptState.clear();

        }
    }
}
