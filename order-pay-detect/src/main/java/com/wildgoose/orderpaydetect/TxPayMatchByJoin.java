package com.wildgoose.orderpaydetect;

import com.wildgoose.orderpaydetect.beans.OrderEvent;
import com.wildgoose.orderpaydetect.beans.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * @Description : 支付实时对账，通过 Join 实现
 * @Author : JustxzzZ
 * @Date : 2023.05.05 15:54
 */
public class TxPayMatchByJoin {

    // 定义侧输出流
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays") {};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts") {};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> orderEventSource =
                env.readTextFile(Objects.requireNonNull(TxPayMatchByJoin.class.getResource("/OrderLog.csv")).getPath());
        DataStreamSource<String> receiptEventSource =
                env.readTextFile(Objects.requireNonNull(TxPayMatchByJoin.class.getResource("/ReceiptLog.csv")).getPath());

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

        // TODO 3.支付实时对账，双流匹配，使用 join 实现，这种方式只能获取到匹配的事件，匹配不到的无法获取
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxPayMatchDetectByJoin());


        // 4.输出结果
        resultStream.print("matched");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute("transaction pay match by join job");
    }


    /**
     * 自定义 ProcessJoinFunction 函数，实现双流匹配
     */
    private static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
        @Override
        public void processElement(
                OrderEvent left,
                ReceiptEvent right,
                ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context ctx,
                Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

            out.collect(new Tuple2<>(left, right));

        }
    }
}
