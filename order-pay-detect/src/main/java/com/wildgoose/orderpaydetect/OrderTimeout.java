package com.wildgoose.orderpaydetect;

import com.wildgoose.orderpaydetect.beans.OrderEvent;
import com.wildgoose.orderpaydetect.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Description : 订单超时检测
 * @Author : JustxzzZ
 * @Date : 2023.05.05 10:43
 */
public class OrderTimeout {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件读取数据
        String filePath = Objects.requireNonNull(OrderTimeout.class.getResource("/OrderLog.csv")).getPath();
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

        // TODO 3.订单超时检测 - 15分钟内未完成支付
        // 3.1 定义模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern
                .<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        // 3.2 应用模式
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern);

        // 3.3 提取事件
        OutputTag<OrderResult> orderTimeout = new OutputTag<OrderResult>("orderTimeout") {};

        SingleOutputStreamOperator<OrderResult> resultStream = patternStream
                .select(
                        // 侧输出流标签
                        orderTimeout,
                        // 自定义超时事件提取
                        new PatternTimeoutFunction<OrderEvent, OrderResult>() {
                            @Override
                            public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                                return new OrderResult(map.get("create").get(0).getOrderId(), "timeout " + l / 1000);
                            }
                        },
                        // 自定义匹配事件提取
                        new PatternSelectFunction<OrderEvent, OrderResult>() {
                            @Override
                            public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
                                return new OrderResult(map.get("create").get(0).getOrderId(), "payed");
                            }
                        });

        // 4.输出结果
        resultStream.print("payed");
        resultStream.getSideOutput(orderTimeout).print("timeout");

        env.execute("order timeout detect with cep job");

    }
}
