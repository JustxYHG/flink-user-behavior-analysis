package com.wildgoose.networkflow;

import com.wildgoose.networkflow.beans.PageViewCount;
import com.wildgoose.networkflow.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @Description : 实时流量分析 - 页面访问量 PV
 * @Author : JustxzzZ
 * @Date : 2023.04.28 10:36
 */
public class PageView {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据源
        DataStreamSource<String> inputStream =
                env.readTextFile("network-flow-analysis/src/main/resources/UserBehavior.csv");

        // 3.转换成 POJOs ，并指定 eventTime 字段
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(
                    new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4])
            );
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // TODO 4.统计流量 - PV
//        SingleOutputStreamOperator<Tuple2<String, Integer>> pvStream = dataStream
//                .filter(data -> "pv".equals(data.getBehavior()))
//                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
//                        return new Tuple2<>(value.getBehavior(), 1);
//                    }
//                })
//                .keyBy(0)
//                .timeWindow(Time.hours(1))
//                .sum(1);

        // 统计 PV - 并行度优化，将 key 打散
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountWindow())
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());

        // 5.输出结果
        pvStream.print("pv");

        // 6.执行
        env.execute("network-flow-pv");

    }


    /**
     * 自定义聚合函数，统计每个窗口内不同 key 值的数量
     */
    private static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator + value.f1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


    /**
     * 自定义窗口函数，获取每个窗口中不同 key 值的数量及窗口信息，并包装成 POJO
     */
    private static class PvCountWindow implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
        }
    }


    /**
     * 自定义处理函数，把相同窗口分组统计的count值叠加
     */
    private static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {

        // 定义一个状态变量，保存每个窗口中的 pv 总数
        private ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount value,
                                   KeyedProcessFunction<Long, PageViewCount, PageViewCount>.Context ctx,
                                   Collector<PageViewCount> out) throws Exception {

            // 更新状态
            totalCountState.update(totalCountState.value() + value.getCount());
            // 定义定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Long, PageViewCount, PageViewCount>.OnTimerContext ctx,
                            Collector<PageViewCount> out) throws Exception {

            // 触发定时器，输出结果
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCountState.value()));

            // 清空状态
            totalCountState.clear();

        }
    }
}
