package com.wildgoose.market;

import com.wildgoose.market.beans.AdClickEvent;
import com.wildgoose.market.beans.AdCountViewByProvince;
import com.wildgoose.market.beans.BlacklistUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
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
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

/**
 * @Description : 页面广告分析，剔除黑名单
 * @Author : JustxzzZ
 * @Date : 2023.05.02 19:23
 */
public class AdStatisticsByProvinceWithoutBlacklist {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1.从文件中读取数据
        URL resource = AdStatisticsByProvinceWithoutBlacklist.class.getResource("/AdClickLog.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 2.转换成 POJOs，并指定 eventTime 和 watermark
        SingleOutputStreamOperator<AdClickEvent> adClickStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // TODO 3.剔除黑名单，即对同一广告点击次数超过一定上限的用户进行剔除与报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickStream
                .keyBy("userId", "adId")    // 基于用户ID和广告ID分组
                .process(new FilterBlackListUser(100));

        // TODO 4.广告推广统计分析
        // 基于省份开窗分组统计 Slide(1h, 5s)
        SingleOutputStreamOperator<AdCountViewByProvince> adCountStream = filterAdClickStream
                .keyBy("province")                       // 基于省份分组
                .timeWindow(Time.hours(1), Time.seconds(5))     // 开一个长度为1h，步长为5s的滑窗
                .aggregate(new AdCountAgg(), new AdCountWindow());

        // 5.输出结果
        adCountStream.print("adCount");
        filterAdClickStream.getSideOutput(new OutputTag<BlacklistUserWarning>("blacklist"){}).print("blacklist");

        env.execute("ad count without blacklist job");

    }


    /**
     * 自定义聚合函数，根据省份统计广告推广数量
     */
    private static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
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
     * 自定义窗口函数，获取各省份广告推广数量及窗口信息，并包装成 POJO
     */
    private static class AdCountWindow implements WindowFunction<Long, AdCountViewByProvince, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {

            String windowEnd = new Timestamp(window.getEnd()).toString();
            out.collect(new AdCountViewByProvince(tuple.getField(0), windowEnd, input.iterator().next()));

        }
    }


    /**
     * 自定义 process 函数，过滤点击次数超限的用户
     */
    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {

        private Integer countUpperBound;  // 点击次数上限
        private ValueState<Long> clickCountState;    // 定义一个状态变量，保存点击次数
        private ValueState<Boolean> isSentState;     // 定义一个状态变量，保存当前用户是否已经发送到黑名单

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            clickCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("click-count", Long.class ,0L));
            isSentState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("is-sent", Boolean.class, false));
        }

        @Override
        public void processElement(AdClickEvent value,
                                   KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>.Context ctx,
                                   Collector<AdClickEvent> out) throws Exception {

            // 判断当前用户对同一个广告的点击次数，如果不超上限，就 count + 1 正常输出；
            // 如果达到上限，直接过滤，并侧输出流输出黑名单用户
            Long clickCount = clickCountState.value();

            // 1. 判断是否是第一个数，如果是，则注册一个24小时后的定时器
            if (clickCount == 0) {
                // (24*60*60*1000) - 1天的毫秒数；(8*60*60*1000) - 8小时的毫秒数，用于转到东八区
                Long ts = (ctx.timerService().currentProcessingTime() / (24*60*60*1000) + 1) * (24*60*60*1000) - (8*60*60*1000);
                ctx.timerService().registerProcessingTimeTimer(ts);
            }

            // 2. 判断是否报警
            if (clickCount >= countUpperBound) {
                // 判断是否已经输出到黑名单，如果没有，则输出；否则不输出
                if (!isSentState.value()) {
                    isSentState.update(true);
                    ctx.output(new OutputTag<BlacklistUserWarning>("blacklist") {},   // 定义侧输出流 Tag
                            new BlacklistUserWarning(value.getUserId(), value.getAdId(),
                                    "click over " + countUpperBound + " times today."));
                }

                return; // 不再执行下面的操作
            }

            clickCountState.update(clickCount + 1);
            out.collect(value);

        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>.OnTimerContext ctx,
                            Collector<AdClickEvent> out) throws Exception {

            // 清空所有状态
            clickCountState.clear();
            isSentState.clear();

        }
    }
}
