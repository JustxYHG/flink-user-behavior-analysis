package com.wildgoose.networkflow;

import com.wildgoose.networkflow.beans.ApacheLogEvent;
import com.wildgoose.networkflow.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @Description : 实时流量统计 - 热门页面浏览数
 *      1.需求描述：读取服务器日志中的每一行 log，统计在一段时间内用户访问每一个 url 的次数，然后排序输出显示。
 *      2.实现逻辑：每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL.
 * @Author : JustxzzZ
 * @Date : 2023.04.25 16:57
 */
public class HotPage {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);

        // 2.读取数据，该数据存在乱序
        DataStreamSource<String> inputStream =
                env.readTextFile("network-flow-analysis/src/main/resources/apache.log");

        // 3.转换成POJOs，并指定 eventTime 字段
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], 0L, timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getEventTime();
            }
        });

        // TODO 4.流量分析
        // keyedBy(url) -> window -> aggregate

        // 定义一个侧输出流 Tag
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {};

        SingleOutputStreamOperator<PageViewCount> pageCountStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))
                .filter( data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                } )
                .keyBy("url")
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountWindow());

        // keyedBy(window) -> process
        SingleOutputStreamOperator<String> hotPageTopNStream = pageCountStream
                .keyBy("windowEnd")
                .process(new HotPageTopN(5));


        // 5.数据结果
        hotPageTopNStream.print("HotPageTopN");
        pageCountStream.getSideOutput(lateTag).print("late");

        // 6.执行
        env.execute("network-flow-hot-page");

    }

    /**
     * 自定义预聚合函数，针对每个窗口，每来一条数据，计数加1
     */
    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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
     * 自定义窗口函数，结合窗口信息，返回每个窗口中每个 url 的访问次数
     */
    private static class PageCountWindow implements WindowFunction<Long, PageViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple,
                          TimeWindow window,
                          Iterable<Long> input,
                          Collector<PageViewCount> out) throws Exception {

            out.collect(new PageViewCount(tuple.getField(0), window.getEnd(), input.iterator().next()));

        }
    }

    /**
     * 自定义排序函数，输出访问量前 N 的 url
     */
    private static class HotPageTopN extends KeyedProcessFunction<Tuple, PageViewCount, String> {

        private Integer topSize;
        private MapState<String, Long> pageViewCountMapState;

        public HotPageTopN(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("page-count-map", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount value,
                                   KeyedProcessFunction<Tuple, PageViewCount, String>.Context ctx,
                                   Collector<String> out) throws Exception {

            pageViewCountMapState.put(value.getUrl(), value.getCount());

            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);   // 与延迟一分钟关闭窗口对应，用来清空状态

        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Tuple, PageViewCount, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {

            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态
            if (timestamp == ((long) ctx.getCurrentKey().getField(0) + 60 * 1000L)) {
                pageViewCountMapState.clear();
                return;
            }

            List<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());

            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("\n===========================================\n");
            result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> pageViewCount = pageViewCounts.get(i);
                result.append("No.").append(i + 1).append(":").append("\t")
                        .append("访问量=").append(pageViewCount.getValue()).append("\t")
                        .append("url=").append(pageViewCount.getKey()).append("\n");
            }

            result.append("===========================================\n\n");

            Thread.sleep(500);

            out.collect(result.toString());
        }
    }
}
