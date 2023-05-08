package com.wildgoose.networkflow;

import com.wildgoose.networkflow.beans.PageViewCount;
import com.wildgoose.networkflow.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Description : 实时流量分析 - 独立访客数 UV
 * @Author : JustxzzZ
 * @Date : 2023.04.28 15:24
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

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

        // TODO 4.统计流量 - UV
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCount());

        // 5.输出结果
        uvStream.print("pv");

        // 6.执行
        env.execute("network-flow-uv");

    }

    /**
     * 自定义全窗口函数类 - 统计页面 UV
     */
    private static class UvCount implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {

            // 定义一个集合，保存 userId 状态
            HashSet<Long> userIdSet = new HashSet<>();

            for (UserBehavior value : values) {
                userIdSet.add(value.getUserId());
            }

            out.collect(new PageViewCount("uv", window.getEnd(), (long) userIdSet.size()));

        }
    }
}
